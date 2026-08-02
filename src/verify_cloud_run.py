"""
verify_cloud_run.py  --  did the DataProc run actually work, and meet the brief?
===============================================================================
"It finished without an error" is not evidence that a distributed run is correct.
A cluster can silently process a truncated input, lose a stage's output, or
produce different numbers because the data got partitioned differently.

This compares the cloud run against the LOCAL FULL-SCALE RUN, which is a
known-good baseline: same dataset, same code, same commit. Where the algorithm
is deterministic, the answers must be IDENTICAL -- and most of this pipeline is.

WHAT MUST MATCH, AND WHY
------------------------
  EXACT   Method D (suffix array). It contains no randomness at all: no sampling,
          no seeds, no hash-order dependence. Its corridors and supports are a
          function of the data alone. If the cloud produced different numbers,
          the cloud read different data -- that is the single most informative
          check in this file.
  EXACT   trip counts, the Phase-1 rejection breakdown, activity-zone cells and
          their PageRank values, anomaly counts. PageRank over a fixed graph is
          as deterministic as the suffix array, so anything but equality here
          means the two runs built different graphs.
  CLOSE   Method C. Deterministic in principle, but heavy-path seeding sorts on
          edge weight and ties can break differently across partitionings.
  LOOSE   Method A. Samples trips (partition-dependent even with a fixed seed)
          and uses MinHash-LSH with unseeded hash functions. Existence and
          sanity only -- demanding equality here would produce false alarms.
  LOOSE   M7 sketches. Space-Saving / Count-Min are mergeable, but the merge
          ORDER depends on partitioning, so estimates may shift slightly.

Getting these tiers wrong in either direction is bad: too strict and you chase
phantom failures, too loose and a real truncation slips through.

Run (after `gsutil -m cp -r gs://<bucket>/porto/outputs ./cloud_outputs`):

    python -m src.verify_cloud_run --cloud-dir ./cloud_outputs
"""
import argparse
import csv
import io
import pathlib
import re
import sys

from src import config

DELIM = ">"
LENGTHS = config.ROUTE_LENGTH_THRESHOLDS_KM

# Artefacts the cloud run must produce. Missing any of these means a stage did
# not run or its output never reached GCS -- the failure mode that cost this
# project a whole run once before.
REQUIRED_ROUTES = [
    "suffix_array_top100", "clustering_top100", "graph_heavy_paths_top100",
    "activity_zones", "anomalies_top50", "temporal_corridors",
]
REQUIRED_REPORTS = [
    "phase1_quality", "phase2_feature_summary", "phase4_encoding_summary",
    "m12_suffix_array", "m9_clustering", "m10_graph", "m11_anomalies",
    "method_comparison", "holdout_validation", "temporal_analysis",
]


class Check:
    """Accumulates results so one failure does not hide the rest."""

    def __init__(self):
        self.rows = []

    def add(self, tier, name, ok, detail=""):
        self.rows.append((tier, name, bool(ok), detail))
        mark = "OK  " if ok else "FAIL"
        print(f"  [{mark}] {name}" + (f" -> {detail}" if detail else ""))
        return ok

    @property
    def failed(self):
        return [r for r in self.rows if not r[2]]


def _read_csv(path):
    p = pathlib.Path(path)
    if not p.exists():
        return None
    return list(csv.DictReader(io.StringIO(p.read_text(encoding="utf-8"))))


def _read_text(path):
    p = pathlib.Path(path)
    return p.read_text(encoding="utf-8") if p.exists() else None


def _trips_from_report(text):
    """Pull the trip count out of a generated report header."""
    if not text:
        return None
    m = re.search(r"trips:\s*([\d,]+)", text)
    return int(m.group(1).replace(",", "")) if m else None


def _corridor_map(rows, key="subroute"):
    """{route -> (support, support_taxis)} for exact comparison."""
    out = {}
    for r in rows or []:
        if r.get(key):
            out[r[key]] = (r.get("support"), r.get("support_taxis"))
    return out


_TABLE_ROW = re.compile(r"^\|\s*([^|]+?)\s*\|\s*([\d,]+)\s*\|")


def _md_counts(text):
    """
    {row label -> integer} for every `| label | 12,345 | ... |` line in a report.

    The generated reports are the only record of the counting stages -- there is
    no parquet to diff, because the cluster's intermediate data dies with it. So
    the numbers have to be compared where they were written down.
    """
    out = {}
    for line in (text or "").splitlines():
        m = _TABLE_ROW.match(line)
        if m and not set(m.group(1)) <= {"-", " ", ":"}:
            out[m.group(1)] = int(m.group(2).replace(",", ""))
    return out


def _header_counts(text):
    """`rows read: 1,710,670 | rows written: 1,614,508 (94.4%)` -> both numbers."""
    out = {}
    for label in ("rows read", "rows written"):
        m = re.search(rf"{label}:\s*([\d,]+)", text or "")
        if m:
            out[label] = int(m.group(1).replace(",", ""))
    return out


def main(cloud_dir, baseline_dir, scale):
    cloud = pathlib.Path(cloud_dir)
    base = pathlib.Path(baseline_dir)
    if not cloud.is_dir():
        raise SystemExit(f"cloud output dir not found: {cloud}\n"
                         f"Download it first:\n"
                         f"  gsutil -m cp -r gs://<bucket>/porto/outputs {cloud}")

    c = Check()
    print(f"cloud    : {cloud}")
    print(f"baseline : {base}  (local {scale}-scale run)\n")

    # ---------------------------------------------------------------- artefacts
    print("A. Every required artefact reached GCS")
    for stem in REQUIRED_ROUTES:
        f = cloud / "routes" / f"{stem}_{scale}.csv"
        c.add("artefact", f"routes/{stem}_{scale}.csv", f.exists())
    for stem in REQUIRED_REPORTS:
        f = cloud / "statistics" / f"{stem}_{scale}.md"
        c.add("artefact", f"statistics/{stem}_{scale}.md", f.exists())

    # ------------------------------------------------------------ scale sanity
    print("\nB. The cluster processed the WHOLE dataset")
    cl_txt = _read_text(cloud / "statistics" / f"m12_suffix_array_{scale}.md")
    bs_txt = _read_text(base / "statistics" / f"m12_suffix_array_{scale}.md")
    cl_n, bs_n = _trips_from_report(cl_txt), _trips_from_report(bs_txt)
    if cl_n is None:
        c.add("scale", "trip count readable from the cloud report", False)
    elif bs_n is None:
        c.add("scale", "baseline present for comparison", False,
              "run the pipeline locally at this scale first")
    else:
        c.add("scale", "trip count matches the local baseline", cl_n == bs_n,
              f"cloud={cl_n:,} baseline={bs_n:,}")

    # ----------------------------------------------------- Phase-1 cleaning
    # The evidence README claims "Phase-1 cleaning matched on all five rejection
    # counts". That claim was made by hand; nothing here checked it. A cluster
    # that silently read a truncated input would still have passed check B if the
    # suffix-array report's header happened to agree, so the rejection breakdown
    # is where a partial read shows up first.
    print("\nB2. Phase-1 cleaning reproduces the baseline (every rejection count)")
    cl_p1 = _read_text(cloud / "statistics" / f"phase1_quality_{scale}.md")
    bs_p1 = _read_text(base / "statistics" / f"phase1_quality_{scale}.md")
    if not cl_p1 or not bs_p1:
        c.add("scale", "phase1_quality report on both sides", False,
              "cloud missing" if not cl_p1 else "baseline missing")
    else:
        cl_c, bs_c = _md_counts(cl_p1), _md_counts(bs_p1)
        cl_h, bs_h = _header_counts(cl_p1), _header_counts(bs_p1)
        c.add("scale", "rows read / written match", cl_h == bs_h and bool(cl_h),
              f"cloud={cl_h} baseline={bs_h}")
        c.add("scale", "rejection rules present", len(cl_c) >= 5,
              f"{len(cl_c)} rules parsed")
        diff = {k: (bs_c.get(k), cl_c.get(k))
                for k in set(cl_c) | set(bs_c) if cl_c.get(k) != bs_c.get(k)}
        c.add("scale", "every rejection count matches", not diff,
              f"{len(diff)} differ: {diff}" if diff else f"{len(cl_c)} identical")

    # --------------------------------------------------- EXACT: the suffix array
    print("\nC. Method D reproduces the baseline EXACTLY (it has no randomness)")
    cl_d = _read_csv(cloud / "routes" / f"suffix_array_top100_{scale}.csv")
    bs_d = _read_csv(base / "routes" / f"suffix_array_top100_{scale}.csv")
    if cl_d is None or bs_d is None:
        c.add("exact", "Method D outputs available on both sides", False)
    else:
        cm, bm = _corridor_map(cl_d), _corridor_map(bs_d)
        shared = set(cm) & set(bm)
        differing = [k for k in shared if cm[k] != bm[k]]
        c.add("exact", "Method D corridor set matches", set(cm) == set(bm),
              f"cloud={len(cm)} baseline={len(bm)} shared={len(shared)}")
        c.add("exact", "Method D supports identical on shared corridors",
              not differing,
              f"{len(differing)} differ" if differing else f"{len(shared)} identical")

    # ------------------------------------------------------- the deliverable
    print("\nD. The deliverable: top-100 at each length configuration")
    for stem, label in (("suffix_array_top100", "D"),
                        ("clustering_top100", "A"),
                        ("graph_heavy_paths_top100", "C")):
        rows = _read_csv(cloud / "routes" / f"{stem}_{scale}.csv")
        if rows is None:
            c.add("deliverable", f"method {label} present", False)
            continue
        counts = {L: sum(1 for r in rows if int(r["min_len_km"]) == L)
                  for L in LENGTHS}
        # >=1/3/5 km must be full lists; the long bands are allowed to be short
        # or empty -- that is a measured property of the data (see FINAL_REPORT
        # 9d), not a failure of the run.
        core_ok = all(counts[L] > 0 for L in (1, 3, 5))
        c.add("deliverable", f"method {label} populates the core bands", core_ok,
              " ".join(f">={L}:{counts[L]}" for L in LENGTHS))

    # -------------------------------------------- activity zones + anomalies
    # Both are graded deliverables in their own right, and PageRank over a fixed
    # graph is as deterministic as the suffix array -- so "close enough" is the
    # wrong tier for it. Different zones across the two runs would mean the two
    # runs built different transition graphs.
    print("\nD2. Activity zones and anomalies (deterministic, must match)")
    cl_z = _read_csv(cloud / "routes" / f"activity_zones_{scale}.csv")
    bs_z = _read_csv(base / "routes" / f"activity_zones_{scale}.csv")
    if cl_z is None or bs_z is None:
        c.add("exact", "activity zones available on both sides", False)
    else:
        c.add("exact", "zone count matches", len(cl_z) == len(bs_z),
              f"cloud={len(cl_z)} baseline={len(bs_z)}")
        cl_cells = [r["cell"] for r in cl_z]
        c.add("exact", "zone cells match, in rank order",
              cl_cells == [r["cell"] for r in bs_z],
              f"{len(cl_cells)} cells")
        pr_diff = [(a["cell"], a["pagerank"], b["pagerank"])
                   for a, b in zip(cl_z, bs_z) if a["pagerank"] != b["pagerank"]]
        c.add("exact", "PageRank values identical", not pr_diff,
              f"{len(pr_diff)} differ" if pr_diff else f"{len(cl_z)} identical")

        prs = [float(r["pagerank"]) for r in cl_z]
        c.add("sanity", "PageRank positive and ranked descending",
              all(p > 0 for p in prs) and prs == sorted(prs, reverse=True),
              f"max={max(prs):.6g} min={min(prs):.6g}" if prs else "empty")
        taxis = [int(r["distinct_taxis"]) for r in cl_z]
        c.add("sanity", "no zone exceeds the 442-taxi fleet",
              all(0 <= t <= config.FLEET_SIZE for t in taxis),
              f"max={max(taxis)}" if taxis else "empty")

    cl_a = _read_csv(cloud / "routes" / f"anomalies_top50_{scale}.csv")
    bs_a = _read_csv(base / "routes" / f"anomalies_top50_{scale}.csv")
    if cl_a is None or bs_a is None:
        c.add("exact", "anomaly output available on both sides", False)
    else:
        c.add("exact", "anomaly row count matches", len(cl_a) == len(bs_a),
              f"cloud={len(cl_a)} baseline={len(bs_a)}")

    # ------------------------------------------------------- physical sanity
    print("\nE. No corridor is an artefact of a GPS gap")
    limit = config.max_cell_hop_km()
    for stem, col in (("suffix_array_top100", "subroute"),
                      ("clustering_top100", "subroute"),
                      ("graph_heavy_paths_top100", "route"),
                      ("approx_top100", "subroute")):
        rows = _read_csv(cloud / "routes" / f"{stem}_{scale}.csv")
        if not rows:
            continue
        # length_km is the column the whole deliverable is filtered on. An empty
        # one is not a geometry violation, so the hop check below skipped it in
        # silence -- which is exactly how --approx-only shipped a blank length on
        # every row it wrote. Missing is now its own failure.
        blank = [r for r in rows if not r.get("length_km")]
        c.add("sanity", f"{stem}: every row has a length", not blank,
              f"{len(blank)} of {len(rows)} rows blank" if blank
              else f"{len(rows)} rows populated")
        bad = [r for r in rows
               if r.get("length_km") and int(r["n_cells"]) > 1
               and float(r["length_km"]) / (int(r["n_cells"]) - 1) > limit]
        c.add("sanity", f"{stem}: km-per-cell within {limit:.2f} km", not bad,
              f"{len(bad)} violations" if bad else f"{len(rows)} rows clean")

    # ------------------------------------------------- tiers we do NOT compare
    print("\nF. Expected-to-differ (reported, not failed)")
    for stem, label, why in (
            ("clustering_top100", "Method A",
             "samples trips + unseeded MinHash-LSH"),
            ("approx_top100", "M7 sketches",
             "mergeable sketches; merge order is partition-dependent")):
        cl = _read_csv(cloud / "routes" / f"{stem}_{scale}.csv")
        bs = _read_csv(base / "routes" / f"{stem}_{scale}.csv")
        if cl is None:
            print(f"  [ -- ] {label}: not produced by the cloud run")
            continue
        n_cl, n_bs = len(cl), len(bs or [])
        print(f"  [info] {label}: cloud={n_cl} rows, baseline={n_bs} rows "
              f"({why}) -- difference here is expected, not a defect")

    # --------------------------------------------------------------- verdict
    print("\n" + "=" * 70)
    if c.failed:
        print(f"CLOUD RUN VERIFICATION FAILED -- {len(c.failed)} check(s):")
        for _t, name, _ok, detail in c.failed:
            print(f"  - {name} {('(' + detail + ')') if detail else ''}")
        print("\nMost likely causes, in order:")
        print("  missing artefacts  -> a stage was not submitted, or OUTPUT_BASE")
        print("                        was not a gs:// path (results died with the")
        print("                        cluster)")
        print("  trip count differs -> the cluster read a truncated or different")
        print("                        input; check the raw upload completed")
        print("  Method D differs   -> same as above. D is deterministic, so a")
        print("                        difference means the INPUT differed")
    else:
        print("CLOUD RUN VERIFICATION PASSED")
        print("  The cluster processed the same data and reproduced the")
        print("  deterministic results exactly.")
    print("=" * 70)

    print("\nStill to evidence BY HAND (not derivable from the outputs):")
    print("  1. >=5 machines. Capture before the cluster is deleted:")
    print("       gcloud dataproc clusters describe <name> --region <region> \\")
    print("         --format='value(config.workerConfig.numInstances)'")
    print("     The brief requires at least 5; the submit script uses WORKERS=5.")
    print("  2. It read from GCS. `gsutil ls gs://<bucket>/porto/processed/`")
    print("     should list the parquet tables the cluster wrote.")
    print("  3. Per-stage runtimes. The Dataproc job list, or")
    print("     gs://<bucket>/porto/outputs/statistics/timings.jsonl")

    raise SystemExit(1 if c.failed else 0)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cloud-dir", required=True,
                    help="downloaded cloud outputs (gsutil -m cp -r ... ./cloud_outputs)")
    ap.add_argument("--baseline-dir", default="outputs",
                    help="local outputs to compare against (default: ./outputs)")
    ap.add_argument("--scale", default="full", choices=config.SCALES)
    a = ap.parse_args()
    main(a.cloud_dir, a.baseline_dir, a.scale)
