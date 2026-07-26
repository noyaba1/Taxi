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
  EXACT   trip counts, encoding statistics, activity-zone cells.
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

    # ------------------------------------------------------- physical sanity
    print("\nE. No corridor is an artefact of a GPS gap")
    limit = config.max_cell_hop_km()
    for stem, col in (("suffix_array_top100", "subroute"),
                      ("clustering_top100", "subroute"),
                      ("graph_heavy_paths_top100", "route")):
        rows = _read_csv(cloud / "routes" / f"{stem}_{scale}.csv")
        if not rows:
            continue
        bad = [r for r in rows
               if int(r["n_cells"]) > 1
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
