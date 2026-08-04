"""
route_mining_approx.py  --  PHASE 7 / Milestone M7  (APPROXIMATE top-k)
======================================================================
Approximate heavy-hitter sub-route mining, compared head-to-head with the exact
M5 baseline.

METHODS
  * Space-Saving / heavy-hitters (PRIMARY): a mergeable frequent-items sketch
    (datasketches `frequent_strings_sketch`, the Misra-Gries/Space-Saving family)
    keeping only ~0.75*2^LG counters yet returning the top-k WITH per-item lower
    and upper support bounds. It is the top-k FINDER.
  * Count-Min Sketch (AUXILIARY): a mergeable frequency oracle that estimates the
    support of ANY queried route and NEVER underestimates. It cannot enumerate the
    top-k by itself (it stores no keys), so it is auxiliary.

WHY THIS SCALES: the exact path shuffles ~1.1M window rows into an 810k-key
groupBy; here we build a small sketch per partition (mapPartitions) and MERGE the
sketches, so only a handful of summaries move. Support semantics are preserved
because the window stream is deduped-within-trip (occurrence == distinct-trip
support).

All sketch parameters live in config.py (SKETCH_LG_MAX_K, CM_HASHES,
CM_LG_BUCKETS, SKETCH_SEED, TOP_K, ROUTE_LENGTH_THRESHOLDS_KM).

Run:
    python -m src.route_mining_approx --sample
"""
import pickle
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src import cells as cells_mod
from src import cli, config, storage
from src.route_mining_exact import DELIM, MIN_SUPPORT, emit_windows
from src.spark_session import get_spark

log = cli.setup_logging("m7")

THRESHOLDS = config.ROUTE_LENGTH_THRESHOLDS_KM
TOP_K = config.TOP_K
LG = config.SKETCH_LG_MAX_K
CM_HASHES = config.CM_HASHES
CM_BUCKETS = 1 << config.CM_LG_BUCKETS


def _new_cm(ds):
    """
    Construct a Count-Min sketch with the library's fixed default seed.
    NOTE: datasketches `count_min_sketch.deserialize(bytes)` always rebuilds with
    the default seed, so all partition sketches MUST share that seed to be
    mergeable. Overriding the seed breaks the cross-partition merge. The default
    seed is fixed, so the sketches remain fully deterministic (see SKETCH_SEED).
    """
    return ds.count_min_sketch(CM_HASHES, CM_BUCKETS)


def _build_partition_sketches(rows):
    """
    mapPartitions: build one heavy-hitter sketch per threshold + one Count-Min.

    Rows are CONSUMED AS A STREAM -- the window frame is never materialised.
    That is the whole point: memory here is bounded by sketch capacity, not by
    how many windows the trips happen to produce. (Caching the input to count it
    is what made this stage OOM at 200k trips, which rather defeated the object
    of the cheap method.) We count rows as they pass instead.
    """
    import datasketches as ds
    ss = [ds.frequent_strings_sketch(LG) for _ in THRESHOLDS]
    cm = _new_cm(ds)
    n = 0
    for r in rows:
        n += 1
        sr, length = r["subroute"], r["length_km"]
        cm.update(sr, 1)
        for i, L in enumerate(THRESHOLDS):
            if length >= L:
                ss[i].update(sr, 1)
    yield pickle.dumps({"ss": [s.serialize() for s in ss], "cm": cm.serialize(),
                        "n": n})


def _merge_bundles(b1, b2):
    """Merge two serialized sketch bundles (runs on executors via treeReduce)."""
    import datasketches as ds
    d1, d2 = pickle.loads(b1), pickle.loads(b2)
    ss = []
    for i in range(len(THRESHOLDS)):
        s = ds.frequent_strings_sketch.deserialize(d1["ss"][i])
        s.merge(ds.frequent_strings_sketch.deserialize(d2["ss"][i]))
        ss.append(s.serialize())
    cm = ds.count_min_sketch.deserialize(d1["cm"])
    cm.merge(ds.count_min_sketch.deserialize(d2["cm"]))
    return pickle.dumps({"ss": ss, "cm": cm.serialize(),
                         "n": d1["n"] + d2["n"]})


def build_sketches(windows_df):
    """
    Distributed build + MERGE-ON-EXECUTORS. Returns (ss_list, cm, n_parts).

    We use treeReduce (not collect): each partition builds a small bundle and the
    bundles are merged pairwise across executors, so the driver receives only ONE
    final (capacity-bounded ~65 MB) bundle -- no matter how many partitions or how
    big the data. Collecting all partition bundles blows spark.driver.maxResultSize
    at scale (found in the 50k dry run: 13 x ~80 MB > 1 GB).

    `n_parts` doubles as the number of sketch bundles that cross the network,
    which is the figure to compare against the exact path's shuffled row count.
    `n_rows` is the window count, accumulated during the same single pass.
    """
    import datasketches as ds
    rdd = windows_df.select("subroute", "length_km").rdd
    n_parts = rdd.getNumPartitions()
    final = rdd.mapPartitions(_build_partition_sketches).treeReduce(_merge_bundles)
    d = pickle.loads(final)
    ss = [ds.frequent_strings_sketch.deserialize(x) for x in d["ss"]]
    cm = ds.count_min_sketch.deserialize(d["cm"])
    return ss, cm, n_parts, d["n"]


def approx_topk(ss):
    """Per-threshold top-k candidates from the heavy-hitter sketches."""
    import datasketches as ds
    nfn = ds.frequent_items_error_type.NO_FALSE_NEGATIVES
    out = {}
    for i, L in enumerate(THRESHOLDS):
        items = ss[i].get_frequent_items(nfn)   # (item, est, lb, ub)
        items.sort(key=lambda x: -x[1])
        out[L] = items                           # full candidate list (pre-cut)
    return out


def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def _n_cells(key):
    return len(key.split(DELIM))


def _length_of(key):
    """
    Ground length of a sub-route, recomputed from its own key.

    The sketches store keys and nothing else, so length used to be looked up in
    the exact aggregate -- which `--approx-only` deliberately never builds. Every
    row it wrote therefore carried an EMPTY length_km, i.e. the mode that runs at
    mid and full scale produced the one column the deliverable is filtered on.

    The key is the DELIM-joined cell path, so the length is recoverable from the
    key alone by the same function that produced it in `emit_windows` -- exact,
    not an estimate, and driver-side over at most TOP_K x |THRESHOLDS| keys.
    """
    return cells_mod.path_length_km(key.split(DELIM))


def main(scale: str, approx_only: bool) -> None:
    spark = get_spark("route-mining-approx")
    paths = config.dataset_paths(scale)

    with cli.stage("m7_approx", scale, log) as st:
        enc = spark.read.parquet(paths["encoded"]).select("TRIP_ID", "h3_seq_compact")
        # Drop windows that hit the length cap BEFORE either path sees them: a
        # truncated window's `length_km` is the cap rather than a measurement
        # (see route_mining_exact's docstring). Filtering here rather than in
        # each path keeps the sketches and the exact baseline counting the same
        # population -- which is the only reason their comparison means anything.
        windows = emit_windows(enc).filter(
            ~F.coalesce(F.col("truncated"), F.lit(False)))

        # ---------- APPROX (Space-Saving + Count-Min via mapPartitions + merge) ----------
        # Deliberately NOT cached: one streaming pass, memory bounded by the
        # sketches. The window count comes back from that same pass.
        t0 = time.time()
        ss, cm, n_parts, n_windows = build_sketches(windows)
        cand = approx_topk(ss)
        t_approx = time.time() - t0

        # ---------- EXACT baseline (ground truth), optional ----------
        # --approx-only exists because the exact groupBy is the very cost the
        # sketches are meant to avoid. Computing it inside this job made M7
        # strictly MORE expensive than M5, so at full scale the "cheap" method
        # could not run anywhere the expensive one could not.
        agg = exact_top = info = None
        t_exact = float("nan")
        n_distinct = -1
        if not approx_only:
            t0 = time.time()
            agg = (windows.groupBy("subroute")
                   .agg(F.count(F.lit(1)).alias("support"),
                        F.first("length_km").alias("length_km")))
            agg.cache()
            n_distinct = agg.count()
            t_exact = time.time() - t0
            exact_top = {}
            for L in THRESHOLDS:
                rows = (agg.filter((F.col("length_km") >= L)
                                   & (F.col("support") >= MIN_SUPPORT))
                        .orderBy(F.col("support").desc(), F.col("length_km").desc(),
                                 F.col("subroute").asc())
                        .limit(TOP_K).collect())
                exact_top[L] = [r["subroute"] for r in rows]
            # exact support+length for every approx candidate (error metrics + CSV)
            approx_keys = list({k for L in THRESHOLDS for (k, *_1) in cand[L][:TOP_K]})
            info = {r["subroute"]: (r["support"], r["length_km"])
                    for r in agg.filter(F.col("subroute").isin(approx_keys)).collect()}

        # ---------- memory ----------
        approx_mem = sum(len(s.serialize()) for s in ss) + len(cm.serialize())

        rep = [f"# M7 Approximate vs Exact Sub-route Mining ({scale})",
               f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
               "",
               f"config: frequent_strings lg={LG} (~{int(0.75 * (1 << LG)):,} counters) "
               f"x{len(THRESHOLDS)} ; count_min {CM_HASHES}x{CM_BUCKETS:,} ; "
               f"seed=lib-default(deterministic) ; top_k={TOP_K}",
               "",
               "## Runtime, memory & shuffle",
               f"- approx sketch time    : {t_approx:.1f}s",
               f"- approx memory (real)  : {approx_mem / 1e6:.1f} MB  (fixed by capacity)",
               f"- exact shuffled records: {n_windows:,}  (window rows into groupBy)",
               f"- approx merged bundles : {n_parts:,}  (1 sketch bundle per partition)"]

        if approx_only:
            rep += ["", "_Run with `--approx-only`: the exact baseline was skipped, "
                    "so no accuracy comparison is reported here._"]
            log.info("approx-only: skipped the exact baseline")
        else:
            avg_key_len = agg.select(F.avg(F.length("subroute"))).collect()[0][0] or 0
            exact_mem = int(n_distinct * (avg_key_len + 48))
            rep += [f"- exact groupBy time    : {t_exact:.1f}s",
                    f"- exact memory (est)    : {exact_mem / 1e6:.1f} MB  "
                    f"({n_distinct:,} keys x ~{avg_key_len:.0f}B)",
                    f"- memory ratio          : "
                    f"{exact_mem / max(approx_mem, 1):.1f}x smaller",
                    "",
                    "## Accuracy vs exact top-100",
                    "| min_len | emitted_cand | overlap@100 | precision@100 | recall@100 "
                    "| abs_err(MAE) | rel_err(MRE) | CMS_MAE |",
                    "|---|---|---|---|---|---|---|---|"]

        csv_rows = []
        for L in THRESHOLDS:
            # Filter on the sketch's LOWER bound, not its upper one.
            #
            # Space-Saving guarantees only that a returned item's true support
            # lies in [lb, ub]. Filtering on `ub >= MIN_SUPPORT` (the first
            # attempt) asks "could this be popular?", which is the right
            # question for retaining candidates and the wrong one for a
            # deliverable: the >=40 km band came back with 100 rows whose
            # estimate was 8 and whose lb was 1, i.e. the sketch could not rule
            # out that every one of them was a single trip. Method D, which is
            # exact, reports that band empty.
            #
            # `lb >= MIN_SUPPORT` asks "is this route DEMONSTRABLY shared?" and
            # only reports what the sketch can stand behind. The gap between the
            # two is not noise to be hidden -- it is the one-sided error of a
            # frequency sketch, showing up exactly where support is thinnest,
            # which is the sparse tail these long bands live in.
            a = [c for c in cand[L] if c[2] >= MIN_SUPPORT][:TOP_K]
            ss_ae, ss_re, cms_ae = [], [], []
            for rank, (k, est, lb, ub) in enumerate(a, 1):
                ex_sup, _ = (info or {}).get(k, (None, None))
                if ex_sup:
                    ss_ae.append(abs(est - ex_sup))
                    ss_re.append(abs(est - ex_sup) / ex_sup)
                    cms_ae.append(abs(cm.get_estimate(k) - ex_sup))
                csv_rows.append((L, rank, est, lb, ub, cm.get_estimate(k),
                                 ex_sup if ex_sup is not None else "",
                                 round(_length_of(k), 3), _n_cells(k), k))
            if not approx_only:
                a_set = {k for (k, *_1) in a}
                e_set = set(exact_top[L])
                overlap = len(a_set & e_set)
                precision = overlap / len(a_set) if a_set else 0.0
                recall = overlap / len(e_set) if e_set else 0.0
                rep.append(f"| {L} | {len(cand[L]):,} | {overlap} | {precision:.2f} | "
                           f"{recall:.2f} | {_mean(ss_ae):.1f} | {_mean(ss_re):.3f} | "
                           f"{_mean(cms_ae):.1f} |")

        csv_path = storage.write_csv(
            storage.out_path("routes", f"approx_top100_{scale}.csv"),
            ["min_len_km", "rank", "ss_estimate", "ss_lb", "ss_ub", "cms_estimate",
             "exact_support", "length_km", "n_cells", "subroute"],
            csv_rows)
        rp = storage.write_lines(
            storage.out_path("statistics", f"m7_approx_mining_{scale}.md"), rep)

        log.info("\n%s", "\n".join(rep))
        log.info("wrote approx routes -> %s", csv_path)
        log.info("wrote report        -> %s", rp)
        st.update(shuffle_records=n_windows, sketch_bundles=n_parts,
                  approx_s=round(t_approx, 1), approx_mem_bytes=approx_mem,
                  exact_s=(None if approx_only else round(t_exact, 1)),
                  distinct_keys=(None if approx_only else n_distinct))

    spark.stop()
    log.info("M7 APPROXIMATE MINING COMPLETE.")


if __name__ == "__main__":
    ap = cli.scale_parser(__doc__)
    ap.add_argument("--approx-only", action="store_true",
                    help="skip the exact baseline (the cost the sketches replace)")
    args = ap.parse_args()
    main(cli.scale_of(args), args.approx_only)
