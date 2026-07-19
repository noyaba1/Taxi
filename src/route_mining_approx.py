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
import argparse
import csv
import os
import pickle
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.spark_session import get_spark
from src import config
from src.route_mining_exact import subroutes_udf

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
    """mapPartitions: build one heavy-hitter sketch per threshold + one Count-Min."""
    import datasketches as ds
    ss = [ds.frequent_strings_sketch(LG) for _ in THRESHOLDS]
    cm = _new_cm(ds)
    for r in rows:
        sr, length = r["subroute"], r["length_km"]
        cm.update(sr, 1)
        for i, L in enumerate(THRESHOLDS):
            if length >= L:
                ss[i].update(sr, 1)
    yield pickle.dumps({"ss": [s.serialize() for s in ss], "cm": cm.serialize()})


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
    return pickle.dumps({"ss": ss, "cm": cm.serialize()})


def build_sketches(windows_df):
    """
    Distributed build + MERGE-ON-EXECUTORS. Returns (ss_list, cm, n_parts, n_parts).
    We use treeReduce (not collect): each partition builds a small bundle and the
    bundles are merged pairwise across executors, so the driver receives only ONE
    final (capacity-bounded ~65 MB) bundle -- no matter how many partitions or how
    big the data. Collecting all partition bundles blows spark.driver.maxResultSize
    at scale (found in the 50k dry run: 13 x ~80 MB > 1 GB).
    """
    import datasketches as ds
    rdd = windows_df.select("subroute", "length_km").rdd
    n_parts = rdd.getNumPartitions()
    final = rdd.mapPartitions(_build_partition_sketches).treeReduce(_merge_bundles)
    d = pickle.loads(final)
    ss = [ds.frequent_strings_sketch.deserialize(x) for x in d["ss"]]
    cm = ds.count_min_sketch.deserialize(d["cm"])
    return ss, cm, n_parts, n_parts


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


def _report(name, lines):
    d = os.path.join(config.OUTPUT_BASE, "statistics")
    os.makedirs(d, exist_ok=True)
    p = os.path.join(d, name)
    with open(p, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return p


def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def main(use_sample: bool) -> None:
    spark = get_spark("route-mining-approx")
    suffix = "sample" if use_sample else "full"
    res = config.H3_RESOLUTION

    enc_path = config.CLEAN_PARQUET.replace(".parquet", f"_encoded_r{res}_{suffix}.parquet")
    enc = spark.read.parquet(enc_path).select("TRIP_ID", "h3_seq_compact")
    windows = enc.select(F.explode(subroutes_udf("h3_seq_compact")).alias("w")).select("w.*")
    windows.cache()
    n_windows = windows.count()   # exact shuffle INPUT records; materialise cache

    # ---------- EXACT baseline (ground truth) ----------
    t0 = time.time()
    agg = (windows.groupBy("subroute")
           .agg(F.count(F.lit(1)).alias("support"), F.first("length_km").alias("length_km")))
    agg.cache()
    n_distinct = agg.count()
    t_exact = time.time() - t0
    exact_top = {}
    for L in THRESHOLDS:
        rows = (agg.filter(F.col("length_km") >= L)
                .orderBy(F.col("support").desc(), F.col("length_km").desc())
                .limit(TOP_K).collect())
        exact_top[L] = [r["subroute"] for r in rows]

    # ---------- APPROX (Space-Saving + Count-Min via mapPartitions + merge) ----------
    t0 = time.time()
    ss, cm, n_parts, merge_records = build_sketches(windows)
    cand = approx_topk(ss)
    t_approx = time.time() - t0

    # exact support+length for every approx candidate (for error metrics + CSV)
    approx_keys = list({k for L in THRESHOLDS for (k, *_1) in cand[L][:TOP_K]})
    info = {r["subroute"]: (r["support"], r["length_km"])
            for r in agg.filter(F.col("subroute").isin(approx_keys)).collect()}

    # ---------- memory ----------
    approx_mem = sum(len(s.serialize()) for s in ss) + len(cm.serialize())
    avg_key_len = agg.select(F.avg(F.length("subroute"))).collect()[0][0] or 0
    exact_mem = int(n_distinct * (avg_key_len + 48))

    rep = [f"# M7 Approximate vs Exact Sub-route Mining ({suffix})",
           f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
           f"\nconfig: frequent_strings lg={LG} (~{int(0.75*(1<<LG)):,} counters) x{len(THRESHOLDS)}"
           f" ; count_min {CM_HASHES}x{CM_BUCKETS:,} ; seed=lib-default(deterministic) ; top_k={TOP_K}\n",
           "## Runtime, memory & shuffle",
           f"- exact groupBy time   : {t_exact:.1f}s",
           f"- approx sketch time    : {t_approx:.1f}s",
           f"- exact memory (est)    : {exact_mem/1e6:.1f} MB  ({n_distinct:,} keys x ~{avg_key_len:.0f}B)",
           f"- approx memory (real)  : {approx_mem/1e6:.1f} MB  (fixed by capacity)",
           f"- memory ratio          : {exact_mem/max(approx_mem,1):.1f}x smaller",
           f"- exact shuffled records: {n_windows:,}  (window rows into groupBy)",
           f"- approx merged records : {merge_records:,}  (1 sketch bundle / partition, {n_parts} parts)\n",
           "## Accuracy vs exact top-100",
           "| min_len | emitted_cand | overlap@100 | precision@100 | recall@100 | abs_err(MAE) | rel_err(MRE) | CMS_MAE |",
           "|---|---|---|---|---|---|---|---|"]

    routes_dir = os.path.join(config.OUTPUT_BASE, "routes")
    os.makedirs(routes_dir, exist_ok=True)
    csv_rows = []
    for L in THRESHOLDS:
        emitted = len(cand[L])                       # candidates the sketch surfaced
        a = cand[L][:TOP_K]
        a_set = {k for (k, *_1) in a}
        e_set = set(exact_top[L])
        overlap = len(a_set & e_set)
        precision = overlap / len(a_set) if a_set else 0.0
        recall = overlap / len(e_set) if e_set else 0.0
        ss_ae, ss_re, cms_ae = [], [], []
        for rank, (k, est, lb, ub) in enumerate(a, 1):
            ex_sup, ex_len = info.get(k, (None, None))
            if ex_sup is not None:
                ss_ae.append(abs(est - ex_sup)); ss_re.append(abs(est - ex_sup) / ex_sup)
                cms_ae.append(abs(cm.get_estimate(k) - ex_sup))
            csv_rows.append((L, rank, est, lb, ub, cm.get_estimate(k),
                             ex_sup if ex_sup is not None else "",
                             round(ex_len, 3) if ex_len is not None else "", k))
        rep.append(f"| {L} | {emitted:,} | {overlap} | {precision:.2f} | {recall:.2f} | "
                   f"{_mean(ss_ae):.1f} | {_mean(ss_re):.3f} | {_mean(cms_ae):.1f} |")

    csv_path = os.path.join(routes_dir, f"approx_top100_{suffix}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["min_len_km", "rank", "ss_estimate", "ss_lb", "ss_ub",
                    "cms_estimate", "exact_support", "length_km", "subroute"])
        w.writerows(csv_rows)

    rp = _report(f"m7_approx_mining_{suffix}.md", rep)
    print("\n".join(rep))
    print(f"\n[m7] wrote approx routes -> {csv_path}")
    print(f"[m7] wrote report        -> {rp}")
    spark.stop()
    print("M7 APPROXIMATE MINING COMPLETE.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
