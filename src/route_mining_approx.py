"""
route_mining_approx.py  --  PHASE 7 / Milestone M7  (APPROXIMATE top-k)
======================================================================
Approximate heavy-hitter sub-route mining, compared head-to-head with the exact
M5 baseline.

METHODS
  * Space-Saving / heavy-hitters (PRIMARY): a mergeable frequent-items sketch
    (datasketches `frequent_strings_sketch`, the Misra-Gries/Space-Saving family)
    that keeps only ~0.75*2^LG counters yet returns the top-k with per-item lower
    and upper support bounds. It directly answers "the top-100 routes".
  * Count-Min Sketch (AUXILIARY): a mergeable frequency oracle that estimates the
    support of ANY given route (never underestimates). It does not by itself find
    the top-k (no key list), so it plays the auxiliary role of a frequency
    estimator we can query for the Space-Saving candidates.

WHY THIS SCALES (the whole point):
  the exact M5 path shuffles ~1.1M window rows into an 810k-key groupBy. Here we
  build a SMALL sketch per partition (mapPartitions) and MERGE the sketches, so
  only kilobytes-per-partition move across the network -- no big key shuffle.
  Support semantics are preserved because the window stream is already
  deduped-within-trip (occurrence == distinct-trip support).

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
from src.route_mining_exact import subroutes_udf, THRESHOLDS_KM, TOP_N

# --- sketch sizing (tunable; trades memory for accuracy) ---
LG_MAX_K = 16          # frequent-items map size = 2^16 -> ~49k counters/threshold
CM_HASHES = 5          # Count-Min depth (failure prob ~ 2^-5)
CM_BUCKETS = 1 << 17   # Count-Min width per row


def _build_partition_sketches(rows):
    """mapPartitions: build 6 heavy-hitter sketches + 1 Count-Min per partition."""
    import datasketches as ds
    ss = [ds.frequent_strings_sketch(LG_MAX_K) for _ in THRESHOLDS_KM]
    cm = ds.count_min_sketch(CM_HASHES, CM_BUCKETS)
    for r in rows:
        sr, length = r["subroute"], r["length_km"]
        cm.update(sr, 1)
        for i, L in enumerate(THRESHOLDS_KM):
            if length >= L:
                ss[i].update(sr, 1)
    yield pickle.dumps({"ss": [s.serialize() for s in ss], "cm": cm.serialize()})


def _merge(blobs):
    import datasketches as ds
    ss = [ds.frequent_strings_sketch(LG_MAX_K) for _ in THRESHOLDS_KM]
    cm = ds.count_min_sketch(CM_HASHES, CM_BUCKETS)
    for blob in blobs:
        d = pickle.loads(blob)
        for i in range(len(THRESHOLDS_KM)):
            ss[i].merge(ds.frequent_strings_sketch.deserialize(d["ss"][i]))
        cm.merge(ds.count_min_sketch.deserialize(d["cm"]))
    return ss, cm


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
    import datasketches as ds
    spark = get_spark("route-mining-approx")
    suffix = "sample" if use_sample else "full"
    res = config.H3_RESOLUTION

    enc_path = config.CLEAN_PARQUET.replace(".parquet", f"_encoded_r{res}_{suffix}.parquet")
    enc = spark.read.parquet(enc_path).select("TRIP_ID", "h3_seq_compact")
    windows = enc.select(F.explode(subroutes_udf("h3_seq_compact")).alias("w")).select("w.*")
    windows.cache()
    n_windows = windows.count()   # materialise cache so timings are fair

    # ---------- EXACT baseline (ground truth) ----------
    t0 = time.time()
    agg = (windows.groupBy("subroute")
           .agg(F.count(F.lit(1)).alias("support"), F.first("length_km").alias("length_km")))
    agg.cache()
    n_distinct = agg.count()
    t_exact = time.time() - t0

    # exact top-100 sets + a small exact-support lookup for the approx candidates
    exact_top = {}
    for L in THRESHOLDS_KM:
        rows = (agg.filter(F.col("length_km") >= L)
                .orderBy(F.col("support").desc(), F.col("length_km").desc())
                .limit(TOP_N).collect())
        exact_top[L] = [r["subroute"] for r in rows]

    # ---------- APPROX (Space-Saving + Count-Min via mapPartitions + merge) ----------
    t0 = time.time()
    blobs = windows.select("subroute", "length_km").rdd.mapPartitions(_build_partition_sketches).collect()
    ss, cm = _merge(blobs)
    t_approx = time.time() - t0

    NFN = ds.frequent_items_error_type.NO_FALSE_NEGATIVES
    approx_top = {}
    for i, L in enumerate(THRESHOLDS_KM):
        items = ss[i].get_frequent_items(NFN)          # (item, est, lb, ub)
        items.sort(key=lambda x: -x[1])
        approx_top[L] = items[:TOP_N]

    # exact support for every approx candidate (<=600 keys) for error metrics
    approx_keys = list({k for L in THRESHOLDS_KM for (k, *_1) in approx_top[L]})
    exact_sup = {r["subroute"]: r["support"]
                 for r in agg.filter(F.col("subroute").isin(approx_keys)).collect()}

    # ---------- comparison metrics ----------
    approx_mem = sum(len(s.serialize()) for s in ss) + len(cm.serialize())
    avg_key_len = agg.select(F.avg(F.length("subroute"))).collect()[0][0] or 0
    exact_mem = int(n_distinct * (avg_key_len + 48))   # key bytes + object overhead

    rep = [f"# M7 Approximate vs Exact Sub-route Mining ({suffix})",
           f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
           f"\nwindows: {n_windows:,} | exact distinct keys: {n_distinct:,}",
           f"sketch config: frequent_strings lg={LG_MAX_K} (~{int(0.75*(1<<LG_MAX_K)):,} counters) x"
           f"{len(THRESHOLDS_KM)} ; count_min {CM_HASHES}x{CM_BUCKETS:,}\n",
           "## Runtime & memory",
           f"- exact groupBy time  : {t_exact:.1f}s",
           f"- approx sketch time  : {t_approx:.1f}s",
           f"- exact memory (est)  : {exact_mem/1e6:.1f} MB  ({n_distinct:,} keys x ~{avg_key_len:.0f}B)",
           f"- approx memory (real): {approx_mem/1e6:.1f} MB  (serialized sketches)",
           f"- memory ratio        : {exact_mem/max(approx_mem,1):.1f}x smaller\n",
           "## Accuracy vs exact top-100",
           "| min_len | precision@100 | recall@100 | SS supp MAE | SS supp MRE | CMS supp MAE |",
           "|---|---|---|---|---|---|"]

    routes_dir = os.path.join(config.OUTPUT_BASE, "routes")
    os.makedirs(routes_dir, exist_ok=True)
    csv_rows = []
    for L in THRESHOLDS_KM:
        a = approx_top[L]
        a_set = {k for (k, *_1) in a}
        e_set = set(exact_top[L])
        inter = a_set & e_set
        precision = len(inter) / len(a_set) if a_set else 0.0
        recall = len(inter) / len(e_set) if e_set else 0.0
        ss_ae, ss_re, cms_ae = [], [], []
        for rank, (k, est, lb, ub) in enumerate(a, 1):
            ex = exact_sup.get(k)
            if ex is not None:
                ss_ae.append(abs(est - ex)); ss_re.append(abs(est - ex) / ex)
                cms_ae.append(abs(cm.get_estimate(k) - ex))
            csv_rows.append((L, rank, est, lb, ub, cm.get_estimate(k),
                             ex if ex is not None else "", k))
        rep.append(f"| {L} | {precision:.2f} | {recall:.2f} | {_mean(ss_ae):.1f} | "
                   f"{_mean(ss_re):.3f} | {_mean(cms_ae):.1f} |")

    csv_path = os.path.join(routes_dir, f"approx_top100_{suffix}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["min_len_km", "rank", "ss_estimate", "ss_lb", "ss_ub",
                    "cms_estimate", "exact_support", "subroute"])
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
