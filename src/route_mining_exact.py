"""
route_mining_exact.py  --  PHASE 5 / Milestone M5  (EXACT baseline)
==================================================================
Exact frequent contiguous sub-route mining over the H3-encoded trips.

MODEL (see the M5 spec):
  * sub-route      = a CONTIGUOUS window cells[i..j] of `h3_seq_compact`
  * length         = Haversine sum between consecutive compact cell centres
  * support        = number of DISTINCT trips containing the sub-route
                     (deduped within a trip, so repeats inside one trip count once)
  * output         = top-100 by support for min-lengths {1,3,5,10,20,40} km

WHY CONTIGUOUS n-grams (and NOT PrefixSpan / gapped sequential patterns):
  a physical route is a continuous path; a *gap* in a sub-sequence means the taxi
  teleported between non-adjacent cells, which is not a real route. Gapped mining
  (PrefixSpan) both allows those invalid patterns and solves a strictly harder,
  more expensive problem than we need. Contiguous substring counting is the
  correct AND cheaper model. See docs/DESIGN_REVIEW.md section 5.

This is the EXACT baseline. Its cost (emit every window, then one big groupBy
shuffle) is exactly what the approximate sketches in M7 will reduce.

Run:
    python -m src.route_mining_exact --sample
"""
import argparse
import os
import time
from datetime import datetime, timezone

import h3
from pyspark.sql import functions as F, types as T

from src.spark_session import get_spark
from src import config

THRESHOLDS_KM = [1, 3, 5, 10, 20, 40]
MIN_L = float(min(THRESHOLDS_KM))       # smallest threshold -> emit windows >= this
MAX_L_CAP = 45.0                        # stop extending a window past this (safety)
TOP_N = 100
DELIM = ">"                             # cell separator inside a sub-route key

# UDF returns an array of (subroute_key, length_km, n_cells) for one trip.
_SUBROUTE_SCHEMA = T.ArrayType(T.StructType([
    T.StructField("subroute", T.StringType()),
    T.StructField("length_km", T.DoubleType()),
    T.StructField("n_cells", T.IntegerType()),
]))


def _subroutes(cells):
    """
    All CONTIGUOUS windows of `cells` with MIN_L <= length <= MAX_L_CAP, deduped
    within the trip (support counts trips, not occurrences).
    Returns list of (subroute_key, length_km, n_cells).
    """
    if cells is None or len(cells) < 2:
        return []
    n = len(cells)
    # Precompute cumulative centre-to-centre distance so a window length is O(1).
    centers = [h3.h3_to_geo(c) for c in cells]
    cum = [0.0] * n
    for k in range(1, n):
        cum[k] = cum[k - 1] + h3.point_dist(centers[k - 1], centers[k], unit="km")

    seen = set()
    out = []
    for i in range(n):
        for j in range(i + 1, n):
            length = cum[j] - cum[i]
            if length > MAX_L_CAP:        # windows only get longer -> stop this start
                break
            if length >= MIN_L:
                key = DELIM.join(cells[i:j + 1])
                if key not in seen:        # dedupe within trip
                    seen.add(key)
                    out.append((key, float(length), j - i + 1))
    return out


subroutes_udf = F.udf(_subroutes, _SUBROUTE_SCHEMA)


def _report(name, lines):
    d = os.path.join(config.OUTPUT_BASE, "statistics")
    os.makedirs(d, exist_ok=True)
    p = os.path.join(d, name)
    with open(p, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return p


def main(use_sample: bool) -> None:
    spark = get_spark("route-mining-exact")
    t0 = time.time()

    res = config.H3_RESOLUTION
    enc_path = config.CLEAN_PARQUET.replace(
        ".parquet", f"_encoded_r{res}_sample.parquet" if use_sample
        else f"_encoded_r{res}_full.parquet")
    enc = spark.read.parquet(enc_path).select("TRIP_ID", "h3_seq_compact")
    n_trips = enc.count()

    # ---- emit windows, then aggregate to (subroute -> trip support) ----
    windows = enc.select(F.explode(subroutes_udf("h3_seq_compact")).alias("w")).select("w.*")
    windows.cache()
    n_windows = windows.count()   # shuffle input size (materialise once)

    agg = (
        windows.groupBy("subroute")
        .agg(F.count(F.lit(1)).alias("support"),      # deduped in-trip => trips
             F.first("length_km").alias("length_km"),
             F.first("n_cells").alias("n_cells"))
    )
    agg.cache()
    n_subroutes = agg.count()     # shuffle output size (distinct sub-routes)

    print("=" * 60)
    print(f"trips                : {n_trips:,}")
    print(f"window emissions     : {n_windows:,}   (groupBy shuffle INPUT)")
    print(f"distinct sub-routes  : {n_subroutes:,} (groupBy shuffle OUTPUT)")
    print("=" * 60)

    # ---- top-100 per threshold; collect small results (<=100 rows each) ----
    rep = [f"# M5 Exact Sub-route Mining ({'sample' if use_sample else 'full'})",
           f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
           f"\ntrips: {n_trips:,} | window emissions: {n_windows:,} | "
           f"distinct sub-routes: {n_subroutes:,}\n",
           "| min_len_km | #candidates(>=L) | top_support | median_support_top100 |",
           "|---|---|---|---|"]

    routes_dir = os.path.join(config.OUTPUT_BASE, "routes")
    os.makedirs(routes_dir, exist_ok=True)
    all_rows = []
    for L in THRESHOLDS_KM:
        cand = agg.filter(F.col("length_km") >= L)
        n_cand = cand.count()
        top = (cand.orderBy(F.col("support").desc(), F.col("length_km").desc())
               .limit(TOP_N).collect())
        top_support = top[0]["support"] if top else 0
        med = top[len(top) // 2]["support"] if top else 0
        rep.append(f"| {L} | {n_cand:,} | {top_support:,} | {med:,} |")
        for rank, r in enumerate(top, 1):
            all_rows.append((L, rank, r["support"], round(r["length_km"], 3),
                             r["n_cells"], r["subroute"]))

    # Write the combined top-100 table (<=600 rows) as readable CSV (ignored dir).
    import csv
    suffix = "sample" if use_sample else "full"
    csv_path = os.path.join(routes_dir, f"exact_top100_{suffix}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["min_len_km", "rank", "support", "length_km", "n_cells", "subroute"])
        w.writerows(all_rows)

    rp = _report(f"m5_exact_mining_{suffix}.md", rep)
    elapsed = time.time() - t0
    print("\n".join(rep))
    print(f"\n[m5] wrote top routes -> {csv_path}")
    print(f"[m5] wrote report     -> {rp}")
    print(f"[m5] wall time        : {elapsed:.1f}s")
    spark.stop()
    print("M5 EXACT MINING COMPLETE.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
