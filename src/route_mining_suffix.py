"""
route_mining_suffix.py  --  PHASE 5 / Milestone M6  (MAXIMAL / closed routes)
============================================================================
Builds on the exact M5 n-gram support table and keeps only MAXIMAL (closed)
frequent sub-routes, so we don't report dozens of overlapping fragments of the
same corridor.

HOW THIS DIFFERS FROM M5
------------------------
M5 counts EVERY contiguous sub-route and returns the top-100 by support. Because
a popular corridor contains many popular fragments, the M5 lists are full of
near-duplicate prefixes/suffixes of the same route.

M6 keeps only CLOSED sub-routes. A sub-route `s` is closed iff no proper
contiguous super-route has (nearly) the same support. Support is monotonic under
extension, so the best support any super-route can reach equals the best
SINGLE-CELL extension's support. Hence:

    s is maximal  <=>  max(best_left_ext_support, best_right_ext_support)
                        <  support(s) * (1 - SUPPORT_TOL)

This is the suffix-tree "branching node" property (left/right maximal repeats)
computed directly over the sub-route set:
  * right-parent(t) = t without its LAST  cell   (t extends its right-parent)
  * left-parent(t)  = t without its FIRST cell   (t extends its left-parent)
  * group by parent, take max child support = best single-cell extension support.

SUPPORT_TOL = 0.0 gives the classical closed-frequent-substring set (drop s only
if a longer route has EQUAL support). Raising it also collapses near-equal
containments ("mostly contained ... with similar support").

Run:
    python -m src.route_mining_suffix --sample
"""
import argparse
import csv
import os
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.spark_session import get_spark
from src import config
# Reuse the EXACT M5 window emitter so the support table is identical.
from src.route_mining_exact import subroutes_udf, THRESHOLDS_KM, TOP_N, DELIM

SUPPORT_TOL = 0.0   # 0.0 = classical closed; raise (e.g. 0.1) to merge near-equal


def build_support_table(spark, use_sample):
    """Rebuild the M5 (subroute -> distinct-trip support) table."""
    res = config.H3_RESOLUTION
    suffix = "sample" if use_sample else "full"
    enc_path = config.CLEAN_PARQUET.replace(".parquet", f"_encoded_r{res}_{suffix}.parquet")
    enc = spark.read.parquet(enc_path).select("TRIP_ID", "h3_seq_compact")
    windows = enc.select(F.explode(subroutes_udf("h3_seq_compact")).alias("w")).select("w.*")
    agg = (windows.groupBy("subroute")
           .agg(F.count(F.lit(1)).alias("support"),
                F.first("length_km").alias("length_km"),
                F.first("n_cells").alias("n_cells")))
    return enc.count(), agg


def keep_maximal(agg):
    """Filter `agg` to closed/maximal sub-routes via single-cell extension check."""
    cells = F.split(F.col("subroute"), DELIM)
    # Parents that THIS row extends (only meaningful when n_cells >= 3, since a
    # 2-cell route's parent is a single cell that is never in `agg`).
    with_parents = agg.withColumn(
        "right_parent", F.array_join(F.slice(cells, F.lit(1), F.col("n_cells") - 1), DELIM)
    ).withColumn(
        "left_parent", F.array_join(F.slice(cells, F.lit(2), F.col("n_cells") - 1), DELIM)
    )
    best_right = with_parents.groupBy("right_parent").agg(F.max("support").alias("r_ext"))
    best_left = with_parents.groupBy("left_parent").agg(F.max("support").alias("l_ext"))

    joined = (
        agg.join(best_right, agg.subroute == best_right.right_parent, "left").drop("right_parent")
           .join(best_left, agg.subroute == best_left.left_parent, "left").drop("left_parent")
    )
    max_ext = F.greatest(F.coalesce(F.col("r_ext"), F.lit(0)),
                         F.coalesce(F.col("l_ext"), F.lit(0)))
    return joined.withColumn("max_ext_support", max_ext).filter(
        F.col("max_ext_support") < F.col("support") * (1.0 - SUPPORT_TOL)
    ).select("subroute", "support", "length_km", "n_cells", "max_ext_support")


def _report(name, lines):
    d = os.path.join(config.OUTPUT_BASE, "statistics")
    os.makedirs(d, exist_ok=True)
    p = os.path.join(d, name)
    with open(p, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return p


def main(use_sample: bool) -> None:
    spark = get_spark("route-mining-suffix")
    t0 = time.time()
    suffix = "sample" if use_sample else "full"

    n_trips, agg = build_support_table(spark, use_sample)
    agg.cache()
    n_all = agg.count()

    maximal = keep_maximal(agg)
    maximal.cache()
    n_max = maximal.count()

    print("=" * 60)
    print(f"trips                    : {n_trips:,}")
    print(f"all sub-routes (M5)      : {n_all:,}")
    print(f"maximal sub-routes (M6)  : {n_max:,}  "
          f"({100*n_max/n_all:.1f}% kept, {n_all-n_max:,} redundant dropped)")
    print("=" * 60)

    rep = [f"# M6 Suffix-style Maximal Route Mining ({suffix})",
           f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
           f"\ntrips: {n_trips:,} | SUPPORT_TOL={SUPPORT_TOL}",
           f"all sub-routes (M5): {n_all:,} | maximal (M6): {n_max:,} "
           f"({100*n_max/n_all:.1f}% kept)\n",
           "| min_len_km | M5 candidates | M6 maximal | reduction | top_support |",
           "|---|---|---|---|---|"]

    routes_dir = os.path.join(config.OUTPUT_BASE, "routes")
    os.makedirs(routes_dir, exist_ok=True)
    all_rows = []
    for L in THRESHOLDS_KM:
        m5_cand = agg.filter(F.col("length_km") >= L).count()
        cand = maximal.filter(F.col("length_km") >= L)
        m6_cand = cand.count()
        top = (cand.orderBy(F.col("support").desc(), F.col("length_km").desc())
               .limit(TOP_N).collect())
        top_support = top[0]["support"] if top else 0
        reduction = f"{100*(1 - m6_cand/m5_cand):.1f}%" if m5_cand else "n/a"
        rep.append(f"| {L} | {m5_cand:,} | {m6_cand:,} | {reduction} | {top_support:,} |")
        for rank, r in enumerate(top, 1):
            all_rows.append((L, rank, r["support"], round(r["length_km"], 3),
                             r["n_cells"], r["subroute"]))

    csv_path = os.path.join(routes_dir, f"suffix_maximal_top100_{suffix}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["min_len_km", "rank", "support", "length_km", "n_cells", "subroute"])
        w.writerows(all_rows)

    rp = _report(f"m6_suffix_mining_{suffix}.md", rep)
    elapsed = time.time() - t0
    print("\n".join(rep))
    print(f"\n[m6] wrote maximal routes -> {csv_path}")
    print(f"[m6] wrote report         -> {rp}")
    print(f"[m6] wall time            : {elapsed:.1f}s")
    spark.stop()
    print("M6 MAXIMAL MINING COMPLETE.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
