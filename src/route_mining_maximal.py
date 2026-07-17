"""
route_mining_maximal.py  --  PHASE 5 / Milestone M8  (PDF-aligned definition)
============================================================================
"Popular long sub-route" exactly as the lecturer's PDF defines it:

    keep a CONTIGUOUS sub-route iff >= X% of trips traversed it (frequent),
    and it is MAXIMAL: no one-cell extension is still frequent.

Because support is monotonic under extension, "maximal among frequent" == the
LONGEST contiguous stretch that still clears X% -> this is the PDF's "maximise
the sub-route length subject to >= X% of trips".

THE "HOLES" (as in the Haifa->Ashdod example): a maximal-frequent sub-route
TERMINATES exactly where traffic forks (each continuation drops below X%). So a
popular corridor appears as a COLLECTION of contiguous sub-routes with holes at
the divergence points. We report those forks explicitly (holes analysis).

Relation to earlier work:
  * M5 (route_mining_exact)  = top-k by raw support per length (a proxy).
  * M6 (route_mining_suffix) = closed (no EQUAL-support extension), no X% floor.
  * M8 (this file)           = frequent (>= X%) AND maximal -> the PDF definition.
It reuses the SAME support table builder as M6 (one build, shared).

Run:
    python -m src.route_mining_maximal --sample
"""
import argparse
import csv
import math
import os
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.spark_session import get_spark
from src import config
from src.route_mining_exact import DELIM
from src.route_mining_suffix import build_support_table

THRESHOLDS = config.ROUTE_LENGTH_THRESHOLDS_KM
TOP_K = config.TOP_K


def _with_parents(agg):
    """Attach right_parent (drop last cell) and left_parent (drop first cell)."""
    cells = F.split(F.col("subroute"), DELIM)
    return (agg
            .withColumn("right_parent", F.array_join(F.slice(cells, F.lit(1), F.col("n_cells") - 1), DELIM))
            .withColumn("left_parent", F.array_join(F.slice(cells, F.lit(2), F.col("n_cells") - 1), DELIM)))


def keep_maximal_frequent(agg, min_sup: int):
    """
    Maximal-frequent contiguous sub-routes at absolute support floor `min_sup`.
    s kept  <=>  support(s) >= min_sup  AND  best single-cell extension < min_sup.
    """
    wp = _with_parents(agg)
    best_right = wp.groupBy("right_parent").agg(F.max("support").alias("r_ext"))
    best_left = wp.groupBy("left_parent").agg(F.max("support").alias("l_ext"))
    joined = (agg
              .join(best_right, agg.subroute == best_right.right_parent, "left").drop("right_parent")
              .join(best_left, agg.subroute == best_left.left_parent, "left").drop("left_parent"))
    max_ext = F.greatest(F.coalesce(F.col("r_ext"), F.lit(0)), F.coalesce(F.col("l_ext"), F.lit(0)))
    return (joined.withColumn("max_ext_support", max_ext)
            .filter((F.col("support") >= min_sup) & (F.col("max_ext_support") < min_sup))
            .select("subroute", "support", "length_km", "n_cells", "max_ext_support"))


def _report(name, lines):
    d = os.path.join(config.OUTPUT_BASE, "statistics")
    os.makedirs(d, exist_ok=True)
    p = os.path.join(d, name)
    with open(p, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return p


def main(use_sample: bool) -> None:
    spark = get_spark("route-mining-maximal")
    t0 = time.time()
    suffix = "sample" if use_sample else "full"

    n_trips, agg = build_support_table(spark, use_sample)
    agg.cache()
    n_all = agg.count()

    x_pct = config.SUPPORT_X_PCT
    min_sup = max(2, math.ceil(x_pct / 100.0 * n_trips))   # at least 2 trips
    maximal = keep_maximal_frequent(agg, min_sup)
    maximal.cache()
    n_max = maximal.count()

    print("=" * 62)
    print(f"trips                    : {n_trips:,}")
    print(f"X% support               : {x_pct}%  -> min_sup = {min_sup} trips")
    print(f"all sub-routes           : {n_all:,}")
    print(f"maximal-frequent routes  : {n_max:,}")
    print("=" * 62)

    rep = [f"# M8 Min-Support Maximal Sub-routes ({suffix}) -- PDF definition",
           f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
           f"\ntrips: {n_trips:,} | X% = {x_pct}% (min_sup={min_sup}) | "
           f"all sub-routes: {n_all:,} | maximal-frequent: {n_max:,}\n",
           "## Top maximal-frequent routes per length config",
           "| min_len_km | #maximal(>=L) | top_support | longest_km |",
           "|---|---|---|---|"]

    routes_dir = os.path.join(config.OUTPUT_BASE, "routes")
    os.makedirs(routes_dir, exist_ok=True)
    all_rows = []
    for L in THRESHOLDS:
        cand = maximal.filter(F.col("length_km") >= L)
        n_cand = cand.count()
        top = (cand.orderBy(F.col("support").desc(), F.col("length_km").desc())
               .limit(TOP_K).collect())
        top_support = top[0]["support"] if top else 0
        longest = max((r["length_km"] for r in top), default=0.0)
        rep.append(f"| {L} | {n_cand:,} | {top_support:,} | {longest:.2f} |")
        for rank, r in enumerate(top, 1):
            all_rows.append((L, rank, r["support"], round(r["length_km"], 3),
                             r["n_cells"], r["subroute"]))

    csv_path = os.path.join(routes_dir, f"maximal_frequent_top100_{suffix}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["min_len_km", "rank", "support", "length_km", "n_cells", "subroute"])
        w.writerows(all_rows)

    # ---- X% sweep (the PDF asks us to experiment with X) ----
    rep += ["\n## X% sweep (experiment)",
            "| X% | min_sup | #maximal-frequent | longest_km |", "|---|---|---|---|"]
    for x in config.SUPPORT_X_PCT_SWEEP:
        ms = max(2, math.ceil(x / 100.0 * n_trips))
        m = keep_maximal_frequent(agg, ms)
        row = m.agg(F.count(F.lit(1)).alias("n"), F.max("length_km").alias("mx")).collect()[0]
        rep.append(f"| {x} | {ms} | {row['n']:,} | {(row['mx'] or 0):.2f} |")

    # ---- HOLES analysis: where do the top maximal routes terminate? ----
    # A maximal route ends because its continuations each fall below X%. Show the
    # top continuations (forks) for the highest-support maximal routes.
    rep += ["\n## Holes analysis (why maximal routes terminate = traffic forks)",
            f"For each top route: support S, then its best right-continuations "
            f"(each < min_sup={min_sup} => a HOLE forms here).", ""]
    top_max = [r["subroute"] for r in
               maximal.orderBy(F.col("support").desc()).limit(5).collect()]
    children = (_with_parents(agg)
                .select(F.col("right_parent").alias("parent"),
                        F.col("subroute").alias("child"),
                        F.col("support").alias("child_support")))
    forks = {p: [] for p in top_max}
    for r in children.filter(F.col("parent").isin(top_max)).collect():
        forks[r["parent"]].append((r["child_support"], r["child"]))
    sup_of = {r["subroute"]: r["support"]
              for r in maximal.filter(F.col("subroute").isin(top_max)).collect()}
    for p in top_max:
        branches = sorted(forks.get(p, []), reverse=True)[:3]
        tail = " ; ".join(f"branch_sup={s}" for s, _ in branches) or "(no continuation)"
        rep.append(f"- route(support={sup_of.get(p)}, {p.count(DELIM)+1} cells): forks -> {tail}")

    rp = _report(f"m8_maximal_mining_{suffix}.md", rep)
    elapsed = time.time() - t0
    print("\n".join(rep))
    print(f"\n[m8] wrote maximal-frequent routes -> {csv_path}")
    print(f"[m8] wrote report                  -> {rp}")
    print(f"[m8] wall time                     : {elapsed:.1f}s")
    spark.stop()
    print("M8 MAXIMAL-FREQUENT MINING COMPLETE.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
