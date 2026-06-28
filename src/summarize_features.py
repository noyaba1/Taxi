"""
summarize_features.py  --  M1 validation & summary for Phase 2
=============================================================
Reads the feature Parquet BACK (never trust a silent write), verifies schema,
row count and sample rows, then produces distribution statistics and an anomaly
breakdown. Writes a small human-readable report under outputs/statistics/.

Quantiles use Spark's approxQuantile (Greenwald-Khanna sketch) so the SAME code
scales to the full file / DataProc without an O(N log N) global sort.

Run:
    python -m src.summarize_features --sample
    python -m src.summarize_features --full
"""
import argparse
import os
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.spark_session import get_spark
from src import config

# Numeric features we profile (distance/speed/duration/shape).
NUMERIC = [
    "total_distance_km", "straight_line_km", "duration_sec",
    "avg_speed_kmh", "max_seg_speed_kmh", "sinuosity",
]
FLAGS = ["is_teleport", "is_idle", "is_too_fast", "is_too_short", "is_anomalous"]
QUANTILES = [0.0, 0.25, 0.5, 0.75, 0.95, 1.0]


def main(use_sample: bool) -> None:
    spark = get_spark("summarize-features")
    print(f"Spark version : {spark.version}")

    path = config.CLEAN_PARQUET.replace(".parquet", "_features_sample.parquet") \
        if use_sample else config.CLEAN_PARQUET.replace(".parquet", "_features.parquet")
    df = spark.read.parquet(path)
    n = df.count()

    print(f"\n=== feature Parquet read back: {path}")
    print(f"rows : {n:,}")
    print("\n=== schema ===")
    df.printSchema()
    print("=== 5 sample rows ===")
    df.select("TRIP_ID", "n_points", "duration_sec",
              F.round("total_distance_km", 3).alias("dist_km"),
              F.round("straight_line_km", 3).alias("straight_km"),
              F.round("avg_speed_kmh", 1).alias("speed"),
              F.round("sinuosity", 2).alias("sinuosity"),
              "is_anomalous").show(5, truncate=False)

    # ---- Distribution stats (mean/stddev exact; quantiles via GK sketch) ----
    means = df.select(
        *[F.round(F.avg(c), 3).alias(c) for c in NUMERIC]
    ).collect()[0].asDict()
    stds = df.select(
        *[F.round(F.stddev(c), 3).alias(c) for c in NUMERIC]
    ).collect()[0].asDict()
    # approxQuantile handles nulls by ignoring them; 1% relative error.
    qmap = {c: df.approxQuantile(c, QUANTILES, 0.01) for c in NUMERIC}

    # ---- Anomaly flag breakdown (single pass) ----
    flag_row = df.select(
        *[F.sum(F.col(c).cast("int")).alias(c) for c in FLAGS]
    ).collect()[0].asDict()

    # ---- Build report text ----
    lines = []
    lines.append(f"# Phase 2 Feature Summary ({'sample' if use_sample else 'full'})")
    lines.append(f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_")
    lines.append(f"\nrows: {n:,}  |  source: {os.path.basename(path)}\n")

    lines.append("## Distribution statistics")
    header = "| feature | mean | stddev | min | p25 | p50 | p75 | p95 | max |"
    lines.append(header)
    lines.append("|" + "---|" * 9)
    for c in NUMERIC:
        q = qmap[c]
        def f(x): return f"{x:,.3f}" if x is not None else "NA"
        lines.append(
            f"| {c} | {f(means[c])} | {f(stds[c])} | "
            f"{f(q[0])} | {f(q[1])} | {f(q[2])} | {f(q[3])} | {f(q[4])} | {f(q[5])} |"
        )

    lines.append("\n## Anomaly flags")
    lines.append("| flag | count | pct |")
    lines.append("|---|---|---|")
    for c in FLAGS:
        cnt = flag_row[c] or 0
        lines.append(f"| {c} | {cnt:,} | {100*cnt/n:.2f}% |")

    report = "\n".join(lines) + "\n"
    print("\n" + report)

    # ---- Save small report (git-ignored, see .gitignore) ----
    out_dir = os.path.join(config.OUTPUT_BASE, "statistics")
    os.makedirs(out_dir, exist_ok=True)
    suffix = "sample" if use_sample else "full"
    out_file = os.path.join(out_dir, f"phase2_feature_summary_{suffix}.md")
    with open(out_file, "w", encoding="utf-8") as fh:
        fh.write(report)
    print(f"[summary] wrote report -> {out_file}")

    spark.stop()
    print("\nM1 SUMMARY COMPLETE.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
