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
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src import cli, config, storage
from src.spark_session import get_spark

log = cli.setup_logging("m1")

# Numeric features we profile (distance/speed/duration/shape).
NUMERIC = [
    "total_distance_km", "straight_line_km", "duration_sec",
    "avg_speed_kmh", "max_seg_speed_kmh", "sinuosity",
]
FLAGS = ["is_teleport", "is_idle", "is_too_fast", "is_too_short",
         "is_offgrid", "is_anomalous"]
QUANTILES = [0.0, 0.25, 0.5, 0.75, 0.95, 1.0]


def main(scale: str) -> None:
    spark = get_spark("summarize-features")
    paths = config.dataset_paths(scale)

    with cli.stage("m1_summary", scale, log) as st:
        df = spark.read.parquet(paths["features"])
        n = df.count()

        log.info("feature parquet read back: %s (%s rows)", paths["features"], f"{n:,}")
        df.printSchema()
        df.select("TRIP_ID", "n_points", "duration_sec",
                  F.round("total_distance_km", 3).alias("dist_km"),
                  F.round("straight_line_km", 3).alias("straight_km"),
                  F.round("avg_speed_kmh", 1).alias("speed"),
                  F.round("sinuosity", 2).alias("sinuosity"),
                  "is_anomalous").show(5, truncate=False)

        # ---- Distribution stats (mean/stddev exact; quantiles via GK sketch) ----
        means = df.select(*[F.round(F.avg(c), 3).alias(c) for c in NUMERIC]
                          ).collect()[0].asDict()
        stds = df.select(*[F.round(F.stddev(c), 3).alias(c) for c in NUMERIC]
                         ).collect()[0].asDict()
        qmap = {c: df.approxQuantile(c, QUANTILES, 0.01) for c in NUMERIC}
        flag_row = df.select(*[F.sum(F.col(c).cast("int")).alias(c) for c in FLAGS]
                             ).collect()[0].asDict()

        lines = [f"# Phase 2 Feature Summary ({scale})",
                 f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
                 "",
                 f"rows: {n:,}  |  source: {paths['features']}",
                 "",
                 "## Distribution statistics",
                 "| feature | mean | stddev | min | p25 | p50 | p75 | p95 | max |",
                 "|" + "---|" * 9]

        def f(x):
            return f"{x:,.3f}" if x is not None else "NA"

        for c in NUMERIC:
            q = qmap[c]
            q = q + [None] * (len(QUANTILES) - len(q))     # empty column -> NA
            lines.append(
                f"| {c} | {f(means[c])} | {f(stds[c])} | "
                + " | ".join(f(v) for v in q) + " |")

        lines += ["", "## Anomaly flags",
                  "(counted on the UNFILTERED feature table; spatial_encoding "
                  "excludes `is_anomalous` trips before mining)",
                  "| flag | count | pct |", "|---|---|---|"]
        for c in FLAGS:
            cnt = flag_row[c] or 0
            lines.append(f"| {c} | {cnt:,} | {100 * cnt / n:.2f}% |")

        rp = storage.write_lines(
            storage.out_path("statistics", f"phase2_feature_summary_{scale}.md"), lines)
        log.info("\n%s", "\n".join(lines))
        log.info("wrote report -> %s", rp)
        st.update(rows_in=n, rows_out=n)

    spark.stop()
    log.info("M1 SUMMARY COMPLETE.")


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
