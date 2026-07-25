"""
anomaly_analysis.py  --  Milestone M11  (anomalous-route detection)
==================================================================
The assignment asks us to "identify anomalous routes". We combine several
independent, individually-defensible detectors over the Phase-2 feature table
(no recompute) into a per-trip anomaly score:

  1. speed     : is_teleport OR is_too_fast   (GPS jump / impossible speed)
  2. idle      : is_idle                       (parked / no movement)
  3. distance  : total_distance_km  > p99      (unusually long trip)
  4. shape     : sinuosity          > p99      (excessive detour / looping)
  5. drift     : the route's bounding box escapes the Porto metro bbox
                 (intermediate GPS left the city -> the M10 zone finding, formalised)

Percentile fences use approxQuantile (Greenwald-Khanna sketch) so the SAME code
scales without an O(N log N) sort. anomaly_score = number of detectors firing.

Run:
    python -m src.anomaly_analysis --sample
"""
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src import cli, config, storage
from src.spark_session import get_spark

log = cli.setup_logging("m11")

DETECTORS = ["a_speed", "a_idle", "a_distance", "a_shape", "a_drift"]


def add_anomaly_flags(df):
    """Attach the 5 detector booleans + anomaly_score. p99 fences via sketch."""
    p = config.ANOMALY_PCT
    # Compute the fence on NORMAL trips only: outliers (GPS teleports) must not set
    # their own threshold, or the p99 fence is dragged to ~max and flags nothing.
    # relativeError 0.001 (not 0.01): on a heavy tail, coarse error returns ~max
    # for a high quantile and the fence would flag nothing.
    normal = df.filter(~(F.col("is_teleport") | F.col("is_too_fast")))

    def _fence(col, fallback):
        # approxQuantile returns [] when the column is empty or all-null (a tiny
        # sample, or a run where every trip tripped a speed flag). Falling back to
        # +inf disables that detector instead of crashing the stage.
        q = normal.approxQuantile(col, [p], 0.001)
        return q[0] if q and q[0] is not None else fallback

    dist_hi = _fence("total_distance_km", float("inf"))
    sinu_hi = _fence("sinuosity", float("inf"))
    m = config.ANOMALY_METRO_MARGIN
    lon_lo, lon_hi = config.PORTO_LON_RANGE[0] - m, config.PORTO_LON_RANGE[1] + m
    lat_lo, lat_hi = config.PORTO_LAT_RANGE[0] - m, config.PORTO_LAT_RANGE[1] + m

    df = (df
          .withColumn("a_speed", F.col("is_teleport") | F.col("is_too_fast"))
          .withColumn("a_idle", F.col("is_idle"))
          .withColumn("a_distance", F.col("total_distance_km") > F.lit(dist_hi))
          .withColumn("a_shape", F.coalesce(F.col("sinuosity"), F.lit(0.0)) > F.lit(sinu_hi))
          .withColumn("a_drift",
                      (F.col("min_lon") < lon_lo) | (F.col("max_lon") > lon_hi)
                      | (F.col("min_lat") < lat_lo) | (F.col("max_lat") > lat_hi)))
    score = sum(F.col(c).cast("int") for c in DETECTORS)
    return df.withColumn("anomaly_score", score), dist_hi, sinu_hi


def main(scale: str) -> None:
    spark = get_spark("anomaly-analysis")
    paths = config.dataset_paths(scale)

    with cli.stage("m11_anomalies", scale, log) as st:
        # NOTE: reads the UNFILTERED feature table on purpose. spatial_encoding
        # drops anomalous trips before mining because they corrupt route support;
        # studying them is this stage's whole job, so it must see them.
        df = spark.read.parquet(paths["features"])
        df, dist_hi, sinu_hi = add_anomaly_flags(df)
        df.cache()
        n = df.count()

        # per-detector counts + overall, in one pass
        agg = df.select(
            *[F.sum(F.col(c).cast("int")).alias(c) for c in DETECTORS],
            F.sum((F.col("anomaly_score") > 0).cast("int")).alias("any"),
            F.sum((F.col("anomaly_score") >= 2).cast("int")).alias("multi"),
        ).collect()[0].asDict()

        log.info("=" * 56)
        log.info("trips              : %s", f"{n:,}")
        log.info("p99 distance / sinuosity : %.2f km / %.2f", dist_hi, sinu_hi)
        for c in DETECTORS:
            log.info("  %-12s: %6s  (%5.2f%%)", c, f"{agg[c]:,}", 100 * agg[c] / n)
        log.info("  ANY anomaly : %s (%.2f%%) | >=2 detectors: %s",
                 f"{agg['any']:,}", 100 * agg['any'] / n, f"{agg['multi']:,}")
        log.info("=" * 56)

        # top anomalies (most detectors, then longest) for the report / map
        cols = ["TRIP_ID", "anomaly_score", *DETECTORS, "dist_km", "speed",
                "max_seg", "sinuosity", "start_lat", "start_lon", "end_lat", "end_lon"]
        top = (df.filter(F.col("anomaly_score") >= 1)
               .orderBy(F.col("anomaly_score").desc(), F.col("total_distance_km").desc())
               .limit(50)
               .select("TRIP_ID", "anomaly_score", *DETECTORS,
                       F.round("total_distance_km", 2).alias("dist_km"),
                       F.round("avg_speed_kmh", 1).alias("speed"),
                       F.round("max_seg_speed_kmh", 1).alias("max_seg"),
                       F.round("sinuosity", 2).alias("sinuosity"),
                       "start_lat", "start_lon", "end_lat", "end_lon").collect())

        apath = storage.write_csv(
            storage.out_path("routes", f"anomalies_top50_{scale}.csv"),
            cols, [[r[c] for c in cols] for r in top])

        rep = [f"# M11 Anomalous Route Analysis ({scale})",
               f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
               "",
               f"trips: {n:,} | p99 distance={dist_hi:.2f} km, p99 sinuosity={sinu_hi:.2f}",
               "",
               "| detector | count | pct |", "|---|---|---|"]
        for c in DETECTORS:
            rep.append(f"| {c} | {agg[c]:,} | {100 * agg[c] / n:.2f}% |")
        rep.append(f"| **any** | {agg['any']:,} | {100 * agg['any'] / n:.2f}% |")
        rep.append(f"| >=2 detectors | {agg['multi']:,} | {100 * agg['multi'] / n:.2f}% |")

        rp = storage.write_lines(
            storage.out_path("statistics", f"m11_anomalies_{scale}.md"), rep)

        log.info("\n%s", "\n".join(rep))
        log.info("wrote anomalies -> %s", apath)
        log.info("wrote report    -> %s", rp)
        st.update(trips=n, **{c: int(agg[c]) for c in DETECTORS},
                  any_anomaly=int(agg["any"]))

    spark.stop()
    log.info("M11 ANOMALY ANALYSIS COMPLETE.")


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
