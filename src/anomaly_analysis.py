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
import argparse
import csv
import os
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src.spark_session import get_spark
from src import config

DETECTORS = ["a_speed", "a_idle", "a_distance", "a_shape", "a_drift"]


def add_anomaly_flags(df):
    """Attach the 5 detector booleans + anomaly_score. p99 fences via sketch."""
    p = config.ANOMALY_PCT
    # Compute the fence on NORMAL trips only: outliers (GPS teleports) must not set
    # their own threshold, or the p99 fence is dragged to ~max and flags nothing.
    # relativeError 0.001 (not 0.01): on a heavy tail, coarse error returns ~max
    # for a high quantile and the fence would flag nothing.
    normal = df.filter(~(F.col("is_teleport") | F.col("is_too_fast")))
    dist_hi = normal.approxQuantile("total_distance_km", [p], 0.001)[0]
    sinu_hi = normal.approxQuantile("sinuosity", [p], 0.001)[0]
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


def main(use_sample: bool) -> None:
    spark = get_spark("anomaly-analysis")
    suffix = "sample" if use_sample else "full"
    feat_path = config.CLEAN_PARQUET.replace(
        ".parquet", "_features_sample.parquet" if use_sample else "_features.parquet")
    df = spark.read.parquet(feat_path)
    df, dist_hi, sinu_hi = add_anomaly_flags(df)
    df.cache()
    n = df.count()

    # per-detector counts + overall, in one pass
    agg = df.select(
        *[F.sum(F.col(c).cast("int")).alias(c) for c in DETECTORS],
        F.sum((F.col("anomaly_score") > 0).cast("int")).alias("any"),
        F.sum((F.col("anomaly_score") >= 2).cast("int")).alias("multi"),
    ).collect()[0].asDict()

    print("=" * 56)
    print(f"trips              : {n:,}")
    print(f"p99 distance / sinuosity : {dist_hi:.2f} km / {sinu_hi:.2f}")
    for c in DETECTORS:
        print(f"  {c:<12}: {agg[c]:>6,}  ({100*agg[c]/n:5.2f}%)")
    print(f"  ANY anomaly : {agg['any']:,} ({100*agg['any']/n:.2f}%) | "
          f">=2 detectors: {agg['multi']:,}")
    print("=" * 56)

    # top anomalies (most detectors, then longest) for the report / map
    top = (df.filter(F.col("anomaly_score") >= 1)
           .orderBy(F.col("anomaly_score").desc(), F.col("total_distance_km").desc())
           .limit(50)
           .select("TRIP_ID", "anomaly_score", *DETECTORS,
                   F.round("total_distance_km", 2).alias("dist_km"),
                   F.round("avg_speed_kmh", 1).alias("speed"),
                   F.round("max_seg_speed_kmh", 1).alias("max_seg"),
                   F.round("sinuosity", 2).alias("sinuosity"),
                   "start_lat", "start_lon", "end_lat", "end_lon").collect())

    routes_dir = os.path.join(config.OUTPUT_BASE, "routes")
    os.makedirs(routes_dir, exist_ok=True)
    apath = os.path.join(routes_dir, f"anomalies_top50_{suffix}.csv")
    with open(apath, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        cols = ["TRIP_ID", "anomaly_score", *DETECTORS, "dist_km", "speed",
                "max_seg", "sinuosity", "start_lat", "start_lon", "end_lat", "end_lon"]
        w.writerow(cols)
        for r in top:
            w.writerow([r[c] for c in cols])

    rep = [f"# M11 Anomalous Route Analysis ({suffix})",
           f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
           f"\ntrips: {n:,} | p99 distance={dist_hi:.2f} km, p99 sinuosity={sinu_hi:.2f}\n",
           "| detector | count | pct |", "|---|---|---|"]
    for c in DETECTORS:
        rep.append(f"| {c} | {agg[c]:,} | {100*agg[c]/n:.2f}% |")
    rep.append(f"| **any** | {agg['any']:,} | {100*agg['any']/n:.2f}% |")
    rep.append(f"| >=2 detectors | {agg['multi']:,} | {100*agg['multi']/n:.2f}% |")

    rp = os.path.join(config.OUTPUT_BASE, "statistics", f"m11_anomalies_{suffix}.md")
    os.makedirs(os.path.dirname(rp), exist_ok=True)
    with open(rp, "w", encoding="utf-8") as fh:
        fh.write("\n".join(rep) + "\n")

    print("\n".join(rep))
    print(f"\n[m11] wrote anomalies -> {apath}")
    print(f"[m11] wrote report    -> {rp}")
    spark.stop()
    print("M11 ANOMALY ANALYSIS COMPLETE.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
