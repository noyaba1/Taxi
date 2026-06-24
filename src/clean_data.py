"""
clean_data.py  --  PHASE 1 main script
======================================
Loads raw Porto Taxi CSV -> parses POLYLINE JSON -> extracts number of GPS
points & basic geometry -> removes invalid trips -> writes a clean Parquet.

Run on the SAMPLE first:
    python -m src.clean_data --sample
Then on the FULL file:
    python -m src.clean_data --full

WHY PARQUET (not CSV) for the output?
  - Columnar + compressed: ~5-10x smaller than CSV, much faster to re-read.
  - Stores the schema and the *parsed* array column, so we never re-parse JSON.
  - On DataProc/GCS this is the standard intermediate format.
"""
import argparse
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, DoubleType

from src.spark_session import get_spark
from src import config
from src.load_data import load_raw


# POLYLINE looks like: "[[-8.58, 41.14], [-8.58, 41.14], ...]"
# i.e. an array of [lon, lat] pairs -> array<array<double>>.
POLYLINE_TYPE = ArrayType(ArrayType(DoubleType()))


def parse_and_feature(df: DataFrame) -> DataFrame:
    """Parse the POLYLINE JSON and derive first-order features."""
    df = df.withColumn("points", F.from_json("POLYLINE", POLYLINE_TYPE))

    # n_points: how many GPS samples in the trajectory.
    df = df.withColumn("n_points", F.size("points"))

    # duration_sec: GPS every 15s => (n_points - 1) * 15. -1 because the first
    # sample is t=0. We guard against negative when n_points = 0.
    df = df.withColumn(
        "duration_sec",
        F.when(F.col("n_points") > 0, (F.col("n_points") - 1) * config.GPS_INTERVAL_SEC)
        .otherwise(F.lit(None)),
    )

    # start / end coordinates. element_at is 1-indexed; [1] = lon, [2] = lat.
    df = df.withColumn("start_lon", F.col("points")[0][0]) \
           .withColumn("start_lat", F.col("points")[0][1]) \
           .withColumn("end_lon", F.element_at("points", -1)[0]) \
           .withColumn("end_lat", F.element_at("points", -1)[1])

    # Human-readable start time from the unix TIMESTAMP.
    df = df.withColumn("start_time", F.from_unixtime("TIMESTAMP"))
    return df


def add_validity_flags(df: DataFrame) -> DataFrame:
    """Mark *why* a trip is invalid (we keep the reason for the EDA report)."""
    in_porto = (
        F.col("start_lon").between(*config.PORTO_LON_RANGE)
        & F.col("start_lat").between(*config.PORTO_LAT_RANGE)
        & F.col("end_lon").between(*config.PORTO_LON_RANGE)
        & F.col("end_lat").between(*config.PORTO_LAT_RANGE)
    )
    return df.withColumn(
        "is_valid",
        (F.col("MISSING_DATA") == "False")                 # GPS not flagged missing
        & (F.col("n_points") >= config.MIN_POINTS)          # has a direction
        & (F.col("n_points") <= config.MAX_POINTS)          # not corrupted-long
        & in_porto,                                         # inside Porto box
    )


def clean(df: DataFrame) -> DataFrame:
    df = parse_and_feature(df)
    df = add_validity_flags(df)
    return df


def main(use_sample: bool) -> None:
    spark = get_spark("clean-data")

    if use_sample:
        src_path = config.SAMPLE_CSV + "_dir"
        print(f"[clean] reading SAMPLE from {src_path}")
    else:
        src_path = config.RAW_TRAIN
        print(f"[clean] reading FULL file from {src_path}")

    raw = load_raw(spark, src_path)
    df = clean(raw)

    # --- Quality report BEFORE dropping (so we can defend our cleaning) ---
    total = df.count()
    valid = df.filter(F.col("is_valid")).count()
    print("=" * 50)
    print(f"  total trips       : {total:,}")
    print(f"  valid trips       : {valid:,}  ({100*valid/total:.1f}%)")
    print(f"  dropped (invalid) : {total - valid:,}")
    print("=" * 50)

    clean_df = (
        df.filter(F.col("is_valid"))
        .select(
            "TRIP_ID", "TAXI_ID", "CALL_TYPE", "TIMESTAMP", "start_time",
            "n_points", "duration_sec",
            "start_lon", "start_lat", "end_lon", "end_lat",
            "points",  # keep parsed trajectory for downstream phases
        )
    )

    out = config.CLEAN_PARQUET if not use_sample else config.CLEAN_PARQUET.replace(
        ".parquet", "_sample.parquet"
    )
    clean_df.write.mode("overwrite").parquet(out)
    print(f"[clean] wrote clean parquet -> {out}")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true", help="use the small sample")
    g.add_argument("--full", action="store_true", help="use the full 1.9GB file")
    args = ap.parse_args()
    main(use_sample=args.sample)
