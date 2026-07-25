"""
verify_phase1.py
================
Independent verification of the Phase 1 output. We do NOT trust that the write
worked just because no exception was thrown -- we read the Parquet back and
inspect it, and we recompute WHY trips were rejected so the cleaning is
defensible.

Run:
    python -m src.verify_phase1 --sample
    python -m src.verify_phase1 --full
"""
from pyspark.sql import functions as F

from src import cli, config, storage
from src.spark_session import get_spark
from src.load_data import load_raw
from src.clean_data import parse_and_feature


def main(scale: str) -> None:
    spark = get_spark("verify-phase1")
    print(f"Spark version : {spark.version}")

    # ---- 1. Read the cleaned Parquet BACK (proves the write is valid) ----
    paths = config.dataset_paths(scale)
    parquet_path = paths["clean"]
    clean = spark.read.parquet(parquet_path)
    clean_n = clean.count()
    print(f"\n=== cleaned Parquet read back: {parquet_path}")
    print(f"rows after cleaning : {clean_n:,}")

    print("\n=== schema of cleaned Parquet ===")
    clean.printSchema()

    print("\n=== 5 sample rows (trajectory hidden, geometry shown) ===")
    clean.select(
        "TRIP_ID", "n_points", "duration_sec",
        F.round("start_lon", 5).alias("start_lon"),
        F.round("start_lat", 5).alias("start_lat"),
        F.round("end_lon", 5).alias("end_lon"),
        F.round("end_lat", 5).alias("end_lat"),
    ).show(5, truncate=False)

    # ---- 2. Verify POLYLINE parsed correctly ----
    # The parsed `points` array length MUST equal n_points for every row.
    mism = clean.filter(F.size("points") != F.col("n_points")).count()
    pts_stats = clean.select(
        F.min("n_points").alias("min_pts"),
        F.max("n_points").alias("max_pts"),
        F.round(F.avg("n_points"), 1).alias("avg_pts"),
    ).collect()[0]
    print("=== POLYLINE parse check ===")
    ok = True
    c_parse = (mism == 0)
    ok &= c_parse
    print(f"[{'OK' if c_parse else 'FAIL'}] size(points) == n_points for every row "
          f"({mism} mismatches, must be 0)")
    print(f"points per trip  min/avg/max : {pts_stats['min_pts']} / "
          f"{pts_stats['avg_pts']} / {pts_stats['max_pts']}")
    print("first trip first 3 coords (lon,lat):")
    first_pts = clean.select("points").first()["points"][:3]
    for p in first_pts:
        print(f"    {p}")

    # ---- 3. Recompute REJECTION REASONS from the raw input ----
    raw_path = paths["raw_csv"]
    raw = load_raw(spark, raw_path)
    raw_n = raw.count()
    df = parse_and_feature(raw)

    in_porto = (
        F.col("start_lon").between(*config.PORTO_LON_RANGE)
        & F.col("start_lat").between(*config.PORTO_LAT_RANGE)
        & F.col("end_lon").between(*config.PORTO_LON_RANGE)
        & F.col("end_lat").between(*config.PORTO_LAT_RANGE)
    )
    reasons = df.select(
        F.count(F.when(F.col("MISSING_DATA") != "False", True)).alias("missing_data_flag"),
        F.count(F.when(F.col("n_points") < config.MIN_POINTS, True)).alias("too_few_points"),
        F.count(F.when(F.col("n_points") > config.MAX_POINTS, True)).alias("too_many_points"),
        F.count(F.when(~in_porto, True)).alias("outside_porto_bbox"),
    ).collect()[0]

    print("\n=== row counts before/after & rejection reasons ===")
    print(f"rows BEFORE cleaning (raw) : {raw_n:,}")
    print(f"rows AFTER  cleaning       : {clean_n:,}")
    print(f"total dropped              : {raw_n - clean_n:,}")
    print("--- reason counts (note: a trip can fail several checks) ---")
    print(f"  MISSING_DATA == True   : {reasons['missing_data_flag']:,}")
    print(f"  too few points (<{config.MIN_POINTS}) : {reasons['too_few_points']:,}")
    print(f"  too many points (>{config.MAX_POINTS}): {reasons['too_many_points']:,}")
    print(f"  outside Porto bbox     : {reasons['outside_porto_bbox']:,}")

    # Real gates, not just printed counts: this file used to describe the data
    # and always return success, so nothing it "checked" could ever fail a run.
    c_rows = 0 < clean_n <= raw_n
    ok &= c_rows
    print(f"[{'OK' if c_rows else 'FAIL'}] cleaned rows in (0, raw]: "
          f"{clean_n:,} of {raw_n:,}")
    spark.stop()
    print("\n" + ("PHASE 1 VERIFICATION PASSED." if ok
                  else "PHASE 1 VERIFICATION FAILED."))
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
