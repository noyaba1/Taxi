"""
clean_data.py  --  PHASE 1 main script
======================================
Loads raw Porto Taxi CSV -> parses POLYLINE JSON -> extracts number of GPS
points & basic geometry -> removes invalid trips -> writes a clean Parquet.

    python -m src.clean_data --sample     # 5k trips, seconds
    python -m src.clean_data --mid        # 200k trips, real shuffle
    python -m src.clean_data --full       # the whole 1.9 GB file

WHY PARQUET (not CSV) for the output?
  - Columnar + compressed: ~5-10x smaller than CSV, much faster to re-read.
  - Stores the schema and the *parsed* array column, so we never re-parse JSON.
  - On DataProc/GCS this is the standard intermediate format.

WHAT WE REJECT AND WHY (the assignment's "think about what in the data may
affect your results"). Each rule is counted separately and written to
outputs/statistics/, because "we cleaned the data" is not a defensible claim
without the numbers:

  MISSING_DATA=True  the vendor already flagged gaps in the GPS stream, so the
                     trajectory is not a continuous path.
  n_points < 2       no direction; cannot be a route.
  n_points > 4000    ~16.6 h of continuous sampling; a stuck meter, not a trip.
  endpoints off-map  start or end outside the Porto metro box.
  duplicate TRIP_ID  the same trip counted twice would inflate the support of
                     every sub-route it contains and of every cluster it joins.

Trajectory-level corruption (a mid-route GPS teleport) is NOT visible here --
it needs the per-segment speeds computed in Phase 2 -- so it is caught by
`is_anomalous` there and excluded before encoding.
"""
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, DoubleType

from src import cli, config, storage
from src.load_data import load_raw
from src.spark_session import get_spark

# POLYLINE looks like: "[[-8.58, 41.14], [-8.58, 41.14], ...]"
# i.e. an array of [lon, lat] pairs -> array<array<double>>.
POLYLINE_TYPE = ArrayType(ArrayType(DoubleType()))

log = cli.setup_logging("clean")


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


def rejection_counts(df: DataFrame) -> dict:
    """One pass over the flagged frame: how many trips fail each rule."""
    in_porto = (
        F.col("start_lon").between(*config.PORTO_LON_RANGE)
        & F.col("start_lat").between(*config.PORTO_LAT_RANGE)
        & F.col("end_lon").between(*config.PORTO_LON_RANGE)
        & F.col("end_lat").between(*config.PORTO_LAT_RANGE)
    )
    row = df.select(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.col("is_valid").cast("int")).alias("valid"),
        F.sum((F.col("MISSING_DATA") != "False").cast("int")).alias("missing_data_flag"),
        F.sum((F.col("n_points") < config.MIN_POINTS).cast("int")).alias("too_few_points"),
        F.sum((F.col("n_points") > config.MAX_POINTS).cast("int")).alias("too_many_points"),
        F.sum((~in_porto).cast("int")).alias("endpoints_off_map"),
    ).collect()[0].asDict()
    return {k: (v or 0) for k, v in row.items()}


def main(scale: str) -> None:
    spark = get_spark("clean-data")
    paths = config.dataset_paths(scale)

    with cli.stage("p1_clean", scale, log) as st:
        src_path = paths["raw_csv"]
        log.info("reading %s data from %s", scale, src_path)

        raw = load_raw(spark, src_path)
        df = clean(raw)
        df.cache()

        # --- Quality report BEFORE dropping (so we can defend our cleaning) ---
        counts = rejection_counts(df)
        total, valid = counts["total"], counts["valid"]
        if total == 0:
            raise RuntimeError(f"no rows read from {src_path!r}; check the path")

        kept = (
            df.filter(F.col("is_valid"))
            .select(
                "TRIP_ID", "TAXI_ID", "CALL_TYPE", "TIMESTAMP", "start_time",
                "n_points", "duration_sec",
                "start_lon", "start_lat", "end_lon", "end_lat",
                "points",  # keep parsed trajectory for downstream phases
            )
        )
        # A repeated TRIP_ID would be counted twice by every support metric
        # downstream. Deduplicate once, here, and say how many we removed.
        deduped = kept.dropDuplicates(["TRIP_ID"])
        deduped.cache()
        n_kept = deduped.count()
        counts["duplicate_trip_id"] = valid - n_kept
        counts["written"] = n_kept

        log.info("=" * 56)
        log.info("  total trips       : %s", f"{total:,}")
        log.info("  valid trips       : %s (%.1f%%)", f"{valid:,}", 100 * valid / total)
        log.info("  duplicates removed: %s", f"{counts['duplicate_trip_id']:,}")
        log.info("  written           : %s", f"{n_kept:,}")
        log.info("=" * 56)

        deduped.write.mode("overwrite").parquet(paths["clean"])
        log.info("wrote clean parquet -> %s", paths["clean"])

        st.update(rows_in=total, rows_out=n_kept, **{
            k: counts[k] for k in
            ("missing_data_flag", "too_few_points", "too_many_points",
             "endpoints_off_map", "duplicate_trip_id")})

        _write_report(scale, counts)

    spark.stop()


def _write_report(scale: str, c: dict) -> None:
    total = c["total"]
    rules = [
        ("MISSING_DATA flagged by vendor", "missing_data_flag"),
        (f"fewer than {config.MIN_POINTS} GPS points", "too_few_points"),
        (f"more than {config.MAX_POINTS} GPS points", "too_many_points"),
        ("start or end outside Porto metro box", "endpoints_off_map"),
        ("duplicate TRIP_ID", "duplicate_trip_id"),
    ]
    lines = [
        f"# Phase 1 Data Quality ({scale})",
        f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
        "",
        f"rows read: {total:,} | rows written: {c['written']:,} "
        f"({100 * c['written'] / total:.1f}%)",
        "",
        "## Rejection reasons (a trip can fail several rules)",
        "| rule | trips | share |", "|---|---|---|",
    ]
    lines += [f"| {label} | {c[key]:,} | {100 * c[key] / total:.2f}% |"
              for label, key in rules]
    lines += [
        "",
        "Trajectory-level corruption (mid-route GPS teleports, impossible speeds,",
        "parked vehicles) is not detectable from endpoints alone; it is flagged in",
        "Phase 2 and excluded before spatial encoding. See the Phase 2 report.",
    ]
    p = storage.write_lines(
        storage.out_path("statistics", f"phase1_quality_{scale}.md"), lines)
    log.info("wrote quality report -> %s", p)


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
