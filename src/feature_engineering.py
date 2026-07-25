"""
feature_engineering.py  --  PHASE 2
===================================
Reads the clean Parquet from Phase 1 and derives per-trip features:

    total_distance_km   total path length (sum of Haversine over consecutive pts)
    straight_line_km    Haversine from start to end
    sinuosity           total / straight_line  (1.0 = perfectly straight)
    avg_speed_kmh       total_distance / duration
    max_seg_speed_kmh   fastest single 15s segment (used for GPS-jump detection)
    min/max lon/lat     trajectory bounding box
    anomaly flags       is_teleport, is_idle, is_too_fast, is_too_short,
                        is_offgrid  ->  is_anomalous

`is_anomalous` is not decoration: spatial_encoding EXCLUDES those trips before
building the cell sequences, because a trajectory with a GPS teleport in it
produces sub-routes no taxi ever drove.

SCALABILITY / DataProc-readiness
--------------------------------
We use a single ARROW-VECTORIZED pandas_udf that consumes the whole `points`
array per trip and returns ALL metrics in one pass. This:
  * runs distributed on every executor (NOT on the driver) via Apache Arrow,
  * keeps ONE row per trip (no `explode` -> no 85M-row shuffle),
  * is embarrassingly parallel -> linear speedup when DataProc adds machines.

Run:
    python -m src.feature_engineering --sample | --mid | --full
"""
import numpy as np
import pandas as pd
from pyspark.sql import functions as F, types as T
from pyspark.sql.pandas.functions import pandas_udf

from src.spark_session import get_spark
from src import cli, config

EARTH_RADIUS_KM = 6371.0088
log = cli.setup_logging("features")

# Struct returned by the per-trip UDF. Declaring it explicitly lets Spark
# build the output schema without inference.
_METRICS_SCHEMA = T.StructType([
    T.StructField("total_distance_km", T.DoubleType()),
    T.StructField("straight_line_km", T.DoubleType()),
    T.StructField("max_seg_speed_kmh", T.DoubleType()),
    T.StructField("min_lon", T.DoubleType()),
    T.StructField("max_lon", T.DoubleType()),
    T.StructField("min_lat", T.DoubleType()),
    T.StructField("max_lat", T.DoubleType()),
])


def _haversine_km(lon1, lat1, lon2, lat2):
    """Vectorized Haversine on numpy arrays. Inputs in degrees, output in km."""
    lon1, lat1, lon2, lat2 = map(np.radians, (lon1, lat1, lon2, lat2))
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 2.0 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))


@pandas_udf(_METRICS_SCHEMA)
def trip_metrics_udf(points_series: pd.Series) -> pd.DataFrame:
    """
    points_series: a batch of trajectories, each a list of [lon, lat] pairs.
    Returns one row of metrics per trajectory. Runs per-executor via Arrow.
    """
    out = {k: [] for k in _METRICS_SCHEMA.names}

    for pts in points_series:
        # pts is a numpy object array / list of [lon, lat]; guard empties.
        if pts is None or len(pts) < 2:
            for k in out:
                out[k].append(None)
            continue

        # Spark hands `array<array<double>>` to pandas as an object array of
        # per-point arrays; build a clean (n, 2) float matrix explicitly.
        arr = np.array([(p[0], p[1]) for p in pts], dtype="float64")  # col0=lon col1=lat
        lon, lat = arr[:, 0], arr[:, 1]

        # Consecutive-segment distances (length n-1).
        seg = _haversine_km(lon[:-1], lat[:-1], lon[1:], lat[1:])
        total = float(seg.sum())

        # Each segment spans GPS_INTERVAL_SEC seconds -> instantaneous speed.
        seg_speed = seg / (config.GPS_INTERVAL_SEC / 3600.0)   # km / h
        max_seg = float(seg_speed.max()) if seg_speed.size else 0.0

        straight = float(_haversine_km(lon[0], lat[0], lon[-1], lat[-1]))

        out["total_distance_km"].append(total)
        out["straight_line_km"].append(straight)
        out["max_seg_speed_kmh"].append(max_seg)
        out["min_lon"].append(float(lon.min()))
        out["max_lon"].append(float(lon.max()))
        out["min_lat"].append(float(lat.min()))
        out["max_lat"].append(float(lat.max()))

    return pd.DataFrame(out)


def add_features(df):
    """Attach the UDF metrics, then derive ratios and anomaly flags in SQL."""
    # Call the UDF once into a struct column, then flatten it to top-level cols.
    df = df.withColumn("m", trip_metrics_udf(F.col("points"))).select("*", "m.*").drop("m")

    # Derived ratios (cheap, do them in native SQL not in the UDF).
    df = df.withColumn(
        "avg_speed_kmh",
        F.when(F.col("duration_sec") > 0,
               F.col("total_distance_km") / (F.col("duration_sec") / 3600.0))
        .otherwise(F.lit(None)),
    )
    df = df.withColumn(
        "sinuosity",
        F.when(F.col("straight_line_km") > 0.05,   # ignore <50m to avoid blow-up
               F.col("total_distance_km") / F.col("straight_line_km"))
        .otherwise(F.lit(None)),
    )

    # --- Anomaly flags (each defensible & individually inspectable) ---
    # Phase 1 could only bbox-check the FIRST and LAST GPS point, because the
    # trajectory bounding box is computed here. A trip that starts and ends in
    # Porto but spikes to Null Island mid-route therefore passes cleaning; this
    # is the flag that catches it.
    margin = config.ANOMALY_METRO_MARGIN
    lon_lo, lon_hi = config.PORTO_LON_RANGE[0] - margin, config.PORTO_LON_RANGE[1] + margin
    lat_lo, lat_hi = config.PORTO_LAT_RANGE[0] - margin, config.PORTO_LAT_RANGE[1] + margin

    df = (
        df
        # GPS teleport: a single 15s segment faster than any real car.
        .withColumn("is_teleport", F.col("max_seg_speed_kmh") > config.MAX_SPEED_KMH)
        # Idle/parked: long in time but barely moved.
        .withColumn("is_idle", (F.col("duration_sec") > 120) & (F.col("total_distance_km") < 0.1))
        # Implausible average speed for a city taxi.
        .withColumn("is_too_fast", F.col("avg_speed_kmh") > 120.0)
        # Degenerate: essentially no movement.
        .withColumn("is_too_short", F.col("total_distance_km") < 0.05)
        # Any point of the trajectory outside the metro box (+ margin).
        .withColumn(
            "is_offgrid",
            (F.col("min_lon") < lon_lo) | (F.col("max_lon") > lon_hi)
            | (F.col("min_lat") < lat_lo) | (F.col("max_lat") > lat_hi),
        )
        .withColumn(
            "is_anomalous",
            F.col("is_teleport") | F.col("is_idle") | F.col("is_too_fast")
            | F.col("is_too_short") | F.col("is_offgrid"),
        )
    )
    return df


FLAG_COLS = ["is_teleport", "is_idle", "is_too_fast", "is_too_short",
             "is_offgrid", "is_anomalous"]


def main(scale: str) -> None:
    spark = get_spark("feature-engineering")
    # Arrow must be on for pandas_udf performance.
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    paths = config.dataset_paths(scale)

    with cli.stage("p2_features", scale, log) as st:
        df = spark.read.parquet(paths["clean"])

        feats = add_features(df)
        feats.cache()  # we read it twice below (report + write)

        # --- Defensible quality report ---
        # One pass computes the row count + every flag count together (no N+1 jobs).
        agg = feats.select(
            F.count(F.lit(1)).alias("n"),
            *[F.sum(F.col(c).cast("int")).alias(c) for c in FLAG_COLS],
        ).collect()[0]
        n = agg["n"]
        log.info("=" * 56)
        log.info("  trips with features : %s", f"{n:,}")
        for c in FLAG_COLS:
            cnt = agg[c] or 0
            log.info("  %-16s : %8s  (%5.2f%%)", c, f"{cnt:,}", 100 * cnt / n)
        log.info("=" * 56)
        feats.select(
            F.round(F.avg("total_distance_km"), 2).alias("avg_dist_km"),
            F.round(F.avg("duration_sec") / 60, 1).alias("avg_dur_min"),
            F.round(F.avg("avg_speed_kmh"), 1).alias("avg_speed_kmh"),
            F.round(F.avg("sinuosity"), 2).alias("avg_sinuosity"),
        ).show()

        feats.write.mode("overwrite").parquet(paths["features"])
        log.info("wrote -> %s", paths["features"])
        st.update(rows_in=n, rows_out=n,
                  **{c: int(agg[c] or 0) for c in FLAG_COLS})

    spark.stop()


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
