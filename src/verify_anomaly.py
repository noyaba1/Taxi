"""
verify_anomaly.py  --  M11 verification
======================================
Checks the anomaly detectors are internally consistent and semantically correct.

  1. anomaly_score == sum of the 5 detector booleans (all rows)
  2. a_speed  => (is_teleport OR is_too_fast)
  3. a_drift  => the trip bbox really escapes the metro bbox (+ margin)
  4. a_distance rows really exceed the p99 distance fence
  5. the exported top-anomaly CSV rows are self-consistent (score == sum flags)

Run:
    python -m src.verify_anomaly --sample
"""

from pyspark.sql import functions as F

from src import cli, config, storage
from src.spark_session import get_spark
from src.anomaly_analysis import add_anomaly_flags, DETECTORS


def main(scale: str) -> None:
    spark = get_spark("verify-anomaly")
    feat_path = config.dataset_paths(scale)["features"]

    df, dist_hi, _sinu = add_anomaly_flags(spark.read.parquet(feat_path))
    df.cache()
    n = df.count()
    print(f"Spark {spark.version} | trips={n:,} | p99 dist fence={dist_hi:.2f} km")
    ok = True

    # 1. score == sum(detectors)
    score_expr = sum(F.col(c).cast("int") for c in DETECTORS)
    c1 = df.filter(F.col("anomaly_score") != score_expr).count()
    ok &= (c1 == 0)
    print(f"[{'OK' if c1 == 0 else 'FAIL'}] anomaly_score == sum(detectors): {c1} mismatches")

    # 2. a_speed => is_teleport|is_too_fast
    c2 = df.filter(F.col("a_speed") & ~(F.col("is_teleport") | F.col("is_too_fast"))).count()
    ok &= (c2 == 0)
    print(f"[{'OK' if c2 == 0 else 'FAIL'}] a_speed implies teleport/too_fast: {c2} bad")

    # 3. a_drift => bbox escapes metro+margin
    m = config.ANOMALY_METRO_MARGIN
    lon_lo, lon_hi = config.PORTO_LON_RANGE[0] - m, config.PORTO_LON_RANGE[1] + m
    lat_lo, lat_hi = config.PORTO_LAT_RANGE[0] - m, config.PORTO_LAT_RANGE[1] + m
    outside = ((F.col("min_lon") < lon_lo) | (F.col("max_lon") > lon_hi)
               | (F.col("min_lat") < lat_lo) | (F.col("max_lat") > lat_hi))
    c3 = df.filter(F.col("a_drift") & ~outside).count()
    ok &= (c3 == 0)
    print(f"[{'OK' if c3 == 0 else 'FAIL'}] a_drift implies bbox outside metro: {c3} bad")

    # 4. a_distance => distance > p99 fence
    c4 = df.filter(F.col("a_distance") & (F.col("total_distance_km") <= dist_hi)).count()
    ok &= (c4 == 0)
    print(f"[{'OK' if c4 == 0 else 'FAIL'}] a_distance implies > p99 fence: {c4} bad")

    # 5. exported CSV self-consistency
    apath = storage.out_path("routes", f"anomalies_top50_{scale}.csv")
    rows = storage.read_csv_rows(apath)
    bad_csv = sum(1 for r in rows
                  if int(r["anomaly_score"]) != sum(1 for c in DETECTORS if str(r[c]).lower() == "true")
                  or int(r["anomaly_score"]) < 1)
    ok &= (bad_csv == 0)
    print(f"[{'OK' if bad_csv == 0 else 'FAIL'}] exported top-{len(rows)} rows consistent: {bad_csv} bad")

    print("\n" + ("ANOMALY VERIFICATION PASSED." if ok else "ANOMALY VERIFICATION FAILED."))
    spark.stop()


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
