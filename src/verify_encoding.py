"""
verify_encoding.py  --  M3 verification
=======================================
Reads the encoded Parquet BACK and independently checks the encoding invariants.
We never trust a silent write.

Checks:
  1. row count preserved vs the feature table
  2. no empty compact sequences for valid trips
  3. every H3 cell is valid (h3.h3_is_valid)
  4. compact length <= raw length for every row
  5. sample coordinates map to plausible Porto H3 cells

Run:
    python -m src.verify_encoding --sample
"""

import h3
import pandas as pd
from pyspark.sql import functions as F, types as T
from pyspark.sql.pandas.functions import pandas_udf

from src import cli, config, storage
from src.spark_session import get_spark


@pandas_udf(T.IntegerType())
def count_invalid_cells(seq_series: pd.Series) -> pd.Series:
    """Per row: how many cells in the compact sequence are NOT valid H3 cells."""
    out = []
    for seq in seq_series:
        if seq is None:
            out.append(0)
        else:
            out.append(sum(0 if h3.h3_is_valid(c) else 1 for c in seq))
    return pd.Series(out)


def main(scale: str) -> None:
    spark = get_spark("verify-encoding")
    res = config.H3_RESOLUTION
    print(f"Spark version : {spark.version}  (H3 res {res})")

    paths = config.dataset_paths(scale)
    enc_path, feat_path = paths["encoded"], paths["features"]

    enc = spark.read.parquet(enc_path)
    enc.cache()
    n_enc = enc.count()
    n_feat = spark.read.parquet(feat_path).count()

    ok = True

    # 1. row count preserved
    c1 = (n_enc == n_feat)
    ok &= c1
    print(f"[{'OK' if c1 else 'FAIL'}] row count preserved: encoded={n_enc:,} features={n_feat:,}")

    # 2. no empty compact sequences (every valid trip must yield >=1 cell)
    empty = enc.filter((F.col("n_cells_compact") < 1) | F.col("h3_seq_compact").isNull()).count()
    c2 = (empty == 0)
    ok &= c2
    print(f"[{'OK' if c2 else 'FAIL'}] empty compact sequences: {empty} (must be 0)")

    # 3. all H3 cells valid
    invalid = enc.select(F.sum(count_invalid_cells(F.col("h3_seq_compact"))).alias("x")).collect()[0]["x"]
    c3 = (invalid == 0)
    ok &= c3
    print(f"[{'OK' if c3 else 'FAIL'}] invalid H3 cells across all rows: {invalid} (must be 0)")

    # 4. compact length <= raw length
    bad_len = enc.filter(F.col("n_cells_compact") > F.col("n_cells_raw")).count()
    c4 = (bad_len == 0)
    ok &= c4
    print(f"[{'OK' if c4 else 'FAIL'}] rows with compact>raw: {bad_len} (must be 0)")

    # 5. sample cells map to plausible Porto coordinates
    print("\n=== sample: first compact cell -> centre (must be inside Porto bbox) ===")
    lon_lo, lon_hi = config.PORTO_LON_RANGE
    lat_lo, lat_hi = config.PORTO_LAT_RANGE
    c5 = True
    for row in enc.select("TRIP_ID", "h3_seq_compact").limit(5).collect():
        cell = row["h3_seq_compact"][0]
        lat, lon = h3.h3_to_geo(cell)
        inside = (lon_lo <= lon <= lon_hi) and (lat_lo <= lat <= lat_hi)
        c5 &= inside
        print(f"  {row['TRIP_ID']}: {cell} -> ({lat:.5f},{lon:.5f}) inside={inside}")
    ok &= c5
    print(f"[{'OK' if c5 else 'FAIL'}] sampled cells inside Porto bbox")

    print("\n" + ("ENCODING VERIFICATION PASSED." if ok else "ENCODING VERIFICATION FAILED."))
    spark.stop()


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
