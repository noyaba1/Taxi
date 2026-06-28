"""
spatial_encoding.py  --  PHASE 4 / Milestone M3
================================================
Convert each trip's GPS trajectory into an ordered sequence of H3 cells.

WHY H3 (vs Geohash/S2): hexagons have uniform neighbour distance, so a route is
a clean walk on the grid with no diagonal/orthogonal distortion. See
docs/DESIGN_REVIEW.md section 1-2. Resolution 9 (~174 m edge) is justified by the
sampling geometry: a taxi at ~50 km/h moves ~210 m per 15 s ~= one res-9 cell.

OUTPUT per trip:
    h3_seq_raw        one cell per GPS point (order preserved)
    h3_seq_compact    consecutive duplicates removed (staying in a cell != route)
    n_cells_raw       len(raw)
    n_cells_compact   len(compact)
    compression_ratio raw / compact (how much idling/dwelling we collapsed)
    encoded_len_km    sum of Haversine between consecutive COMPACT cell centres

NOTE: POLYLINE points are [lon, lat]; H3 wants (lat, lon) -> we pass p[1], p[0].

Run:
    python -m src.spatial_encoding --sample          # encode at res 9 + sweep
"""
import argparse
import os
from datetime import datetime, timezone

import h3
import pandas as pd
from pyspark.sql import functions as F, types as T
from pyspark.sql.pandas.functions import pandas_udf

from src.spark_session import get_spark
from src import config

SWEEP_RESOLUTIONS = [8, 9, 10]

# Struct returned by the per-trip encoder. Built per-resolution by the factory.
_ENC_SCHEMA = T.StructType([
    T.StructField("h3_seq_raw", T.ArrayType(T.StringType())),
    T.StructField("h3_seq_compact", T.ArrayType(T.StringType())),
    T.StructField("n_cells_raw", T.IntegerType()),
    T.StructField("n_cells_compact", T.IntegerType()),
    T.StructField("compression_ratio", T.DoubleType()),
    T.StructField("encoded_len_km", T.DoubleType()),
])


def _compact(seq):
    """Remove consecutive duplicate cells (run-length collapse)."""
    out = []
    prev = None
    for c in seq:
        if c != prev:
            out.append(c)
            prev = c
    return out


def make_encoder_udf(resolution: int):
    """Return a pandas_udf bound to a specific H3 resolution (for the sweep)."""

    @pandas_udf(_ENC_SCHEMA)
    def _udf(points_series: pd.Series) -> pd.DataFrame:
        cols = {k: [] for k in _ENC_SCHEMA.names}
        for pts in points_series:
            if pts is None or len(pts) == 0:
                cols["h3_seq_raw"].append(None)
                cols["h3_seq_compact"].append(None)
                cols["n_cells_raw"].append(0)
                cols["n_cells_compact"].append(0)
                cols["compression_ratio"].append(None)
                cols["encoded_len_km"].append(None)
                continue

            # p = [lon, lat] -> geo_to_h3(lat, lon, res)
            raw = [h3.geo_to_h3(p[1], p[0], resolution) for p in pts]
            comp = _compact(raw)

            # Encoded length = Haversine between consecutive compact cell centres.
            length = 0.0
            for a, b in zip(comp[:-1], comp[1:]):
                length += h3.point_dist(h3.h3_to_geo(a), h3.h3_to_geo(b), unit="km")

            cols["h3_seq_raw"].append(raw)
            cols["h3_seq_compact"].append(comp)
            cols["n_cells_raw"].append(len(raw))
            cols["n_cells_compact"].append(len(comp))
            cols["compression_ratio"].append(len(raw) / len(comp) if comp else None)
            cols["encoded_len_km"].append(length)
        return pd.DataFrame(cols)

    return _udf


def encode(df, resolution: int):
    """Attach the encoder struct and flatten it to top-level columns."""
    enc = make_encoder_udf(resolution)
    return df.withColumn("e", enc(F.col("points"))).select("*", "e.*").drop("e")


def _features_path(use_sample: bool) -> str:
    base = config.CLEAN_PARQUET
    return base.replace(".parquet", "_features_sample.parquet") if use_sample \
        else base.replace(".parquet", "_features.parquet")


def _write_report(name: str, lines) -> str:
    out_dir = os.path.join(config.OUTPUT_BASE, "statistics")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, name)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return path


def main(use_sample: bool) -> None:
    spark = get_spark("spatial-encoding")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    src = _features_path(use_sample)
    feats = spark.read.parquet(src).select(
        "TRIP_ID", "TAXI_ID", "n_points", "duration_sec", "total_distance_km", "points"
    )
    feats.cache()
    n = feats.count()
    print(f"input trips: {n:,} from {os.path.basename(src)}")

    # ---- 1. Encode at the chosen resolution (9) and persist ----
    res = config.H3_RESOLUTION
    enc = encode(feats, res).drop("points")  # drop heavy raw points; keep cells
    enc.cache()

    summary = enc.select(
        F.round(F.avg("n_cells_raw"), 1).alias("avg_raw"),
        F.round(F.avg("n_cells_compact"), 1).alias("avg_compact"),
        F.round(F.avg("compression_ratio"), 2).alias("avg_compression"),
        F.round(F.avg("encoded_len_km"), 2).alias("avg_encoded_km"),
        F.round(F.avg("total_distance_km"), 2).alias("avg_gps_km"),
    ).collect()[0].asDict()
    print(f"\n=== res {res} encoding summary ===")
    for k, v in summary.items():
        print(f"  {k:<16}: {v}")

    out = config.CLEAN_PARQUET.replace(
        ".parquet", f"_encoded_r{res}_sample.parquet" if use_sample
        else f"_encoded_r{res}.parquet")
    enc.write.mode("overwrite").parquet(out)
    print(f"[encode] wrote -> {out}")

    rep = [f"# Phase 4 H3 Encoding Summary (res {res}, {'sample' if use_sample else 'full'})",
           f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
           f"\nrows: {n:,}\n", "| metric | value |", "|---|---|"]
    rep += [f"| {k} | {v} |" for k, v in summary.items()]
    p = _write_report(f"phase4_encoding_summary_{'sample' if use_sample else 'full'}.md", rep)
    print(f"[encode] wrote report -> {p}")

    # ---- 2. Resolution sweep 8/9/10 (aggregates only, no persist) ----
    print("\n=== resolution sweep 8/9/10 ===")
    sweep_rows = []
    for r in SWEEP_RESOLUTIONS:
        s = encode(feats, r).select(
            F.round(F.avg("n_cells_raw"), 1).alias("avg_raw"),
            F.round(F.avg("n_cells_compact"), 1).alias("avg_compact"),
            F.round(F.avg("compression_ratio"), 2).alias("avg_compression"),
            F.round(F.avg("encoded_len_km"), 2).alias("avg_encoded_km"),
            # stability: encoded length / GPS path length (closer to 1 = better)
            F.round(F.avg(F.col("encoded_len_km") / F.col("total_distance_km")), 3).alias("len_ratio"),
        ).collect()[0].asDict()
        s["res"] = r
        s["edge_m"] = round(h3.edge_length(r, unit="m"), 1)
        sweep_rows.append(s)
        print(f"  res {r}: {s}")

    sweep = [f"# H3 Resolution Comparison ({'sample' if use_sample else 'full'})",
             f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
             f"\nrows: {n:,}  | len_ratio = encoded_len_km / GPS total_distance_km (1.0 = ideal)\n",
             "| res | edge_m | avg_raw | avg_compact | avg_compression | avg_encoded_km | len_ratio |",
             "|---|---|---|---|---|---|---|"]
    for s in sweep_rows:
        sweep.append(f"| {s['res']} | {s['edge_m']} | {s['avg_raw']} | {s['avg_compact']} | "
                     f"{s['avg_compression']} | {s['avg_encoded_km']} | {s['len_ratio']} |")
    p2 = _write_report(f"h3_resolution_comparison_{'sample' if use_sample else 'full'}.md", sweep)
    print(f"[sweep] wrote report -> {p2}")

    spark.stop()
    print("\nM3 ENCODING COMPLETE.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
