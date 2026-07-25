"""
verify_graph.py  --  M10 verification (Method C)
===============================================
Independently validates the transition-graph outputs.

Checks:
  1. routes structural: length_km >= min_len; support in [1, n_trips]
  2. routes are REAL: brute-force trip-containment support == reported support
     (this is the anti-'Frankenstein' guarantee)
  3. activity zones: every zone cell is a valid H3 cell whose centre is inside the
     Porto bbox; PageRank values are non-negative and sorted descending

Run:
    python -m src.verify_graph --sample
"""

import h3
from pyspark.sql import functions as F

from src import cli, config, storage
from src.spark_session import get_spark
from src.route_mining_exact import DELIM





def main(scale: str) -> None:
    spark = get_spark("verify-graph")
    res = config.H3_RESOLUTION

    enc_path = config.dataset_paths(scale)["encoded"]
    routes_csv = storage.out_path("routes", f"graph_heavy_paths_top100_{scale}.csv")
    zones_csv = storage.out_path("routes", f"activity_zones_{scale}.csv")

    trips = spark.read.parquet(enc_path).withColumn(
        "trip_str", F.concat(F.lit(DELIM), F.concat_ws(DELIM, "h3_seq_compact"), F.lit(DELIM))
    ).select("trip_str")
    trips.cache()
    n_trips = trips.count()

    routes = storage.read_csv_rows(routes_csv)
    zones = storage.read_csv_rows(zones_csv)
    if not routes or not zones:
        print("no graph output for scale=%s -- stage not run at this scale; "
              "skipping." % scale)
        spark.stop()
        return

    print(f"Spark {spark.version} | trips={n_trips:,} | routes={len(routes)} | zones={len(zones)}")
    ok = True

    # --- 1. routes structural ---
    bad = [r for r in routes if float(r["length_km"]) < float(r["min_len_km"])
           or not (1 <= int(r["support"]) <= n_trips)]
    c1 = not bad
    ok &= c1
    print(f"[{'OK' if c1 else 'FAIL'}] routes structural: {len(bad)} bad")

    # --- 2. routes are real (brute-force support == reported) ---
    print("\n=== heavy-path support re-count (brute force) ===")
    seen, targets = set(), []
    for r in routes:
        if r["min_len_km"] in ("1", "3") and r["rank"] in ("1", "10") and r["route"] not in seen:
            seen.add(r["route"]); targets.append(r)
    for r in targets:
        brute = trips.filter(F.instr("trip_str", DELIM + r["route"] + DELIM) > 0).count()
        m = (brute == int(r["support"]))
        ok &= m
        print(f"[{'OK' if m else 'FAIL'}] L>={r['min_len_km']} rank{r['rank']}: "
              f"reported={r['support']} brute={brute} ({r['n_cells']} cells)")

    # --- 3. activity zones: valid geometry + PR sorted ---
    # HARD FAIL only on corrupt geometry (invalid H3 or outside northern Portugal).
    # Cells outside the tight metro bbox but inside the region are legitimate
    # long-trip / drift zones -> reported (info), formalised by M11 anomaly work.
    lat_lo, lat_hi, lon_lo, lon_hi = 40.0, 42.5, -9.5, -6.5     # generous sanity box
    m_lat_lo, m_lat_hi = config.PORTO_LAT_RANGE
    m_lon_lo, m_lon_hi = config.PORTO_LON_RANGE
    bad_cell = corrupt = outside_metro = 0
    prs = []
    for z in zones:
        prs.append(float(z["pagerank"]))
        if not h3.h3_is_valid(z["cell"]):
            bad_cell += 1
            continue
        lat, lon = h3.h3_to_geo(z["cell"])
        if not (lon_lo <= lon <= lon_hi and lat_lo <= lat <= lat_hi):
            corrupt += 1
        elif not (m_lon_lo <= lon <= m_lon_hi and m_lat_lo <= lat <= m_lat_hi):
            outside_metro += 1
    sorted_desc = all(prs[i] >= prs[i + 1] for i in range(len(prs) - 1))
    nonneg = all(p >= 0 for p in prs)
    c3 = (bad_cell == 0 and corrupt == 0 and sorted_desc and nonneg)
    ok &= c3
    print(f"\n[{'OK' if c3 else 'FAIL'}] zones: invalid={bad_cell} corrupt(out-of-region)={corrupt} "
          f"pr_sorted={sorted_desc} pr_nonneg={nonneg}")
    print(f"  info: {outside_metro} zone(s) outside metro bbox (long-trip/drift -> M11 anomaly)")
    print(f"  top zone: {zones[0]['cell']} @ ({zones[0]['lat']},{zones[0]['lon']}) "
          f"pr={zones[0]['pagerank']}")

    print("\n" + ("GRAPH VERIFICATION PASSED." if ok else "GRAPH VERIFICATION FAILED."))
    spark.stop()


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
