"""
verify_maximal.py  --  M8 verification
======================================
Independently validates the min-support X% + maximal output.

Checks (independent of the mining's own aggregate):
  1. structural: length_km >= min_len; support in [1, n_trips]
  2. support correctness: brute-force trip-containment == reported support
  3. FREQUENT: reported support >= min_sup (= ceil(X% * n_trips))
  4. MAXIMAL: best single-cell right/left extension support < min_sup, computed by
     regexp-extracting the cell after/before the route across trips (independent
     of the aggregate). Together with (3) this proves "maximal among frequent".
  5. holes: the top route's continuations (forks) are each < min_sup (a hole).

Run:
    python -m src.verify_maximal --sample
"""
import math

from pyspark.sql import functions as F

from src import cli, config, storage
from src.spark_session import get_spark
from src.route_mining_exact import DELIM





def _contain_support(trips, route):
    return trips.filter(F.instr("trip_str", DELIM + route + DELIM) > 0).count()


def _best_right_ext(trips, route):
    """Max distinct-trip count of any single cell following `route` (1st match)."""
    pat = DELIM + route + DELIM + "([^>]+)" + DELIM
    nxt = trips.withColumn("c", F.regexp_extract("trip_str", pat, 1)).filter(F.col("c") != "")
    row = nxt.groupBy("c").count().agg(F.max("count").alias("m")).collect()[0]
    return row["m"] or 0


def _best_left_ext(trips, route):
    pat = DELIM + "([^>]+)" + DELIM + route + DELIM
    prv = trips.withColumn("c", F.regexp_extract("trip_str", pat, 1)).filter(F.col("c") != "")
    row = prv.groupBy("c").count().agg(F.max("count").alias("m")).collect()[0]
    return row["m"] or 0


def main(scale: str) -> None:
    spark = get_spark("verify-maximal")
    res = config.H3_RESOLUTION

    enc_path = config.dataset_paths(scale)["encoded"]
    csv_path = storage.out_path("routes", f"maximal_frequent_top100_{scale}.csv")

    trips = spark.read.parquet(enc_path).withColumn(
        "trip_str", F.concat(F.lit(DELIM), F.concat_ws(DELIM, "h3_seq_compact"), F.lit(DELIM))
    ).select("trip_str")
    trips.cache()
    n_trips = trips.count()
    # The floor is calibrated PER LENGTH BAND now, so a single global X% is the
    # wrong yardstick -- checking a >=5 km route against the >=1 km floor made
    # this verifier report failures for routes that were perfectly correct.
    # Each row carries the floor it was mined at; verify against that.

    rows = storage.read_csv_rows(csv_path)
    if not rows:
        print("no maximal-frequent output for scale={scale} -- stage not run at this "
              "scale; skipping.".format(scale=scale))
        spark.stop()
        return

    print(f"Spark {spark.version} | trips={n_trips:,} | rows={len(rows)} "
          f"| floors used: {sorted({int(r['min_sup']) for r in rows})}")
    ok = True

    # --- 1. structural ---
    bad = [r for r in rows if float(r["length_km"]) < float(r["min_len_km"])
           or not (1 <= int(r["support"]) <= n_trips)]
    c1 = not bad
    ok &= c1
    print(f"[{'OK' if c1 else 'FAIL'}] structural (length>=min_len, support in range): {len(bad)} bad")

    # --- 2-4. per-route: support correctness + frequent + maximal (independent) ---
    targets, seen = [], set()
    for r in rows:
        if r["min_len_km"] in ("1", "3", "5") and r["rank"] in ("1", "20") \
                and r["subroute"] not in seen:
            seen.add(r["subroute"]); targets.append(r)

    print("\n=== independent frequent + maximal checks ===")
    for r in targets:
        s = r["subroute"]
        min_sup = int(r["min_sup"])            # the floor THIS row was mined at
        sup = _contain_support(trips, s)
        rext = _best_right_ext(trips, s)
        lext = _best_left_ext(trips, s)
        c_sup = (sup == int(r["support"]))
        c_freq = (sup >= min_sup)
        c_max = (rext < min_sup and lext < min_sup)
        ok &= c_sup and c_freq and c_max
        print(f"[{'OK' if (c_sup and c_freq and c_max) else 'FAIL'}] "
              f"L>={r['min_len_km']} rank{r['rank']} (floor={min_sup}): "
              f"support={sup} (reported {r['support']}) >=floor? {c_freq} "
              f"| best_ext L={lext} R={rext} <floor? {c_max}")

    # --- 5. holes: top route's forks each below min_sup ---
    print("\n=== holes: top route continuations (each must be < min_sup) ===")
    top = min(rows, key=lambda r: (int(r["min_len_km"]) != 1, int(r["rank"])))  # L>=1 rank1
    min_sup = int(top["min_sup"])
    s = top["subroute"]
    pat = DELIM + s + DELIM + "([^>]+)" + DELIM
    forks = (trips.withColumn("c", F.regexp_extract("trip_str", pat, 1))
             .filter(F.col("c") != "").groupBy("c").count()
             .orderBy(F.col("count").desc()).limit(3).collect())
    c5 = all(f["count"] < min_sup for f in forks) if forks else True
    ok &= c5
    print(f"  route support={top['support']} forks into: "
          + " ; ".join(f"{f['count']}" for f in forks))
    print(f"[{'OK' if c5 else 'FAIL'}] each continuation < min_sup ({min_sup}) => genuine hole")

    print("\n" + ("MAXIMAL VERIFICATION PASSED." if ok else "MAXIMAL VERIFICATION FAILED."))
    spark.stop()
    # A verifier that cannot fail is not a verifier: exit non-zero so
    # run_pipeline --verify actually gates on the result.
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
