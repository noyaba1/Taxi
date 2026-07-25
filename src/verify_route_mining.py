"""
verify_route_mining.py  --  M5 verification
===========================================
Independently checks the exact mining output. The mining computed support by
emitting windows + groupBy; here we RE-COUNT support for a few top sub-routes by
a completely different method (substring containment scan over the trips), so a
match is strong evidence of correctness.

Checks:
  1. every reported route satisfies length_km >= its min_len threshold
  2. support in [1, n_trips]
  3. for sampled top routes, brute-force trip-containment support == reported
     support (delimiter-wrapped so cell boundaries can't partial-match)

Run:
    python -m src.verify_route_mining --sample
"""

from pyspark.sql import functions as F

from src import cli, config, storage
from src.spark_session import get_spark

DELIM = ">"





def main(scale: str) -> None:
    spark = get_spark("verify-route-mining")
    res = config.H3_RESOLUTION

    enc_path = config.dataset_paths(scale)["encoded"]
    csv_path = storage.out_path("routes", f"exact_top100_{scale}.csv")

    enc = spark.read.parquet(enc_path).select("h3_seq_compact")
    # Wrap each trip's cell string with delimiters so '>A>B>' can't match inside a cell.
    wrapped = enc.withColumn(
        "trip_str",
        F.concat(F.lit(DELIM), F.concat_ws(DELIM, "h3_seq_compact"), F.lit(DELIM)),
    ).select("trip_str")
    wrapped.cache()
    n_trips = wrapped.count()
    print(f"Spark {spark.version} | trips={n_trips:,} | verifying {csv_path}")

    rows = storage.read_csv_rows(csv_path)
    if not rows:
        print("no exact-mining output for scale={scale} -- stage not run at this "
              "scale; skipping.".format(scale=scale))
        spark.stop()
        return

    ok = True

    # --- 1 & 2: cheap structural checks over ALL reported routes ---
    bad_len = [r for r in rows if float(r["length_km"]) < float(r["min_len_km"])]
    bad_sup = [r for r in rows if not (1 <= int(r["support"]) <= n_trips)]
    c1 = len(bad_len) == 0
    c2 = len(bad_sup) == 0
    ok &= c1 and c2
    print(f"[{'OK' if c1 else 'FAIL'}] all routes length_km >= min_len : {len(bad_len)} violations")
    print(f"[{'OK' if c2 else 'FAIL'}] all support in [1, n_trips]     : {len(bad_sup)} violations")

    # --- 3: independent brute-force support for a spread of top routes ---
    # Pick top-1 and a middle route for the small thresholds that have real support.
    sample_targets = []
    for L in ("1", "3", "5"):
        grp = [r for r in rows if r["min_len_km"] == L]
        if grp:
            sample_targets.append(grp[0])                 # rank 1
            sample_targets.append(grp[len(grp) // 2])     # middle rank
    print("\n=== independent containment re-count ===")
    for r in sample_targets:
        needle = DELIM + r["subroute"] + DELIM
        brute = wrapped.filter(F.instr("trip_str", needle) > 0).count()
        match = (brute == int(r["support"]))
        ok &= match
        print(f"[{'OK' if match else 'FAIL'}] L>={r['min_len_km']}km rank{r['rank']}: "
              f"reported={r['support']} brute={brute} (len={r['length_km']}km, "
              f"{r['n_cells']} cells)")

    # --- 4: distinct-TAXI support, recounted independently ---------------
    # Method D reports support_taxis from inside its suffix-array buckets. Here
    # we recount by containment against the trip table and compare the TAXI sets.
    # This is the check that "popular means many drivers, not many trips by one
    # driver" is actually implemented and not merely claimed.
    sa_rows = storage.read_csv_rows(
        storage.out_path("routes", f"suffix_array_top100_{scale}.csv"))
    if sa_rows:
        print("\n=== distinct-taxi support re-count (brute force) ===")
        enc_taxi = (spark.read.parquet(config.dataset_paths(scale)["encoded"])
                    .select("TAXI_ID", "h3_seq_compact")
                    .withColumn("trip_str",
                                F.concat(F.lit(DELIM),
                                         F.concat_ws(DELIM, "h3_seq_compact"),
                                         F.lit(DELIM)))
                    .select("TAXI_ID", "trip_str")).cache()
        seen, targets = set(), []
        for r in sa_rows:
            if r["subroute"] not in seen:
                seen.add(r["subroute"])
                targets.append(r)
            if len(targets) >= 4:
                break
        for r in targets:
            needle = DELIM + r["subroute"] + DELIM
            brute = (enc_taxi.filter(F.instr("trip_str", needle) > 0)
                     .select("TAXI_ID").distinct().count())
            reported = int(r["support_taxis"])
            match = brute == reported
            ok &= match
            print(f"[{'OK' if match else 'FAIL'}] L>={r['min_len_km']}km: "
                  f"taxis reported={reported} brute={brute} "
                  f"(trips={r['support']}, {float(r['length_km']):.2f} km)")

    print("\n" + ("ROUTE-MINING VERIFICATION PASSED." if ok else "ROUTE-MINING VERIFICATION FAILED."))
    spark.stop()
    # A verifier that cannot fail is not a verifier: exit non-zero so
    # run_pipeline --verify actually gates on the result.
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
