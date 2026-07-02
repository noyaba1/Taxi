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
import argparse
import csv
import os

from pyspark.sql import functions as F

from src.spark_session import get_spark
from src import config

DELIM = ">"


def _read_top_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def main(use_sample: bool) -> None:
    spark = get_spark("verify-route-mining")
    res = config.H3_RESOLUTION
    suffix = "sample" if use_sample else "full"

    enc_path = config.CLEAN_PARQUET.replace(
        ".parquet", f"_encoded_r{res}_{suffix}.parquet")
    csv_path = os.path.join(config.OUTPUT_BASE, "routes", f"exact_top100_{suffix}.csv")

    enc = spark.read.parquet(enc_path).select("h3_seq_compact")
    # Wrap each trip's cell string with delimiters so '>A>B>' can't match inside a cell.
    wrapped = enc.withColumn(
        "trip_str",
        F.concat(F.lit(DELIM), F.concat_ws(DELIM, "h3_seq_compact"), F.lit(DELIM)),
    ).select("trip_str")
    wrapped.cache()
    n_trips = wrapped.count()
    print(f"Spark {spark.version} | trips={n_trips:,} | verifying {csv_path}")

    rows = _read_top_csv(csv_path)
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

    print("\n" + ("ROUTE-MINING VERIFICATION PASSED." if ok else "ROUTE-MINING VERIFICATION FAILED."))
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
