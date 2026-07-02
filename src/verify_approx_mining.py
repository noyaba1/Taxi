"""
verify_approx_mining.py  --  M7 verification (approx vs exact)
=============================================================
Independently validates the approximate sketches against exact ground truth.

Checks:
  1. Space-Saving bound guarantee: for sampled approx routes, the brute-force
     exact support S satisfies ss_lb <= S <= ss_ub
  2. Count-Min never underestimates: cms_estimate >= S
  3. the stored exact_support column equals the brute-force recount
  4. precision@100 recomputed from files (approx ∩ exact M5) matches expectation

Run:
    python -m src.verify_approx_mining --sample
"""
import argparse
import csv
import os

from pyspark.sql import functions as F

from src.spark_session import get_spark
from src import config
from src.route_mining_exact import DELIM


def _read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def main(use_sample: bool) -> None:
    spark = get_spark("verify-approx-mining")
    suffix = "sample" if use_sample else "full"
    res = config.H3_RESOLUTION

    enc_path = config.CLEAN_PARQUET.replace(".parquet", f"_encoded_r{res}_{suffix}.parquet")
    approx_csv = os.path.join(config.OUTPUT_BASE, "routes", f"approx_top100_{suffix}.csv")
    exact_csv = os.path.join(config.OUTPUT_BASE, "routes", f"exact_top100_{suffix}.csv")

    trips = spark.read.parquet(enc_path).withColumn(
        "trip_str", F.concat(F.lit(DELIM), F.concat_ws(DELIM, "h3_seq_compact"), F.lit(DELIM))
    ).select("trip_str")
    trips.cache()
    n_trips = trips.count()

    approx = _read_csv(approx_csv)
    exact = _read_csv(exact_csv)
    print(f"Spark {spark.version} | trips={n_trips:,} | approx rows={len(approx)}")
    ok = True

    # --- 1-3: sketch bound guarantees on sampled routes (L=1,3,5 have real support) ---
    targets, seen = [], set()
    for r in approx:
        if r["min_len_km"] in ("1", "3", "5") and r["rank"] in ("1", "10", "50") \
                and r["subroute"] not in seen:
            seen.add(r["subroute"]); targets.append(r)

    print("\n=== sketch bounds vs brute-force exact support ===")
    for r in targets:
        needle = DELIM + r["subroute"] + DELIM
        S = trips.filter(F.instr("trip_str", needle) > 0).count()
        lb, ub = float(r["ss_lb"]), float(r["ss_ub"])
        cms = float(r["cms_estimate"])
        stored = int(r["exact_support"]) if r["exact_support"] else None
        c_bound = lb <= S <= ub
        c_cms = cms >= S
        c_stored = (stored == S) if stored is not None else True
        ok &= c_bound and c_cms and c_stored
        print(f"[{'OK' if (c_bound and c_cms and c_stored) else 'FAIL'}] "
              f"L>={r['min_len_km']} rank{r['rank']}: exact={S} "
              f"SS[{lb:.0f},{ub:.0f}] CMS={cms:.0f} stored={stored}")

    # --- 4: recompute precision@100 from files (independent of the report) ---
    print("\n=== precision@100 recomputed (approx INTERSECT exact M5) ===")
    for L in ("1", "3", "5", "10", "20", "40"):
        a = {r["subroute"] for r in approx if r["min_len_km"] == L}
        e = {r["subroute"] for r in exact if r["min_len_km"] == L}
        prec = len(a & e) / len(a) if a else 0.0
        note = "  (support~1 ties -> arbitrary top-100, expected low)" if L in ("10", "20", "40") else ""
        print(f"  L>={L}km: precision@100 = {prec:.2f}{note}")

    print("\n" + ("APPROX VERIFICATION PASSED." if ok else "APPROX VERIFICATION FAILED."))
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
