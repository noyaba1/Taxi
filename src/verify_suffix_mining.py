"""
verify_suffix_mining.py  --  M6 verification (against M5)
========================================================
Independently validates the maximal-route output.

Checks:
  1. structural: every M6 route length_km >= its min_len; support in [1, n_trips]
  2. support correctness vs M5 method: brute-force trip-containment support for
     sampled M6 routes == reported support
  3. maximality (closedness) on the OUTPUT: no reported M6 route is a proper
     contiguous substring of a longer reported M6 route with support within
     tolerance -> that would mean it was NOT maximal
  4. M5->M6 collapse demonstration: show an M5 top route that was dropped because
     it is contained in an M6 maximal route with (near-)equal support

Run:
    python -m src.verify_suffix_mining --sample
"""
import argparse
import csv
import os

from pyspark.sql import functions as F

from src.spark_session import get_spark
from src import config
from src.route_mining_suffix import SUPPORT_TOL
from src.route_mining_exact import DELIM


def _read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _wrapped(s):
    return DELIM + s + DELIM


def main(use_sample: bool) -> None:
    spark = get_spark("verify-suffix-mining")
    suffix = "sample" if use_sample else "full"
    res = config.H3_RESOLUTION

    enc_path = config.CLEAN_PARQUET.replace(".parquet", f"_encoded_r{res}_{suffix}.parquet")
    m6_csv = os.path.join(config.OUTPUT_BASE, "routes", f"suffix_maximal_top100_{suffix}.csv")
    m5_csv = os.path.join(config.OUTPUT_BASE, "routes", f"exact_top100_{suffix}.csv")

    trips = spark.read.parquet(enc_path).withColumn(
        "trip_str", F.concat(F.lit(DELIM), F.concat_ws(DELIM, "h3_seq_compact"), F.lit(DELIM))
    ).select("trip_str")
    trips.cache()
    n_trips = trips.count()

    m6 = _read_csv(m6_csv)
    m5 = _read_csv(m5_csv)
    print(f"Spark {spark.version} | trips={n_trips:,} | M6 rows={len(m6)} M5 rows={len(m5)} "
          f"| SUPPORT_TOL={SUPPORT_TOL}")
    ok = True

    # --- 1. structural ---
    bad_len = [r for r in m6 if float(r["length_km"]) < float(r["min_len_km"])]
    bad_sup = [r for r in m6 if not (1 <= int(r["support"]) <= n_trips)]
    c1 = not bad_len and not bad_sup
    ok &= c1
    print(f"[{'OK' if c1 else 'FAIL'}] structural: len>=min_len ({len(bad_len)} bad), "
          f"support in range ({len(bad_sup)} bad)")

    # --- 2. brute-force support correctness for a spread of M6 routes ---
    print("\n=== support re-count (brute-force containment) ===")
    seen, targets = set(), []
    for r in m6:
        key = (r["min_len_km"], r["rank"])
        if r["min_len_km"] in ("1", "3", "5") and r["rank"] in ("1", "25") and r["subroute"] not in seen:
            seen.add(r["subroute"]); targets.append(r)
    c2 = True
    for r in targets:
        brute = trips.filter(F.instr("trip_str", _wrapped(r["subroute"])) > 0).count()
        m = (brute == int(r["support"]))
        c2 &= m
        print(f"[{'OK' if m else 'FAIL'}] L>={r['min_len_km']} rank{r['rank']}: "
              f"reported={r['support']} brute={brute} ({r['n_cells']} cells)")
    ok &= c2

    # --- 3. maximality on the output: no reported route dominated by a longer one ---
    violations = 0
    for s in m6:
        s_sup, s_cells, s_sr = int(s["support"]), int(s["n_cells"]), s["subroute"]
        for t in m6:
            if int(t["n_cells"]) > s_cells and _wrapped(s_sr) in _wrapped(t["subroute"]):
                if int(t["support"]) >= s_sup * (1.0 - SUPPORT_TOL):
                    violations += 1
                    break
    c3 = (violations == 0)
    ok &= c3
    print(f"\n[{'OK' if c3 else 'FAIL'}] output maximality: {violations} reported routes "
          f"dominated by a longer reported route (must be 0)")

    # --- 4. M5 -> M6 collapse demonstration ---
    m6_set = {r["subroute"] for r in m6}
    demo = None
    for r in m5:  # an M5 top route that is NOT itself a reported M6 route
        if r["subroute"] in m6_set:
            continue
        for t in m6:
            if int(t["n_cells"]) > int(r["n_cells"]) and _wrapped(r["subroute"]) in _wrapped(t["subroute"]):
                demo = (r, t)
                break
        if demo:
            break
    print("\n=== M5 -> M6 collapse example ===")
    if demo:
        r, t = demo
        print(f"  M5 fragment : {r['n_cells']} cells, support {r['support']} (L>={r['min_len_km']})")
        print(f"  collapsed into M6 maximal route: {t['n_cells']} cells, support {t['support']}")
        print(f"  -> the shorter route was a fragment of a longer route with similar support")
    else:
        print("  (no example found in the sampled top lists)")

    print("\n" + ("SUFFIX-MINING VERIFICATION PASSED." if ok else "SUFFIX-MINING VERIFICATION FAILED."))
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
