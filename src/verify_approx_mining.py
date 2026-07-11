"""
verify_approx_mining.py  --  M7 verification (approx vs exact)
=============================================================
Independently validates the approximate sketches against exact ground truth and
checks the sketch invariants required by the spec.

Checks:
  1. structural: estimated supports are never negative; every route length >= its
     min_len threshold
  2. Count-Min never underestimates: cms_estimate >= exact_support (all rows)
  3. Space-Saving bound guarantee: for sampled routes, brute-force exact support S
     satisfies ss_lb <= S <= ss_ub; stored exact_support == brute-force S
  4. metrics recomputed from files: overlap@100 / precision@100 / recall@100
  5. determinism: rebuilding the sketches with the same seed/config yields an
     identical top-k

Run:
    python -m src.verify_approx_mining --sample
"""
import argparse
import csv
import os

from pyspark.sql import functions as F

from src.spark_session import get_spark
from src import config
from src.route_mining_exact import subroutes_udf, DELIM
from src.route_mining_approx import build_sketches, approx_topk, THRESHOLDS, TOP_K


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

    enc = spark.read.parquet(enc_path).select("h3_seq_compact")
    trips = enc.withColumn(
        "trip_str", F.concat(F.lit(DELIM), F.concat_ws(DELIM, "h3_seq_compact"), F.lit(DELIM))
    ).select("trip_str")
    trips.cache()
    n_trips = trips.count()

    approx = _read_csv(approx_csv)
    exact = _read_csv(exact_csv)
    print(f"Spark {spark.version} | trips={n_trips:,} | approx rows={len(approx)}")
    ok = True

    # --- 1. structural: non-negative estimates + thresholds respected ---
    neg = [r for r in approx if min(float(r["ss_estimate"]), float(r["ss_lb"]),
                                    float(r["cms_estimate"])) < 0]
    bad_len = [r for r in approx if r["length_km"] and float(r["length_km"]) < float(r["min_len_km"])]
    c1 = not neg and not bad_len
    ok &= c1
    print(f"[{'OK' if c1 else 'FAIL'}] non-negative estimates ({len(neg)} bad), "
          f"length>=min_len ({len(bad_len)} bad)")

    # --- 2. Count-Min never underestimates (all rows with a known exact support) ---
    under = [r for r in approx if r["exact_support"]
             and float(r["cms_estimate"]) < int(r["exact_support"])]
    c2 = not under
    ok &= c2
    print(f"[{'OK' if c2 else 'FAIL'}] Count-Min >= exact_support for all rows ({len(under)} violations)")

    # --- 3. Space-Saving bounds vs brute-force exact support (sampled) ---
    print("\n=== sketch bounds vs brute-force exact support ===")
    targets, seen = [], set()
    for r in approx:
        if r["min_len_km"] in ("1", "3", "5") and r["rank"] in ("1", "10", "50") \
                and r["subroute"] not in seen:
            seen.add(r["subroute"]); targets.append(r)
    for r in targets:
        needle = DELIM + r["subroute"] + DELIM
        S = trips.filter(F.instr("trip_str", needle) > 0).count()
        lb, ub, cms = float(r["ss_lb"]), float(r["ss_ub"]), float(r["cms_estimate"])
        stored = int(r["exact_support"]) if r["exact_support"] else None
        c = (lb <= S <= ub) and (cms >= S) and (stored is None or stored == S)
        ok &= c
        print(f"[{'OK' if c else 'FAIL'}] L>={r['min_len_km']} rank{r['rank']}: "
              f"exact={S} SS[{lb:.0f},{ub:.0f}] CMS={cms:.0f} stored={stored}")

    # --- 4. recompute overlap/precision/recall from files ---
    print("\n=== metrics recomputed from files (approx vs exact M5) ===")
    for L in ("1", "3", "5", "10", "20", "40"):
        a = {r["subroute"] for r in approx if r["min_len_km"] == L}
        e = {r["subroute"] for r in exact if r["min_len_km"] == L}
        ov = len(a & e)
        prec = ov / len(a) if a else 0.0
        rec = ov / len(e) if e else 0.0
        note = "  (support~1 ties -> arbitrary top-100)" if L in ("10", "20", "40") else ""
        print(f"  L>={L}km: overlap={ov} precision={prec:.2f} recall={rec:.2f}{note}")

    # --- 5. determinism: rebuild sketches twice, compare top-k ---
    print("\n=== determinism (same seed/config -> identical top-k) ===")
    windows = enc.select(F.explode(subroutes_udf("h3_seq_compact")).alias("w")).select("w.*").cache()
    windows.count()
    ss1, _c1, _p1, _m1 = build_sketches(windows)
    ss2, _c2, _p2, _m2 = build_sketches(windows)
    t1, t2 = approx_topk(ss1), approx_topk(ss2)
    det_ok = True
    for L in THRESHOLDS:
        top1 = [(k, est) for (k, est, *_1) in t1[L][:TOP_K]]
        top2 = [(k, est) for (k, est, *_1) in t2[L][:TOP_K]]
        if top1 != top2:
            det_ok = False
    ok &= det_ok
    print(f"[{'OK' if det_ok else 'FAIL'}] two independent builds produced identical top-k")

    print("\n" + ("APPROX VERIFICATION PASSED." if ok else "APPROX VERIFICATION FAILED."))
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
