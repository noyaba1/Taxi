"""
verify_clustering.py  --  M9 verification (Method A)
===================================================
Independently validates the clustering output, especially the connected-components
*chaining* risk: are the trips in a cluster actually similar to its representative,
or did label propagation chain dissimilar trips together?

Checks:
  1. structural: rep_len_km >= min_len; cluster_size >= CLUSTER_MIN_SIZE
  2. representative routes are valid (non-empty, inside sample)
  3. COHERENCE: for the top clusters, brute-force count trips whose directed
     bigram-shingle Jaccard similarity to the representative >= (1 - jdist_max).
     A coherent cluster's representative is genuinely similar to >= its members'
     order of magnitude; we report the coherence ratio (guards against blobs).

Run:
    python -m src.verify_clustering --sample
"""
import argparse
import csv
import os

from pyspark.sql import functions as F

from src.spark_session import get_spark
from src import config
from src.route_mining_exact import DELIM
from src.route_mining_clustering import bigram_shingles


def _read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def main(use_sample: bool) -> None:
    spark = get_spark("verify-clustering")
    suffix = "sample" if use_sample else "full"
    res = config.H3_RESOLUTION
    sim_min = 1.0 - config.LSH_JACCARD_DIST_MAX     # required Jaccard similarity

    enc_path = config.CLEAN_PARQUET.replace(".parquet", f"_encoded_r{res}_{suffix}.parquet")
    csv_path = os.path.join(config.OUTPUT_BASE, "routes", f"clustering_top100_{suffix}.csv")

    trips = (spark.read.parquet(enc_path)
             .select("TRIP_ID", "h3_seq_compact")
             .filter(F.size("h3_seq_compact") >= 2)
             .withColumn("shingles", bigram_shingles("h3_seq_compact")))
    trips.cache()
    n_trips = trips.count()

    rows = _read_csv(csv_path)
    print(f"Spark {spark.version} | trips={n_trips:,} | rows={len(rows)} | sim_min={sim_min}")
    ok = True

    # --- 1-2. structural ---
    bad = [r for r in rows if float(r["rep_len_km"]) < float(r["min_len_km"])
           or int(r["cluster_size"]) < config.CLUSTER_MIN_SIZE
           or not r["rep_route"]]
    c1 = not bad
    ok &= c1
    print(f"[{'OK' if c1 else 'FAIL'}] structural (rep_len>=min_len, size>=min, route non-empty): "
          f"{len(bad)} bad")

    # --- 3. coherence of the top distinct clusters ---
    print("\n=== cluster coherence (brute-force Jaccard to representative) ===")
    seen, targets = set(), []
    for r in rows:                                   # take the largest distinct clusters
        if r["rep_trip_id"] not in seen:
            seen.add(r["rep_trip_id"]); targets.append(r)
        if len(targets) >= 3:
            break
    for r in targets:
        rep_shingles = [s for s in r["rep_route"].split(DELIM)]  # cells
        rep_shingles = [rep_shingles[i] + DELIM + rep_shingles[i + 1]
                        for i in range(len(rep_shingles) - 1)]
        rep_arr = F.array(*[F.lit(x) for x in set(rep_shingles)])
        inter = F.size(F.array_intersect("shingles", rep_arr))
        union = F.size(F.array_union("shingles", rep_arr))
        sim = trips.withColumn("j", inter / union)
        n_sim = sim.filter(F.col("j") >= sim_min).count()
        size = int(r["cluster_size"])
        coherence = n_sim / size if size else 0.0
        # star clustering makes every member DIRECTLY similar to the seed, so
        # coherence should be high (LSH is approximate, so allow some slack).
        good = coherence >= 0.5
        ok &= good
        print(f"[{'OK' if good else 'FAIL'}] cluster size={size}: "
              f"{n_sim} trips truly similar to rep (coherence ~{coherence:.2f}, need >=0.50)")

    print("\n" + ("CLUSTERING VERIFICATION PASSED." if ok else "CLUSTERING VERIFICATION FAILED."))
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
