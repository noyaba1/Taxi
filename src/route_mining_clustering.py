"""
route_mining_clustering.py  --  PHASE 6 / Milestone M9  (METHOD A: clustering)
=============================================================================
Discover popular corridors by CLUSTERING similar trajectories, then reporting the
representative route of each cluster. A different lens from Method B (which mines
frequent sub-route *strings*): here we group *whole trips* by similarity.

PIPELINE (MLlib / Spark-native; no GraphFrames dependency):
  1. trip -> set of DIRECTED bigram shingles (cell_i -> cell_{i+1}). Bigrams (not
     the raw cell set) preserve ORDER and DIRECTION (DESIGN_REVIEW fix #1).
  2. HashingTF -> fixed-dim binary feature vector of the shingle set.
  3. MinHashLSH.approxSimilarityJoin -> pairs of trips with Jaccard distance
     <= threshold. This is the O(N^2)->~O(N) approximate structure (LSH).
  4. GREEDY STAR (canopy) clustering over the pruned similarity graph: repeatedly
     take the highest-degree unassigned trip as a SEED and attach every trip
     DIRECTLY similar to it. Membership requires direct similarity to the seed, so
     clusters cannot "chain" dissimilar trips the way connected-components does
     (DESIGN_REVIEW fix #2). The seed is a central, coherent representative.
  5. cluster size = popularity; representative route = the seed's trajectory.

The heavy step (3) is distributed; step 4 runs on the small pruned edge list.
Knobs in config.py (LSH_*, CLUSTER_MIN_SIZE). At full scale the edge list is
capped (EDGE_COLLECT_CAP) and a distributed community-detection would replace the
driver-side clustering (documented limitation).

Run:
    python -m src.route_mining_clustering --sample
"""
import argparse
import csv
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

from pyspark.sql import functions as F, types as T
from pyspark.ml.feature import HashingTF, MinHashLSH

from src.spark_session import get_spark
from src import config
from src.route_mining_exact import DELIM

THRESHOLDS = config.ROUTE_LENGTH_THRESHOLDS_KM
TOP_K = config.TOP_K
EDGE_COLLECT_CAP = 5_000_000     # safety: refuse to collect an unbounded graph


@F.udf(T.ArrayType(T.StringType()))
def bigram_shingles(seq):
    """Directed consecutive-cell shingles: [c0>c1, c1>c2, ...] (deduped)."""
    if not seq or len(seq) < 2:
        return []
    return list({seq[i] + DELIM + seq[i + 1] for i in range(len(seq) - 1)})


def star_cluster(edges, min_size):
    """
    Greedy star/canopy clustering on an undirected edge list [(src,dst), ...].
    Returns {cluster_id: (seed_node, [members])}. Every member is DIRECTLY
    adjacent to its seed -> coherent, no transitive chaining.
    """
    adj = defaultdict(set)
    for s, d in edges:
        adj[s].add(d)
        adj[d].add(s)
    assigned, clusters, cid = {}, {}, 0
    for node in sorted(adj, key=lambda n: (-len(adj[n]), n)):   # highest degree first
        if node in assigned:
            continue
        members = [node] + [m for m in adj[node] if m not in assigned]
        if len(members) < min_size:
            continue
        for m in members:
            assigned[m] = cid
        clusters[cid] = (node, members)
        cid += 1
    return clusters


def main(use_sample: bool) -> None:
    spark = get_spark("route-mining-clustering")
    t0 = time.time()
    suffix = "sample" if use_sample else "full"
    res = config.H3_RESOLUTION

    enc_path = config.CLEAN_PARQUET.replace(".parquet", f"_encoded_r{res}_{suffix}.parquet")
    trips = (spark.read.parquet(enc_path)
             .select("TRIP_ID", "h3_seq_compact", "encoded_len_km")
             .filter(F.size("h3_seq_compact") >= 2)
             .withColumn("nid", F.monotonically_increasing_id())).cache()
    n_trips = trips.count()

    # ---- 1-2. shingles -> hashed binary features ----
    feats = trips.withColumn("shingles", bigram_shingles("h3_seq_compact")) \
                 .filter(F.size("shingles") >= 1)
    htf = HashingTF(inputCol="shingles", outputCol="features",
                    numFeatures=config.LSH_NUM_FEATURES, binary=True)
    feats = htf.transform(feats).cache()

    # ---- 3. MinHash-LSH approximate similarity self-join (distributed) ----
    lsh = MinHashLSH(inputCol="features", outputCol="hashes",
                     numHashTables=config.LSH_NUM_HASH_TABLES)
    model = lsh.fit(feats)
    pairs = (model.approxSimilarityJoin(feats, feats, config.LSH_JACCARD_DIST_MAX, "jdist")
             .select(F.col("datasetA.nid").alias("src"), F.col("datasetB.nid").alias("dst"))
             .filter(F.col("src") < F.col("dst")))
    n_edges = pairs.count()
    if n_edges > EDGE_COLLECT_CAP:
        raise RuntimeError(f"similarity graph too large to collect ({n_edges:,}); "
                           f"tighten LSH_JACCARD_DIST_MAX or use distributed CC")

    # ---- 4. greedy star clustering on the pruned graph (driver-side, small) ----
    edge_list = [(r["src"], r["dst"]) for r in pairs.collect()]
    clusters = star_cluster(edge_list, config.CLUSTER_MIN_SIZE)
    n_clusters = len(clusters)

    # ---- 5. seed representatives + sizes ----
    seed_nids = {seed for seed, _ in clusters.values()}
    seed_info = {r["nid"]: (r["TRIP_ID"], r["encoded_len_km"], r["h3_seq_compact"])
                 for r in trips.filter(F.col("nid").isin(list(seed_nids)))
                 .select("nid", "TRIP_ID", "encoded_len_km", "h3_seq_compact").collect()}
    # rows: (size, rep_len, rep_trip, rep_route)
    crows = []
    for cid, (seed, members) in clusters.items():
        trip_id, rep_len, route = seed_info[seed]
        crows.append((len(members), float(rep_len or 0.0), trip_id, DELIM.join(route)))

    print("=" * 60)
    print(f"trips (>=2 cells)   : {n_trips:,}")
    print(f"similarity edges    : {n_edges:,}  (Jaccard dist <= {config.LSH_JACCARD_DIST_MAX})")
    print(f"clusters (size>={config.CLUSTER_MIN_SIZE}) : {n_clusters:,}")
    print("=" * 60)

    rep = [f"# M9 Method A: Clustering Route Discovery ({suffix})",
           f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
           f"\ntrips: {n_trips:,} | LSH tables={config.LSH_NUM_HASH_TABLES} "
           f"jdist<={config.LSH_JACCARD_DIST_MAX} (sim>={1-config.LSH_JACCARD_DIST_MAX}) | "
           f"edges: {n_edges:,} | clusters(size>={config.CLUSTER_MIN_SIZE}): {n_clusters:,}\n",
           "## Top clusters (corridors) per length config",
           "| min_len_km | #clusters(rep>=L) | top_size | rep_len_km |", "|---|---|---|---|"]

    routes_dir = os.path.join(config.OUTPUT_BASE, "routes")
    os.makedirs(routes_dir, exist_ok=True)
    all_rows = []
    for L in THRESHOLDS:
        cand = sorted([c for c in crows if c[1] >= L], key=lambda c: (-c[0], -c[1]))[:TOP_K]
        top_size = cand[0][0] if cand else 0
        rep_len = cand[0][1] if cand else 0.0
        rep.append(f"| {L} | {len(cand):,} | {top_size:,} | {rep_len:.2f} |")
        for rank, (size, rlen, trip, route) in enumerate(cand, 1):
            all_rows.append((L, rank, size, round(rlen, 3), trip, route))

    csv_path = os.path.join(routes_dir, f"clustering_top100_{suffix}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["min_len_km", "rank", "cluster_size", "rep_len_km", "rep_trip_id", "rep_route"])
        w.writerows(all_rows)

    rp = os.path.join(config.OUTPUT_BASE, "statistics", f"m9_clustering_{suffix}.md")
    os.makedirs(os.path.dirname(rp), exist_ok=True)
    with open(rp, "w", encoding="utf-8") as fh:
        fh.write("\n".join(rep) + "\n")

    elapsed = time.time() - t0
    print("\n".join(rep))
    print(f"\n[m9] wrote clusters -> {csv_path}")
    print(f"[m9] wrote report   -> {rp}")
    print(f"[m9] wall time      : {elapsed:.1f}s")
    spark.stop()
    print("M9 CLUSTERING COMPLETE.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
