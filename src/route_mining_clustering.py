"""
route_mining_clustering.py  --  METHOD A  (trajectory clustering)
=================================================================
Discover popular corridors by CLUSTERING similar trajectories, then reporting
the SUB-ROUTE that the cluster's members actually share.

WHAT CHANGED AND WHY
--------------------
This method used to report each cluster's seed trip in full -- its entire
trajectory, start to end, with the whole-trip length. That answers "which whole
trips resemble each other", which is precisely what the assignment rules out:
a popular route is NOT a full trip from origin to destination, because full trips
are nearly unique and therefore never popular.

So after clustering we extract, per cluster, the LONGEST CONTIGUOUS CELL RUN
present in at least `CLUSTER_SUBROUTE_PCT` of its members. That is the corridor
the cluster agrees on; the parts where members peel off to their own origins and
destinations are dropped. The result is a sub-route, in the same units as
Methods B/C, so the cross-method comparison finally compares like with like.

Each run's popularity is then re-measured against ALL trips by containment, not
just against its own cluster, so `support` means the same thing in every method.

PIPELINE (MLlib / Spark-native; no GraphFrames dependency):
  1. trip -> set of DIRECTED bigram shingles (cell_i -> cell_{i+1}), taken within
     gap-free segments so a lost-signal jump is never treated as a transition.
  2. HashingTF -> fixed-dim binary feature vector of the shingle set.
  3. MinHashLSH.approxSimilarityJoin -> pairs of trips with Jaccard distance
     <= threshold. This is the O(N^2)->~O(N) approximate structure (LSH).
  4. GREEDY STAR (canopy) clustering over the pruned similarity graph: repeatedly
     take the highest-degree unassigned trip as a SEED and attach every trip
     DIRECTLY similar to it. Membership requires direct similarity to the seed, so
     clusters cannot "chain" dissimilar trips the way connected-components does.
  5. per cluster: longest cell run shared by >= CLUSTER_SUBROUTE_PCT of members.
  6. global support for every run, via one Aho-Corasick pass over all trips.

Run:
    python -m src.route_mining_clustering --sample
"""
import time
from collections import defaultdict
from datetime import datetime, timezone

from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.sql import functions as F, types as T

from src import ahocorasick
from src import cells as cells_mod
from src import cli, config, storage
from src.route_mining_exact import DELIM
from src.spark_session import get_spark

THRESHOLDS = config.ROUTE_LENGTH_THRESHOLDS_KM
TOP_K = config.TOP_K
EDGE_COLLECT_CAP = 5_000_000     # safety: refuse to collect an unbounded graph

log = cli.setup_logging("m9")


@F.udf(T.ArrayType(T.StringType()))
def bigram_shingles(seq):
    """
    Directed consecutive-cell shingles: [c0>c1, c1>c2, ...] (deduped).

    Built per gap-free segment: a bigram spanning a GPS hole is not a transition
    the vehicle made, and letting it into the LSH signature makes unrelated trips
    look similar because they share the same tracking artefact.
    """
    out = set()
    for seg in cells_mod.split_at_gaps(seq):
        for i in range(len(seg) - 1):
            out.add(seg[i] + DELIM + seg[i + 1])
    return list(out)


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


# ------------------------------------------------- shared sub-route extraction
def run_in_at_least(members, h, k):
    """
    Longest-first helper: the most widely shared contiguous run of length `h`
    present in at least `k` of `members`, or None.
    Returns (run_tuple, n_members_containing).

    A member is a LIST OF SEGMENTS, not a sequence: a trip whose trace has GPS
    gaps is split into several gap-free stretches, and all of them belong to the
    one trip. The dedupe set is therefore per MEMBER and spans its segments, so a
    trip that was split three ways still contributes at most 1 to the count.
    Deduping per segment instead let one trip clear a "60% of members" threshold
    by itself.
    """
    counts: dict = {}
    for segments in members:
        seen = set()
        for seq in segments:
            for i in range(len(seq) - h + 1):
                w = tuple(seq[i:i + h])
                if w not in seen:        # count MEMBERS, not occurrences
                    seen.add(w)
                    counts[w] = counts.get(w, 0) + 1
    best = None
    for w, c in counts.items():
        if c >= k and (best is None or c > best[1] or (c == best[1] and w < best[0])):
            best = (w, c)
    return best


def longest_shared_run(members, min_members):
    """
    The longest contiguous cell run contained in >= `min_members` MEMBERS.

    `members` is one entry per cluster member (per trip), each entry a list of
    that member's gap-free segments. A bare sequence is accepted as a member with
    a single segment, so the callers in the tests read naturally.

    Binary search on length is valid because the property is monotone: if a run
    of length h is in k members, each of its length-(h-1) sub-runs is in at
    least k as well. So we probe O(log max_len) lengths instead of all of them.
    """
    members = [[m] if m and isinstance(m[0], str) else m for m in members]
    members = [[s for s in segs if s] for segs in members]
    members = [segs for segs in members if segs]
    if not members:
        return None
    lo = 1
    hi = max(len(s) for segs in members for s in segs)
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        found = run_in_at_least(members, mid, min_members)
        if found:
            best, lo = found, mid + 1
        else:
            hi = mid - 1
    return best


def discover(spark, scale: str):
    """
    Run Method A's full discovery and RETURN the result without writing anything.

    Split out of main() so the sampling-cap experiment can drive the real
    pipeline at several caps -- an experiment that measures a reimplementation
    measures the reimplementation, not the method.

    Returns (routes, meta):
      routes = [(support, cluster_size, members_with_run, length_km, n_cells,
                 subroute)] with `support` counted against ALL trips
      meta   = {n_all, n_clustered, n_edges, n_clusters}
    """
    paths = config.dataset_paths(scale)
    trips = (spark.read.parquet(paths["encoded"])
             .select("TRIP_ID", "h3_seq_compact", "encoded_len_km")
             .filter(F.size("h3_seq_compact") >= 2)
             # Deterministic id: monotonically_increasing_id() changes if the
             # frame is ever recomputed, and it is used as a JOIN KEY between
             # the LSH edge list and the member lookup.
             .withColumn("nid", F.xxhash64("TRIP_ID"))).cache()
    n_all = trips.count()

    # Bound the LSH self-join: its edge count grows ~quadratically with #trips,
    # so above the cap we cluster a representative sample. Whether that loses
    # corridors is measured, not assumed -- see experiment_cluster_cap.py.
    clustered_pop = trips
    if n_all > config.CLUSTERING_MAX_TRIPS:
        clustered_pop = trips.sample(config.CLUSTERING_MAX_TRIPS / n_all,
                                     seed=42).cache()
    n_trips = clustered_pop.count()

    # ---- 1-2. shingles -> hashed binary features ----
    feats = (clustered_pop.withColumn("shingles", bigram_shingles("h3_seq_compact"))
             .filter(F.size("shingles") >= 1))
    htf = HashingTF(inputCol="shingles", outputCol="features",
                    numFeatures=config.LSH_NUM_FEATURES, binary=True)
    feats = htf.transform(feats).cache()

    # ---- 3. MinHash-LSH approximate similarity self-join (distributed) ----
    lsh = MinHashLSH(inputCol="features", outputCol="hashes",
                     numHashTables=config.LSH_NUM_HASH_TABLES)
    model = lsh.fit(feats)
    pairs = (model.approxSimilarityJoin(feats, feats,
                                        config.LSH_JACCARD_DIST_MAX, "jdist")
             .select(F.col("datasetA.nid").alias("src"),
                     F.col("datasetB.nid").alias("dst"))
             .filter(F.col("src") < F.col("dst")))
    n_edges = pairs.count()
    if n_edges > EDGE_COLLECT_CAP:
        raise RuntimeError(
            f"similarity graph too large to collect ({n_edges:,} > "
            f"{EDGE_COLLECT_CAP:,}); tighten LSH_JACCARD_DIST_MAX or lower "
            f"CLUSTERING_MAX_TRIPS")

    # ---- 4. greedy star clustering on the pruned graph (driver-side, bounded) ----
    edge_list = [(r["src"], r["dst"]) for r in pairs.collect()]
    clusters = star_cluster(edge_list, config.CLUSTER_MIN_SIZE)
    meta = {"n_all": n_all, "n_clustered": n_trips, "n_edges": n_edges,
            "n_clusters": len(clusters)}
    log.info("edges=%s -> clusters=%s", f"{n_edges:,}", f"{len(clusters):,}")
    if not clusters:
        return [], meta

    # ---- 5. per cluster: the sub-route its members share ----
    membership = spark.createDataFrame(
        [(cid, int(nid)) for cid, (_seed, members) in clusters.items()
         for nid in members],
        schema=T.StructType([T.StructField("cid", T.IntegerType()),
                             T.StructField("nid", T.LongType())]))
    pct = config.CLUSTER_SUBROUTE_PCT
    min_len = float(min(THRESHOLDS))

    run_schema = T.StructType([
        T.StructField("cid", T.IntegerType()),
        T.StructField("cluster_size", T.IntegerType()),
        T.StructField("members_with_run", T.IntegerType()),
        T.StructField("n_cells", T.IntegerType()),
        T.StructField("length_km", T.DoubleType()),
        T.StructField("subroute", T.StringType()),
    ])

    def _cluster_run(pdf):
        import math

        import pandas as pd

        cid = int(pdf["cid"].iloc[0])
        # One entry PER TRIP, holding that trip's gap-free stretches -- only such
        # a stretch can host a shared corridor, but all of them are still the one
        # trip. Flattening them into a single list made `need` a segment count on
        # one side and a trip count on the other.
        members = [cells_mod.split_at_gaps(list(seq))
                   for seq in pdf["h3_seq_compact"]]
        size = len(pdf)
        need = max(2, math.ceil(pct * size))
        best = longest_shared_run(members, need)
        if not best:
            return pd.DataFrame(columns=run_schema.fieldNames())
        run, n_with = best
        length = cells_mod.path_length_km(list(run))
        if length < min_len:
            return pd.DataFrame(columns=run_schema.fieldNames())
        # A shared run that keeps re-entering the same cell is a cluster of
        # vehicles circling, and `length` above is the sum of the circling.
        # Method D and the window miners apply this at emission; Method A builds
        # its corridors by a different route (longest run common to a cluster)
        # and so needs it applied here rather than inheriting it.
        if not cells_mod.revisits_ok(list(run)):
            return pd.DataFrame(columns=run_schema.fieldNames())
        return pd.DataFrame([(cid, size, int(n_with), len(run), float(length),
                              DELIM.join(run))],
                            columns=run_schema.fieldNames())

    runs = (membership.join(trips.select("nid", "h3_seq_compact"), "nid")
            .groupBy("cid").applyInPandas(_cluster_run, schema=run_schema)
            .collect())

    # ---- 6. global support: how many of ALL trips contain each run? ----
    patterns = [tuple(r["subroute"].split(DELIM)) for r in runs]
    support = {}
    if patterns:
        token_rdd = (trips.select("h3_seq_compact").rdd
                     .map(lambda r: list(r["h3_seq_compact"])))
        support = dict(ahocorasick.containment_support(token_rdd, patterns).collect())

    routes = [(support.get(i, 0), r["cluster_size"], r["members_with_run"],
               float(r["length_km"]), r["n_cells"], r["subroute"])
              for i, r in enumerate(runs)]
    return routes, meta


def main(scale: str) -> None:
    spark = get_spark("route-mining-clustering")

    with cli.stage("m9_clustering", scale, log) as st:
        t0 = time.time()
        crows, meta = discover(spark, scale)
        n_all, n_trips = meta["n_all"], meta["n_clustered"]

        if not crows:
            log.warning("no clusters at size>=%d; nothing to report",
                        config.CLUSTER_MIN_SIZE)
            _write_empty(scale)
            spark.stop()
            return

        log.info("=" * 60)
        log.info("trips (clustered)   : %s of %s", f"{n_trips:,}", f"{n_all:,}")
        log.info("similarity edges    : %s  (Jaccard dist <= %s)",
                 f"{meta['n_edges']:,}", config.LSH_JACCARD_DIST_MAX)
        log.info("clusters (size>=%d)  : %s", config.CLUSTER_MIN_SIZE,
                 f"{meta['n_clusters']:,}")
        log.info("reported sub-routes : %s", f"{len(crows):,}")
        log.info("=" * 60)

        pct = config.CLUSTER_SUBROUTE_PCT
        rep = [f"# M9 Method A: Clustering Route Discovery ({scale})",
               f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
               "",
               f"trips clustered: {n_trips:,} of {n_all:,} "
               f"(cap CLUSTERING_MAX_TRIPS={config.CLUSTERING_MAX_TRIPS:,}) | "
               f"LSH tables={config.LSH_NUM_HASH_TABLES} "
               f"jdist<={config.LSH_JACCARD_DIST_MAX} | edges: {meta['n_edges']:,} | "
               f"clusters: {meta['n_clusters']:,}",
               "",
               f"Each cluster reports the longest cell run shared by >={pct:.0%} of its",
               "members -- a SUB-route, not a whole trip. `support` is then measured",
               "against ALL trips by containment, so it is directly comparable with",
               "Methods B, C and D.",
               ""]
        if n_all > config.CLUSTERING_MAX_TRIPS:
            rep += [f"> **Caveat:** clustering ran on a {n_trips / n_all:.1%} sample of "
                    f"trips ({n_trips:,}/{n_all:,}); `cluster_size` is therefore a "
                    f"sample statistic. `support` is not -- it is measured on all "
                    f"{n_all:,} trips. Whether the cap loses corridors is measured "
                    f"in `experiment_cluster_cap_*.md`.", ""]
        rep += ["| min_len_km | #routes(>=L) | top_support | longest_km |",
                "|---|---|---|---|"]

        all_rows = []
        for L in THRESHOLDS:
            cand = sorted([c for c in crows if c[3] >= L], key=lambda c: (-c[0], -c[3]))
            cand = cand[:TOP_K]
            top_sup = cand[0][0] if cand else 0
            longest = max((c[3] for c in cand), default=0.0)
            rep.append(f"| {L} | {len(cand):,} | {top_sup:,} | {longest:.2f} |")
            for rank, (sup, size, nwith, ln, nc, route) in enumerate(cand, 1):
                all_rows.append((L, rank, sup, round(ln, 3), nc, size, nwith, route))

        csv_path = storage.write_csv(
            storage.out_path("routes", f"clustering_top100_{scale}.csv"),
            ["min_len_km", "rank", "support", "length_km", "n_cells",
             "cluster_size", "members_with_run", "subroute"],
            all_rows)
        rp = storage.write_lines(
            storage.out_path("statistics", f"m9_clustering_{scale}.md"), rep)

        log.info("\n%s", "\n".join(rep))
        log.info("wrote clusters -> %s", csv_path)
        log.info("wrote report   -> %s", rp)
        st.update(trips=n_trips, trips_total=n_all, edges=meta["n_edges"],
                  clusters=meta["n_clusters"], routes=len(crows),
                  mining_s=round(time.time() - t0, 1))

    spark.stop()
    log.info("M9 CLUSTERING COMPLETE.")


def _write_empty(scale):
    storage.write_csv(
        storage.out_path("routes", f"clustering_top100_{scale}.csv"),
        ["min_len_km", "rank", "support", "length_km", "n_cells",
         "cluster_size", "members_with_run", "subroute"], [])


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
