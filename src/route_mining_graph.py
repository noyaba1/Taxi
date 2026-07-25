"""
route_mining_graph.py  --  METHOD C  (transition graph)
=======================================================
The ORIGINAL method (neither clustering nor suffix): model the city as a directed
weighted graph of H3 cells and mine it.

    node   = H3 cell
    edge   = an observed transition cell_i -> cell_{i+1}
    weight = number of DISTINCT trips that made that transition (deduped per trip)

From this graph we produce BOTH assignment deliverables:
  * ACTIVITY ZONES  = cells with the highest PageRank / weighted throughput
    (PageRank = power iteration with DataFrame joins; no GraphFrames).
  * POPULAR ROUTES  = HEAVY PATHS: greedy walks that follow the busiest
    transitions (graph-guided candidate generation, unlike Method B's exhaustive
    window enumeration), then VALIDATED against real trips by containment support
    so we never emit a "Frankenstein route" no taxi drove.

Validation uses one Aho-Corasick pass per trip rather than testing every
candidate against every trip: the old loop was up to 3,000 substring scans x
1.71M trips, which is not a thing that finishes.

Run:
    python -m src.route_mining_graph --sample
"""
import time
from collections import defaultdict
from datetime import datetime, timezone

import h3
from pyspark.sql import functions as F, types as T

from src import ahocorasick
from src import cells as cells_mod
from src import cli, config, storage
from src.route_mining_exact import DELIM
from src.spark_session import get_spark

THRESHOLDS = config.ROUTE_LENGTH_THRESHOLDS_KM
TOP_K = config.TOP_K
MAX_L_CAP = config.MAX_SUBROUTE_KM

log = cli.setup_logging("m10")


@F.udf(T.ArrayType(T.StructType([
    T.StructField("f", T.StringType()), T.StructField("t", T.StringType())])))
def transitions(seq):
    """
    Distinct consecutive (from, to) cell pairs in one trip (deduped).

    Taken within gap-free segments: a jump across lost GPS signal is not a road
    the taxi drove, and an edge built from one would put a phantom shortcut into
    the graph that heavy-path walks would happily follow.
    """
    pairs = set()
    for seg in cells_mod.split_at_gaps(seq):
        for i in range(len(seg) - 1):
            pairs.add((seg[i], seg[i + 1]))
    return [{"f": a, "t": b} for a, b in pairs]


def build_edges(trips):
    """(f, t, weight=distinct-trip transition support)."""
    return (trips.select(F.explode(transitions("h3_seq_compact")).alias("e"))
            .select("e.f", "e.t")
            .groupBy("f", "t").agg(F.count(F.lit(1)).alias("w")))


def pagerank(edges, iters, damping):
    """
    Weighted PageRank via power iteration (Spark-native, lineage-truncated).

    DANGLING NODES ARE HANDLED. Cells with no outgoing frequent transition --
    trip endpoints, taxi ranks, the edge of the covered area -- have nowhere to
    send their rank. Dropping it silently (the obvious implementation) leaks
    probability every iteration: the vector stops summing to 1 and rank drifts
    toward whatever sinks happen to have high in-degree, which is exactly the
    activity-zone ranking we report. So each iteration collects the dangling mass
    and redistributes it uniformly, as the standard formulation requires.
    """
    nodes = (edges.select(F.col("f").alias("id"))
             .union(edges.select(F.col("t").alias("id"))).distinct()).cache()
    n = nodes.count()
    outw = edges.groupBy("f").agg(F.sum("w").alias("outw"))
    e2 = (edges.join(outw, "f")
          .withColumn("frac", F.col("w") / F.col("outw"))
          .select("f", "t", "frac").cache())

    # Nodes with no outgoing edge at all -> their rank is "dangling" mass.
    senders = edges.select(F.col("f").alias("id")).distinct()
    dangling = nodes.join(senders, "id", "left_anti").cache()
    dangling.count()

    pr = nodes.withColumn("pr", F.lit(1.0 / n))
    teleport = (1.0 - damping) / n
    for _ in range(iters):
        contrib = (e2.join(pr, e2.f == pr.id)
                   .withColumn("c", F.col("pr") * F.col("frac"))
                   .groupBy(F.col("t").alias("id")).agg(F.sum("c").alias("inc")))
        # Mass sitting on dangling nodes this iteration, spread over all nodes.
        lost = (dangling.join(pr, "id").agg(F.sum("pr")).collect()[0][0]) or 0.0
        share = damping * lost / n
        pr = (nodes.join(contrib, "id", "left")
              .withColumn("pr",
                          F.lit(teleport + share)
                          + damping * F.coalesce("inc", F.lit(0.0)))
              .select("id", "pr")).localCheckpoint(eager=True)
    return pr, n


def heavy_paths(fedges, min_prob):
    """
    Dominant-flow corridors: greedy walks seeded by the top frequent edges that
    extend only while the next transition carries >= `min_prob` of the current
    cell's flow. This follows the city's main flows and STOPS at forks (where the
    flow splits below min_prob) -> coherent corridors that actually validate,
    instead of over-extended 'Frankenstein' paths. Driver-side on frequent edges.

    Path length is tracked incrementally; recomputing it from scratch each step
    made this O(k^2) per seed for no reason.
    """
    succ, pred = defaultdict(list), defaultdict(list)
    outw, inw = defaultdict(int), defaultdict(int)
    for f, t, w in fedges:
        succ[f].append((w, t))
        pred[t].append((w, f))
        outw[f] += w
        inw[t] += w
    for k in succ:
        succ[k].sort(reverse=True)
    for k in pred:
        pred[k].sort(reverse=True)

    def hop(a, b):
        return h3.point_dist(h3.h3_to_geo(a), h3.h3_to_geo(b), unit="km")

    seeds = sorted(fedges, key=lambda e: -e[2])[:config.GRAPH_MAX_SEEDS]
    paths = set()
    for f0, t0, _w in seeds:
        cells, visited = [f0, t0], {f0, t0}
        length = hop(f0, t0)
        cur = t0                                    # extend forward on dominant flow
        while length < MAX_L_CAP:
            nxt = next(((w, t) for w, t in succ[cur] if t not in visited), None)
            if not nxt or outw[cur] == 0 or nxt[0] / outw[cur] < min_prob:
                break
            step = hop(cur, nxt[1])
            if length + step > MAX_L_CAP:
                break
            cells.append(nxt[1])
            visited.add(nxt[1])
            length += step
            cur = nxt[1]
        cur = f0                                    # extend backward on dominant inflow
        while length < MAX_L_CAP:
            prv = next(((w, f) for w, f in pred[cur] if f not in visited), None)
            if not prv or inw[cur] == 0 or prv[0] / inw[cur] < min_prob:
                break
            step = hop(prv[1], cur)
            if length + step > MAX_L_CAP:
                break
            cells.insert(0, prv[1])
            visited.add(prv[1])
            length += step
            cur = prv[1]
        if len(cells) >= 2:
            paths.add(tuple(cells))
    return [list(p) for p in paths]


def main(scale: str) -> None:
    spark = get_spark("route-mining-graph")
    paths_cfg = config.dataset_paths(scale)

    with cli.stage("m10_graph", scale, log) as st:
        t0 = time.time()
        trips = spark.read.parquet(paths_cfg["encoded"]).select("h3_seq_compact").cache()
        n_trips = trips.count()

        edges = build_edges(trips).cache()
        n_edges = edges.count()

        # ---- ACTIVITY ZONES: PageRank + weighted throughput ----
        pr, n_nodes = pagerank(edges, config.PAGERANK_ITERS, config.PAGERANK_DAMPING)
        throughput = edges.groupBy(F.col("t").alias("id")).agg(F.sum("w").alias("in_w"))
        zones = (pr.join(throughput, "id", "left")
                 .withColumn("in_w", F.coalesce("in_w", F.lit(0)))
                 .orderBy(F.col("pr").desc())
                 .limit(config.ACTIVITY_ZONES_TOP).collect())

        zpath = storage.write_csv(
            storage.out_path("routes", f"activity_zones_{scale}.csv"),
            ["rank", "cell", "lat", "lon", "pagerank", "in_traffic"],
            [(rank, z["id"], *[round(v, 6) for v in h3.h3_to_geo(z["id"])],
              round(z["pr"], 10), z["in_w"])
             for rank, z in enumerate(zones, 1)])

        # ---- POPULAR ROUTES: greedy heavy paths, then validate against trips ----
        fedges = [(r["f"], r["t"], r["w"]) for r in
                  edges.filter(F.col("w") >= config.GRAPH_MIN_EDGE_SUPPORT).collect()]
        cand = heavy_paths(fedges, config.GRAPH_MIN_FLOW_PROB)
        cand_len = [cells_mod.path_length_km(c) for c in cand]

        # Validate ALL candidates in ONE pass per trip (Aho-Corasick), not one
        # pass per (trip, candidate) pair.
        support = {}
        if cand:
            token_rdd = trips.rdd.map(lambda r: list(r[0]))
            support = dict(
                ahocorasick.containment_support(token_rdd, [tuple(c) for c in cand])
                .collect())

        routes = [(support.get(i, 0), cand_len[i], len(cand[i]), DELIM.join(cand[i]))
                  for i in range(len(cand)) if support.get(i, 0) > 0]

        log.info("=" * 60)
        log.info("trips               : %s", f"{n_trips:,}")
        log.info("graph nodes / edges : %s / %s", f"{n_nodes:,}", f"{n_edges:,}")
        log.info("frequent edges (>=%d): %s", config.GRAPH_MIN_EDGE_SUPPORT, f"{len(fedges):,}")
        log.info("heavy paths cand/validated : %s / %s", f"{len(cand):,}", f"{len(routes):,}")
        log.info("=" * 60)

        rep = [f"# M10 Method C: Transition Graph ({scale})",
               f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
               "",
               f"trips: {n_trips:,} | nodes: {n_nodes:,} | edges: {n_edges:,} | "
               f"frequent edges: {len(fedges):,} | heavy paths validated: {len(routes):,}",
               "",
               "## Popular routes (heavy paths validated vs trips) per length config",
               "| min_len_km | #validated(>=L) | top_support | longest_km |",
               "|---|---|---|---|"]

        all_rows = []
        for L in THRESHOLDS:
            c = sorted([r for r in routes if r[1] >= L], key=lambda r: (-r[0], -r[1]))[:TOP_K]
            top_sup = c[0][0] if c else 0
            longest = max((r[1] for r in c), default=0.0)
            rep.append(f"| {L} | {len(c):,} | {top_sup:,} | {longest:.2f} |")
            for rank, (sup, ln, nc, route) in enumerate(c, 1):
                all_rows.append((L, rank, sup, round(ln, 3), nc, route))

        rpath = storage.write_csv(
            storage.out_path("routes", f"graph_heavy_paths_top100_{scale}.csv"),
            ["min_len_km", "rank", "support", "length_km", "n_cells", "route"],
            all_rows)

        rep += ["", "## Top activity zones (PageRank, dangling mass redistributed)",
                "| rank | cell | lat | lon | pagerank |", "|---|---|---|---|---|"]
        for rank, z in enumerate(zones[:10], 1):
            lat, lon = h3.h3_to_geo(z["id"])
            rep.append(f"| {rank} | {z['id']} | {lat:.5f} | {lon:.5f} | {z['pr']:.6f} |")

        rp = storage.write_lines(
            storage.out_path("statistics", f"m10_graph_{scale}.md"), rep)

        log.info("\n%s", "\n".join(rep))
        log.info("wrote heavy paths -> %s", rpath)
        log.info("wrote zones       -> %s", zpath)
        log.info("wrote report      -> %s", rp)
        st.update(trips=n_trips, nodes=n_nodes, edges=n_edges,
                  frequent_edges=len(fedges), candidates=len(cand),
                  validated=len(routes), mining_s=round(time.time() - t0, 1))

    spark.stop()
    log.info("M10 GRAPH MINING COMPLETE.")


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
