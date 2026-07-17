"""
route_mining_graph.py  --  PHASE 7 / Milestone M10  (METHOD C: transition graph)
==============================================================================
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
    so we never emit a "Frankenstein route" no taxi drove (DESIGN_REVIEW C fix).

All knobs in config.py (GRAPH_MIN_EDGE_SUPPORT, PAGERANK_*, GRAPH_MAX_SEEDS,
ACTIVITY_ZONES_TOP). The heavy step (edge build + PageRank) is distributed; the
greedy walks run on the small frequent-edge list.

Run:
    python -m src.route_mining_graph --sample
"""
import argparse
import csv
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import h3
from pyspark.sql import functions as F, types as T

from src.spark_session import get_spark
from src import config
from src.route_mining_exact import DELIM

THRESHOLDS = config.ROUTE_LENGTH_THRESHOLDS_KM
TOP_K = config.TOP_K
MAX_L_CAP = config.MAX_SUBROUTE_KM


@F.udf(T.ArrayType(T.StructType([
    T.StructField("f", T.StringType()), T.StructField("t", T.StringType())])))
def transitions(seq):
    """Distinct consecutive (from, to) cell pairs in one trip (deduped)."""
    if not seq or len(seq) < 2:
        return []
    return [{"f": a, "t": b} for a, b in {(seq[i], seq[i + 1]) for i in range(len(seq) - 1)}]


def build_edges(trips):
    """(f, t, weight=distinct-trip transition support)."""
    return (trips.select(F.explode(transitions("h3_seq_compact")).alias("e"))
            .select("e.f", "e.t")
            .groupBy("f", "t").agg(F.count(F.lit(1)).alias("w")))


def pagerank(edges, iters, damping):
    """Weighted PageRank via power iteration (Spark-native, lineage-truncated)."""
    nodes = (edges.select(F.col("f").alias("id"))
             .union(edges.select(F.col("t").alias("id"))).distinct())
    n = nodes.count()
    outw = edges.groupBy("f").agg(F.sum("w").alias("outw"))
    e2 = edges.join(outw, "f").withColumn("frac", F.col("w") / F.col("outw")).select("f", "t", "frac").cache()
    pr = nodes.withColumn("pr", F.lit(1.0 / n))
    base = (1.0 - damping) / n
    for _ in range(iters):
        contrib = (e2.join(pr, e2.f == pr.id)
                   .withColumn("c", F.col("pr") * F.col("frac"))
                   .groupBy(F.col("t").alias("id")).agg(F.sum("c").alias("inc")))
        pr = (nodes.join(contrib, "id", "left")
              .withColumn("pr", F.lit(base) + damping * F.coalesce("inc", F.lit(0.0)))
              .select("id", "pr")).localCheckpoint(eager=True)
    return pr, n


def _path_len_km(cells):
    tot = 0.0
    for a, b in zip(cells[:-1], cells[1:]):
        tot += h3.point_dist(h3.h3_to_geo(a), h3.h3_to_geo(b), unit="km")
    return tot


def heavy_paths(fedges, min_prob):
    """
    Dominant-flow corridors: greedy walks seeded by the top frequent edges that
    extend only while the next transition carries >= `min_prob` of the current
    cell's flow. This follows the city's main flows and STOPS at forks (where the
    flow splits below min_prob) -> coherent corridors that actually validate,
    instead of over-extended 'Frankenstein' paths. Driver-side on frequent edges.
    """
    succ, pred = defaultdict(list), defaultdict(list)
    outw, inw = defaultdict(int), defaultdict(int)
    for f, t, w in fedges:
        succ[f].append((w, t)); pred[t].append((w, f))
        outw[f] += w; inw[t] += w
    for k in succ:
        succ[k].sort(reverse=True)
    for k in pred:
        pred[k].sort(reverse=True)

    seeds = sorted(fedges, key=lambda e: -e[2])[:config.GRAPH_MAX_SEEDS]
    paths = set()
    for f0, t0, _w in seeds:
        cells, visited = [f0, t0], {f0, t0}
        cur = t0                                    # extend forward on dominant flow
        while _path_len_km(cells) < MAX_L_CAP:
            nxt = next(((w, t) for w, t in succ[cur] if t not in visited), None)
            if not nxt or outw[cur] == 0 or nxt[0] / outw[cur] < min_prob:
                break
            cells.append(nxt[1]); visited.add(nxt[1]); cur = nxt[1]
        cur = f0                                    # extend backward on dominant inflow
        while _path_len_km(cells) < MAX_L_CAP:
            prv = next(((w, f) for w, f in pred[cur] if f not in visited), None)
            if not prv or inw[cur] == 0 or prv[0] / inw[cur] < min_prob:
                break
            cells.insert(0, prv[1]); visited.add(prv[1]); cur = prv[1]
        if len(cells) >= 2:
            paths.add(tuple(cells))
    return [list(p) for p in paths]


def main(use_sample: bool) -> None:
    spark = get_spark("route-mining-graph")
    t0 = time.time()
    suffix = "sample" if use_sample else "full"
    res = config.H3_RESOLUTION

    enc_path = config.CLEAN_PARQUET.replace(".parquet", f"_encoded_r{res}_{suffix}.parquet")
    trips = spark.read.parquet(enc_path).select("h3_seq_compact").cache()
    n_trips = trips.count()

    edges = build_edges(trips).cache()
    n_edges = edges.count()

    # ---- ACTIVITY ZONES: PageRank + weighted throughput ----
    pr, n_nodes = pagerank(edges, config.PAGERANK_ITERS, config.PAGERANK_DAMPING)
    throughput = (edges.groupBy(F.col("t").alias("id")).agg(F.sum("w").alias("in_w")))
    zones = (pr.join(throughput, "id", "left")
             .withColumn("in_w", F.coalesce("in_w", F.lit(0)))
             .orderBy(F.col("pr").desc()).limit(config.ACTIVITY_ZONES_TOP).collect())

    zones_dir = os.path.join(config.OUTPUT_BASE, "routes")
    os.makedirs(zones_dir, exist_ok=True)
    zpath = os.path.join(zones_dir, f"activity_zones_{suffix}.csv")
    with open(zpath, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["rank", "cell", "lat", "lon", "pagerank", "in_traffic"])
        for rank, z in enumerate(zones, 1):
            lat, lon = h3.h3_to_geo(z["id"])
            w.writerow([rank, z["id"], round(lat, 6), round(lon, 6),
                        round(z["pr"], 8), z["in_w"]])

    # ---- POPULAR ROUTES: greedy heavy paths, then validate against trips ----
    fedges = [(r["f"], r["t"], r["w"]) for r in
              edges.filter(F.col("w") >= config.GRAPH_MIN_EDGE_SUPPORT).collect()]
    cand = heavy_paths(fedges, config.GRAPH_MIN_FLOW_PROB)   # dominant-flow corridors
    cand_str = [DELIM + DELIM.join(c) + DELIM for c in cand]   # wrapped for containment
    cand_len = [_path_len_km(c) for c in cand]
    cand_ncells = [len(c) for c in cand]

    # validate ALL candidates in ONE Spark job via broadcast containment
    bc = spark.sparkContext.broadcast(cand_str)

    @F.udf(T.ArrayType(T.IntegerType()))
    def contained_ids(seq):
        s = DELIM + DELIM.join(seq) + DELIM
        return [i for i, c in enumerate(bc.value) if c in s]

    sup_rows = (trips.select(F.explode(contained_ids("h3_seq_compact")).alias("i"))
                .groupBy("i").agg(F.count(F.lit(1)).alias("sup")).collect())
    support = {r["i"]: r["sup"] for r in sup_rows}

    # assemble validated routes
    routes = [(support.get(i, 0), cand_len[i], cand_ncells[i], DELIM.join(cand[i]))
              for i in range(len(cand)) if support.get(i, 0) > 0]

    print("=" * 60)
    print(f"trips               : {n_trips:,}")
    print(f"graph nodes / edges : {n_nodes:,} / {n_edges:,}")
    print(f"frequent edges (>={config.GRAPH_MIN_EDGE_SUPPORT}) : {len(fedges):,}")
    print(f"heavy-path candidates / validated : {len(cand):,} / {len(routes):,}")
    print("=" * 60)

    rep = [f"# M10 Method C: Transition Graph ({suffix})",
           f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
           f"\ntrips: {n_trips:,} | nodes: {n_nodes:,} | edges: {n_edges:,} | "
           f"frequent edges: {len(fedges):,} | heavy paths (validated): {len(routes):,}\n",
           "## Popular routes (heavy paths validated vs trips) per length config",
           "| min_len_km | #validated(>=L) | top_support | longest_km |", "|---|---|---|---|"]

    all_rows = []
    for L in THRESHOLDS:
        c = sorted([r for r in routes if r[1] >= L], key=lambda r: (-r[0], -r[1]))[:TOP_K]
        top_sup = c[0][0] if c else 0
        longest = max((r[1] for r in c), default=0.0)
        rep.append(f"| {L} | {len(c):,} | {top_sup:,} | {longest:.2f} |")
        for rank, (sup, ln, nc, route) in enumerate(c, 1):
            all_rows.append((L, rank, sup, round(ln, 3), nc, route))

    rpath = os.path.join(zones_dir, f"graph_heavy_paths_top100_{suffix}.csv")
    with open(rpath, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["min_len_km", "rank", "support", "length_km", "n_cells", "route"])
        w.writerows(all_rows)

    rep += ["\n## Top activity zones (PageRank)", "| rank | cell | lat | lon | pagerank |",
            "|---|---|---|---|---|"]
    for rank, z in enumerate(zones[:10], 1):
        lat, lon = h3.h3_to_geo(z["id"])
        rep.append(f"| {rank} | {z['id']} | {lat:.5f} | {lon:.5f} | {z['pr']:.6f} |")

    rp = os.path.join(config.OUTPUT_BASE, "statistics", f"m10_graph_{suffix}.md")
    os.makedirs(os.path.dirname(rp), exist_ok=True)
    with open(rp, "w", encoding="utf-8") as fh:
        fh.write("\n".join(rep) + "\n")

    elapsed = time.time() - t0
    print("\n".join(rep))
    print(f"\n[m10] wrote heavy paths  -> {rpath}")
    print(f"[m10] wrote zones        -> {zpath}")
    print(f"[m10] wrote report       -> {rp}")
    print(f"[m10] wall time          : {elapsed:.1f}s")
    spark.stop()
    print("M10 GRAPH MINING COMPLETE.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
