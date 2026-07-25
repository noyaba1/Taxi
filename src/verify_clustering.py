"""
verify_clustering.py  --  Method A verification
===============================================
Independently validates the clustering output. Method A now reports the
SUB-ROUTE a cluster's members share (not the seed's whole trajectory), so the
checks are about that run rather than about cluster membership:

  1. structural: length_km >= min_len; support in [1, n_trips]; route non-empty
  2. SUPPORT CORRECTNESS: brute-force trip-containment support == reported
     support. This is the check that matters most -- the mining computes support
     with an Aho-Corasick automaton, and here we recount by an entirely different
     route (a substring scan per trip), so agreement is real evidence.
  3. COHESION: the run really is shared by >= CLUSTER_SUBROUTE_PCT of its
     cluster's members (guards against a cluster reporting a run only a couple
     of its members contain).
  4. CONTINUITY: no hop inside a reported route exceeds the physical limit, i.e.
     no route is an artefact of a GPS gap.

Run:
    python -m src.verify_clustering --sample
"""
from pyspark.sql import functions as F

from src import cells as cells_mod
from src import cli, config, storage
from src.route_mining_exact import DELIM
from src.spark_session import get_spark


def main(scale: str) -> None:
    spark = get_spark("verify-clustering")

    trips = (spark.read.parquet(config.dataset_paths(scale)["encoded"])
             .select("h3_seq_compact")
             .withColumn("trip_str",
                         F.concat(F.lit(DELIM),
                                  F.concat_ws(DELIM, "h3_seq_compact"),
                                  F.lit(DELIM)))
             .select("trip_str"))
    trips.cache()
    n_trips = trips.count()

    rows = storage.read_csv_rows(
        storage.out_path("routes", f"clustering_top100_{scale}.csv"))
    if not rows:
        print("no clustering output to verify (stage produced no routes)")
        spark.stop()
        return

    pct = config.CLUSTER_SUBROUTE_PCT
    limit = config.max_cell_hop_km()
    print(f"Spark {spark.version} | trips={n_trips:,} | rows={len(rows)} | "
          f"cluster_subroute_pct={pct}")
    ok = True

    # --- 1. structural ---
    bad = [r for r in rows
           if float(r["length_km"]) < float(r["min_len_km"])
           or not (1 <= int(r["support"]) <= n_trips)
           or not r["subroute"]]
    c1 = not bad
    ok &= c1
    print(f"[{'OK' if c1 else 'FAIL'}] structural (len>=min_len, support in range, "
          f"route non-empty): {len(bad)} bad")

    # --- 2. support re-counted by brute-force containment ---
    print("\n=== support re-count (brute-force containment) ===")
    seen, targets = set(), []
    for r in rows:
        if r["subroute"] not in seen:
            seen.add(r["subroute"])
            targets.append(r)
        if len(targets) >= 5:
            break
    c2 = True
    for r in targets:
        needle = DELIM + r["subroute"] + DELIM
        brute = trips.filter(F.instr("trip_str", needle) > 0).count()
        match = brute == int(r["support"])
        c2 &= match
        print(f"[{'OK' if match else 'FAIL'}] L>={r['min_len_km']}km: "
              f"reported={r['support']} brute={brute} ({r['n_cells']} cells, "
              f"{float(r['length_km']):.2f} km)")
    ok &= c2

    # --- 3. cohesion: the run is shared by enough of its cluster ---
    weak = [r for r in rows
            if int(r["members_with_run"]) < max(2, pct * int(r["cluster_size"]))]
    c3 = not weak
    ok &= c3
    print(f"\n[{'OK' if c3 else 'FAIL'}] cohesion: every reported run is in "
          f">={pct:.0%} of its cluster's members ({len(weak)} violations)")

    # --- 4. continuity: no reported route spans a GPS gap ---
    broken = []
    for r in rows:
        hops = cells_mod.hops_km(r["subroute"].split(DELIM))
        if hops and max(hops) > limit:
            broken.append((r["subroute"][:30], max(hops)))
    c4 = not broken
    ok &= c4
    print(f"[{'OK' if c4 else 'FAIL'}] continuity: no route contains a hop > "
          f"{limit:.2f} km ({len(broken)} violations)")
    for route, hop in broken[:3]:
        print(f"     {route}... max hop {hop:.2f} km")

    print("\n" + ("CLUSTERING VERIFICATION PASSED." if ok
                  else "CLUSTERING VERIFICATION FAILED."))
    spark.stop()
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
