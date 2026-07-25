"""
validate_holdout.py  --  do the mined corridors generalise to unseen trips?
==========================================================================
Every correctness check in this project so far is INTERNAL: verifiers recount
support against the same encoded table the mining used, and cross-method
agreement compares methods that all read that table. Those checks catch
implementation bugs. None of them can catch the more embarrassing failure —
corridors that are an artefact of the training trips and describe nothing about
how taxis actually move in Porto.

The dataset ships a held-out set that has never entered the pipeline:

    Porto_taxi_data_test_partial_trajectories.csv   (320 trips, partial paths)

It is the original Kaggle challenge's test split, so it is disjoint from
train.csv and was collected the same way. That makes it a free, genuinely
independent check: encode it with the SAME grid, then ask what fraction of
held-out trips traverse a corridor we mined from the training data.

WHAT THE NUMBER MEANS
---------------------
  high coverage  the corridors describe real, recurring movement — a trip the
                 miner never saw still drives through them.
  low coverage   the corridors are memorised training paths. Support counted on
                 the data you fit is not evidence; this is.

A NULL MODEL IS INCLUDED, because "62% of trips hit a corridor" means nothing on
its own — corridors sit on busy roads, and so do most trips. We compare against
random cell-runs of the same length distribution drawn from the same city. The
LIFT over that null is the actual result.

Run:
    python -m src.validate_holdout --sample     # corridors mined from 5k
    python -m src.validate_holdout --mid
"""
import random
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src import ahocorasick
from src import cells as cells_mod
from src import cli, config, storage
from src.load_data import load_raw
from src.route_mining_exact import DELIM
from src.spark_session import get_spark
from src.spatial_encoding import encode

log = cli.setup_logging("holdout")

# Methods whose mined corridors we test, and the column holding the route.
SOURCES = [
    ("A", "clustering_top100", "subroute"),
    ("C", "graph_heavy_paths_top100", "route"),
    ("D", "suffix_array_top100", "subroute"),
    ("B", "maximal_frequent_top100", "subroute"),
]
NULL_SAMPLES = 200          # random corridors per method for the null model


def _load_holdout(spark):
    """Encode the held-out partial trajectories with the SAME grid as training."""
    raw = load_raw(spark, config.RAW_TEST)
    df = raw.withColumn(
        "points", F.from_json("POLYLINE", "array<array<double>>"))
    df = df.filter(F.size("points") >= config.MIN_POINTS)
    # `encode` needs the same columns the training path gives it.
    df = df.withColumn("total_distance_km", F.lit(1.0))
    enc = encode(df, config.H3_RESOLUTION).select("TRIP_ID", "h3_seq_compact")
    return enc


def _corridors(scale, key, stem, col, min_len_km):
    """Distinct mined corridors for one method at >= min_len_km."""
    rows = storage.read_csv_rows(storage.out_path("routes", f"{stem}_{scale}.csv"))
    out, seen = [], set()
    for r in rows:
        if float(r.get("length_km", 0)) < min_len_km:
            continue
        route = r[col]
        if route and route not in seen:
            seen.add(route)
            out.append(tuple(route.split(DELIM)))
    return out


def _null_corridors(city_cells, lengths, rng):
    """
    Random walks over the observed city graph, matched to the real corridors'
    length distribution.

    Not uniform-random cells: those would be disconnected nonsense that no trip
    could ever contain, making the null trivially zero and the lift meaningless.
    We walk the actual observed adjacency, so a null corridor is a *plausible*
    route that simply was not mined as popular.
    """
    out = []
    nodes = list(city_cells)
    if not nodes:
        return out
    for n in lengths:
        cur = rng.choice(nodes)
        walk = [cur]
        for _ in range(n - 1):
            nbrs = city_cells.get(cur)
            if not nbrs:
                break
            cur = rng.choice(list(nbrs))
            walk.append(cur)
        if len(walk) >= 2:
            out.append(tuple(walk))
    return out


def main(scale: str, min_len_km: float) -> None:
    spark = get_spark("validate-holdout")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    with cli.stage("holdout_validation", scale, log) as st:
        # This is a validation stage, not a deliverable. If the held-out file is
        # absent (e.g. it was not uploaded to the bucket) say so and exit clean --
        # killing a cloud run after the deliverables are computed would be a
        # spectacularly bad trade.
        try:
            enc = _load_holdout(spark).cache()
            n_holdout = enc.count()
        except Exception as exc:  # noqa: BLE001
            log.warning("cannot read held-out set %s (%s: %s); skipping "
                        "generalisation check", config.RAW_TEST,
                        type(exc).__name__, str(exc)[:160])
            spark.stop()
            return
        if n_holdout == 0:
            log.warning("held-out set %s is empty; skipping", config.RAW_TEST)
            spark.stop()
            return
        log.info("held-out trips encoded: %s (from %s)",
                 f"{n_holdout:,}", config.RAW_TEST)

        # Trip token sequences, split at gaps exactly as the miners do.
        seqs = [s for row in enc.select("h3_seq_compact").collect()
                for s in cells_mod.split_at_gaps(list(row[0]))]
        log.info("gap-free held-out segments: %s", f"{len(seqs):,}")

        # Observed adjacency of the held-out city, for the null model.
        adjacency: dict = {}
        for s in seqs:
            for a, b in zip(s[:-1], s[1:]):
                adjacency.setdefault(a, set()).add(b)

        rng = random.Random(42)
        rows, notes = [], []
        for key, stem, col in SOURCES:
            corridors = _corridors(scale, key, stem, col, min_len_km)
            if not corridors:
                notes.append(f"- Method {key}: no corridors at >={min_len_km:g} km "
                             f"for scale={scale} (stage not run, or none found).")
                continue

            auto = ahocorasick.Automaton(corridors)
            hit = sum(1 for s in seqs if auto.matches(s))
            cov = hit / len(seqs)

            lengths = [len(c) for c in corridors][:NULL_SAMPLES]
            null = _null_corridors(adjacency, lengths, rng)
            null_hit = 0
            if null:
                nauto = ahocorasick.Automaton(null)
                null_hit = sum(1 for s in seqs if nauto.matches(s))
            null_cov = null_hit / len(seqs) if null else 0.0
            lift = (cov / null_cov) if null_cov else float("inf")

            rows.append((key, len(corridors), cov, null_cov, lift))
            log.info("Method %s: %s corridors -> %.1f%% held-out coverage "
                     "(null %.1f%%, lift %.1fx)",
                     key, f"{len(corridors):,}", 100 * cov, 100 * null_cov, lift)

        rep = [f"# Held-out validation ({scale})",
               f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
               "",
               f"Corridors mined from the TRAINING data, tested against "
               f"**{n_holdout:,} trips the pipeline has never seen** "
               f"(`{config.RAW_TEST.split('/')[-1]}` — the original challenge's "
               f"held-out split), encoded with the same H3 grid and split at gaps "
               f"the same way ({len(seqs):,} gap-free segments).",
               "",
               "Every other check in this project is internal — verifiers recount "
               "support against the same table the mining used. This is the only "
               "one that can tell the difference between *a real corridor* and *a "
               "memorised training path*.",
               "",
               f"Corridors considered: length >= {min_len_km:g} km.",
               "",
               "| method | corridors | held-out coverage | null model | lift |",
               "|---|---|---|---|---|"]
        for key, n, cov, null_cov, lift in rows:
            lift_s = "inf" if lift == float("inf") else f"{lift:.1f}x"
            rep.append(f"| {key} | {n:,} | {cov:.1%} | {null_cov:.1%} | **{lift_s}** |")
        rep += notes

        rep += ["",
                "**Null model.** Coverage alone proves little: corridors sit on "
                "busy roads and so do most trips. The null is a set of random "
                "walks over the held-out city's OWN observed adjacency, matched to "
                "the real corridors' length distribution — i.e. plausible routes "
                "that simply were not mined as popular. Uniform-random cells would "
                "be disconnected, unmatchable, and would inflate the lift into "
                "meaninglessness.",
                "",
                "**Reading it.** Lift > 1 means mined corridors are traversed by "
                "unseen trips more often than comparable un-mined paths — the "
                "corridors generalise. Lift near 1 would mean the miner found busy "
                "geography in general rather than specific popular routes."]

        rp = storage.write_lines(
            storage.out_path("statistics", f"holdout_validation_{scale}.md"), rep)
        log.info("\n%s", "\n".join(rep))
        log.info("wrote -> %s", rp)
        st.update(holdout_trips=n_holdout, segments=len(seqs),
                  methods_tested=len(rows))

    spark.stop()
    log.info("HELD-OUT VALIDATION COMPLETE.")


if __name__ == "__main__":
    ap = cli.scale_parser(__doc__)
    ap.add_argument("--min-len-km", type=float, default=1.0,
                    help="only test corridors at least this long")
    args = ap.parse_args()
    main(cli.scale_of(args), args.min_len_km)
