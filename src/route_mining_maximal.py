"""
route_mining_maximal.py  --  PHASE 5 / Milestone M8  (PDF-aligned definition)
============================================================================
"Popular long sub-route" exactly as the assignment defines it:

    keep a CONTIGUOUS sub-route iff >= X% of trips traversed it (frequent),
    and it is MAXIMAL: no one-cell extension is still frequent.

Because support is monotonic under extension, "maximal among frequent" == the
LONGEST contiguous stretch that still clears X% -> this is the assignment's
"maximise the sub-route length subject to >= X% of trips".

X IS CALIBRATED PER LENGTH CONFIGURATION
----------------------------------------
The assignment asks for the top 100 at >=1, 3, 5, 10, 20 and 40 km. A single
global X cannot serve all six. Maximal-frequent routes are ALREADY the longest
stretches clearing X, so filtering them at 40 km does not "find 40 km routes" --
it asks whether the one chosen X happened to produce any. At X=0.5% on 1.71M
trips a 40 km corridor would need ~8,500 distinct trips over the same unbroken
stretch, so that configuration comes back empty and the deliverable is missing.

So for each L we take the LARGEST X from a descending grid whose maximal-frequent
set still yields TOP_K routes of length >= L. That answers "maximise length
subject to >= X%" in the direction it was posed, reports the X actually used for
each configuration, and fills all six length configs. One pass over the grid
serves every threshold, so this is cheaper than the old fixed-X sweep too.

THE "HOLES": a maximal-frequent sub-route TERMINATES exactly where traffic forks
(each continuation drops below X%). So a popular corridor appears as a COLLECTION
of contiguous sub-routes with holes at the divergence points. We report those
forks explicitly (holes analysis).

Run:
    python -m src.route_mining_maximal --sample
"""
import math
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src import cli, config, storage
from src.route_mining_exact import DELIM
from src.route_mining_closed import best_extensions, load_support_table, with_parents
from src.spark_session import get_spark

THRESHOLDS = config.ROUTE_LENGTH_THRESHOLDS_KM
TOP_K = config.TOP_K

log = cli.setup_logging("m8")


def keep_maximal_frequent(agg, min_sup: int):
    """
    Maximal-frequent contiguous sub-routes at absolute support floor `min_sup`.
    s kept  <=>  support(s) >= min_sup  AND  best single-cell extension < min_sup.

    Truncated windows (they hit the length cap, so no extension was enumerated)
    are excluded: a missing extension is not evidence that none exists.
    """
    return (best_extensions(agg)
            .filter(~F.coalesce(F.col("truncated"), F.lit(False)))
            .filter((F.col("support") >= min_sup) & (F.col("max_ext_support") < min_sup))
            .select("subroute", "support", "length_km", "n_cells", "max_ext_support"))


def min_support_for(x_pct: float, n_trips: int) -> int:
    """X% of trips, floored at 2 (a 'route' one trip drove is not popular)."""
    return max(2, math.ceil(x_pct / 100.0 * n_trips))


def calibrate_per_threshold(agg, n_trips, thresholds, floors, top_k):
    """
    For each length threshold L, the TIGHTEST absolute support floor that still
    yields `top_k` routes of length >= L.

    Absolute, not percentage, for the same reason as Method D
    (route_mining_suffix_array.calibrate): X=0.01% is 2 trips on the sample but
    171 at 1.71M, so a percentage grid gets harder to clear as the data grows and
    the long length bands empty out. Measured: switching to absolute floors turned
    the >=20 km band from empty into a 21.36 km corridor at mid scale.

    B and D must use the SAME instrument or their outputs stop being comparable,
    and the 0-disagreement invariant between them becomes meaningless.

    Returns (chosen, per_floor) with
      chosen[L]     = (min_sup, x_pct, DataFrame)
      per_floor[ms] = (x_pct, n_routes, longest_km)
    """
    chosen, per_floor, loosest = {}, {}, None
    for ms in sorted(floors, reverse=True):
        if ms > n_trips:
            continue
        maximal = keep_maximal_frequent(agg, ms).cache()
        stats = maximal.agg(F.count(F.lit(1)).alias("n"),
                            F.max("length_km").alias("mx")).collect()[0]
        x_pct = config.pct_of(ms, n_trips)
        per_floor[ms] = (x_pct, stats["n"], stats["mx"] or 0.0)
        loosest = (ms, x_pct, maximal)
        log.info("  min_sup=%-6d (X=%.4f%%)  maximal=%-9s longest=%.2f km",
                 ms, x_pct, f"{stats['n']:,}", stats["mx"] or 0.0)
        for L in thresholds:
            if L not in chosen and \
                    maximal.filter(F.col("length_km") >= L).count() >= top_k:
                chosen[L] = (ms, x_pct, maximal)

    if loosest is None:
        return {}, {}
    for L in thresholds:
        chosen.setdefault(L, loosest)
    return chosen, per_floor


def main(scale: str) -> None:
    spark = get_spark("route-mining-maximal")

    with cli.stage("m8_maximal", scale, log) as st:
        t0 = time.time()
        n_trips, agg = load_support_table(spark, scale)
        agg.cache()
        n_all = agg.count()

        log.info("=" * 62)
        log.info("trips          : %s", f"{n_trips:,}")
        log.info("all sub-routes : %s", f"{n_all:,}")
        log.info("calibrating the support floor per length threshold over %s",
                 config.SUPPORT_MIN_SUP_GRID)
        chosen, per_floor = calibrate_per_threshold(
            agg, n_trips, THRESHOLDS, config.SUPPORT_MIN_SUP_GRID, TOP_K)
        log.info("=" * 62)

        rep = [f"# M8 Min-Support Maximal Sub-routes ({scale})",
               f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
               f"\ntrips: {n_trips:,} | all sub-routes: {n_all:,}\n",
               "## Top maximal-frequent routes per length configuration",
               "",
               "The support floor is calibrated per configuration: the TIGHTEST",
               f"floor that still yields {TOP_K} routes at that minimum length. It is",
               "absolute (a trip count) rather than a percentage, because a",
               "percentage floor gets harder to clear as the dataset grows and",
               "empties the long bands. The equivalent X% at this scale is shown.",
               "",
               "| min_len_km | min_sup | = X% | #maximal(>=L) | top_support | longest_km |",
               "|---|---|---|---|---|---|"]

        all_rows, empty = [], []
        for L in THRESHOLDS:
            min_sup, x, maximal = chosen[L]
            cand = maximal.filter(F.col("length_km") >= L)
            n_at_l = cand.count()
            top = (cand.orderBy(F.col("support").desc(), F.col("length_km").desc())
                   .limit(TOP_K).collect())
            top_support = top[0]["support"] if top else 0
            longest = max((r["length_km"] for r in top), default=0.0)
            rep.append(f"| {L} | {min_sup:,} | {x:.4f}% | {n_at_l:,} "
                       f"| {top_support:,} | {longest:.2f} |")
            if not top:
                empty.append(L)
            for rank, r in enumerate(top, 1):
                all_rows.append((L, min_sup, round(x, 5), rank, r["support"],
                                 round(r["length_km"], 3), r["n_cells"],
                                 r["subroute"]))

        if empty:
            longest_any = max((v[2] for v in per_floor.values()), default=0.0)
            rep += ["",
                    f"> **Empty configurations: {', '.join(f'>={L} km' for L in empty)}.** "
                    f"Even at the loosest floor (2 trips) the longest contiguous "
                    f"stretch shared by more than one trip is {longest_any:.1f} km. "
                    f"With {n_trips:,} trips no {min(empty)} km corridor is driven "
                    f"twice, so there is nothing popular to report at that length -- "
                    f"this is a property of the sample size, not a filter. The "
                    f"M5 baseline still lists single-trip routes at these lengths "
                    f"for contrast."]

        csv_path = storage.write_csv(
            storage.out_path("routes", f"maximal_frequent_top100_{scale}.csv"),
            ["min_len_km", "min_sup", "x_pct", "rank", "support", "length_km",
             "n_cells", "subroute"],
            all_rows)

        # ---- X sweep (what the whole grid looked like) ----
        rep += ["", "## Support-floor sweep (length vs strictness)",
                "| min_sup | = X% | #maximal-frequent | longest_km |",
                "|---|---|---|---|"]
        for ms in sorted(per_floor, reverse=True):
            x_pct, n_routes, longest = per_floor[ms]
            rep.append(f"| {ms:,} | {x_pct:.4f}% | {n_routes:,} | {longest:.2f} |")

        # ---- HOLES analysis at the reference X ----
        rep += _holes_section(agg, n_trips)

        rp = storage.write_lines(
            storage.out_path("statistics", f"m8_maximal_mining_{scale}.md"), rep)

        log.info("\n%s", "\n".join(rep))
        log.info("wrote maximal-frequent routes -> %s", csv_path)
        log.info("wrote report                  -> %s", rp)
        st.update(trips=n_trips, distinct_keys=n_all,
                  floor_per_threshold={str(L): chosen[L][0] for L in THRESHOLDS},
                  mining_s=round(time.time() - t0, 1))

    spark.stop()
    log.info("M8 MAXIMAL-FREQUENT MINING COMPLETE.")


def _holes_section(agg, n_trips):
    """
    Why maximal routes terminate: show the forks. Each continuation of a maximal
    route is below min_sup by construction -- that gap is the assignment's "hole"
    between two popular sub-routes of the same corridor.
    """
    min_sup = min_support_for(config.SUPPORT_X_PCT, n_trips)
    maximal = keep_maximal_frequent(agg, min_sup)
    top_max = [r["subroute"] for r in
               maximal.orderBy(F.col("support").desc()).limit(5).collect()]
    if not top_max:
        return ["", "## Holes analysis", "",
                f"No maximal-frequent route at the reference X="
                f"{config.SUPPORT_X_PCT}% (min_sup={min_sup})."]

    children = (with_parents(agg)
                .select(F.col("right_parent").alias("parent"),
                        F.col("subroute").alias("child"),
                        F.col("support").alias("child_support")))
    forks = {p: [] for p in top_max}
    for r in children.filter(F.col("parent").isin(top_max)).collect():
        forks[r["parent"]].append((r["child_support"], r["child"]))
    sup_of = {r["subroute"]: r["support"]
              for r in maximal.filter(F.col("subroute").isin(top_max)).collect()}

    out = ["", "## Holes analysis (why maximal routes terminate = traffic forks)",
           "",
           f"Reference X={config.SUPPORT_X_PCT}% -> min_sup={min_sup} trips. Each "
           f"continuation below that floor is a HOLE: the corridor continues, but "
           f"traffic splits and no single next cell carries {min_sup} trips.",
           "",
           "| route support | cells | best continuations |", "|---|---|---|"]
    for p in top_max:
        branches = sorted(forks.get(p, []), reverse=True)[:3]
        tail = ", ".join(str(s) for s, _ in branches) or "(none)"
        out.append(f"| {sup_of.get(p)} | {p.count(DELIM) + 1} | {tail} |")
    return out


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
