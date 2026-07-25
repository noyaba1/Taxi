"""
route_mining_closed.py  --  PHASE 5 / Milestone M6  (CLOSED sub-routes)
======================================================================
Keeps only CLOSED (maximal) frequent sub-routes from the M5 n-gram support
table, so we don't report dozens of overlapping fragments of one corridor.

WHAT THIS IS, HONESTLY
----------------------
This computes the closed-frequent-substring SET, which is the same set a suffix
tree's branching nodes would give you -- but it does so with DataFrame joins over
the exhaustive n-gram table, NOT with a suffix structure. It builds no suffix
tree, no suffix array, and gains none of their complexity properties; it shares
M5's support table, so it is best understood as a post-filter on M5 rather than
an independent algorithm.

The assignment's "Suffix Tree / Suffix Array" requirement is met by
`route_mining_suffix_array.py`, which builds a real generalised suffix array with
an LCP array and enumerates LCP intervals. This module stays as the cheap
baseline that method is compared against.

THE FILTER
----------
Support is monotonic under extension, so the best support any super-route can
reach equals the best SINGLE-CELL extension's support. Hence:

    s is closed  <=>  max(best_left_ext, best_right_ext) < support(s)

computed via:
  * right-parent(t) = t without its LAST  cell   (t extends its right-parent)
  * left-parent(t)  = t without its FIRST cell   (t extends its left-parent)
  * group by parent, take max child support = best single-cell extension support.

Routes flagged `truncated` by M5 (they hit the length cap, so their extension was
never enumerated) are EXCLUDED: absence of a recorded extension is not evidence
of maximality.

Run:
    python -m src.route_mining_closed --sample
"""
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src import cli, config, storage
# Reuse the EXACT M5 window emitter so the support table is identical.
from src.route_mining_exact import DELIM, THRESHOLDS_KM, TOP_N, build_support_table
from src.spark_session import get_spark

SUPPORT_TOL = 0.0   # 0.0 = classical closed; raise (e.g. 0.1) to merge near-equal

log = cli.setup_logging("m6")


def load_support_table(spark, scale):
    """(n_trips, agg) for a scale. Kept here so M8 and the verifiers share it."""
    paths = config.dataset_paths(scale)
    enc = spark.read.parquet(paths["encoded"]).select("TRIP_ID", "h3_seq_compact")
    return enc.count(), build_support_table(enc)


def with_parents(agg):
    """Attach right_parent (drop last cell) and left_parent (drop first cell)."""
    cells = F.split(F.col("subroute"), DELIM)
    return (agg
            .withColumn("right_parent",
                        F.array_join(F.slice(cells, F.lit(1), F.col("n_cells") - 1), DELIM))
            .withColumn("left_parent",
                        F.array_join(F.slice(cells, F.lit(2), F.col("n_cells") - 1), DELIM)))


def best_extensions(agg):
    """
    (subroute -> best single-cell extension support), joined onto `agg`.
    Shared by M6 (closed) and M8 (maximal-frequent) so the two agree by
    construction rather than by coincidence.
    """
    wp = with_parents(agg)
    best_right = wp.groupBy("right_parent").agg(F.max("support").alias("r_ext"))
    best_left = wp.groupBy("left_parent").agg(F.max("support").alias("l_ext"))
    joined = (agg
              .join(best_right, agg.subroute == best_right.right_parent, "left")
              .drop("right_parent")
              .join(best_left, agg.subroute == best_left.left_parent, "left")
              .drop("left_parent"))
    return joined.withColumn(
        "max_ext_support",
        F.greatest(F.coalesce(F.col("r_ext"), F.lit(0)),
                   F.coalesce(F.col("l_ext"), F.lit(0))))


def keep_maximal(agg):
    """Filter `agg` to closed/maximal sub-routes via single-cell extension check."""
    return (best_extensions(agg)
            # A truncated window's extension was never enumerated, so a missing
            # extension proves nothing. Drop rather than wrongly promote.
            .filter(~F.coalesce(F.col("truncated"), F.lit(False)))
            .filter(F.col("max_ext_support") < F.col("support") * (1.0 - SUPPORT_TOL))
            .select("subroute", "support", "length_km", "n_cells", "max_ext_support"))


def main(scale: str) -> None:
    spark = get_spark("route-mining-suffix")

    with cli.stage("m6_closed", scale, log) as st:
        t0 = time.time()
        n_trips, agg = load_support_table(spark, scale)
        agg.cache()
        n_all = agg.count()

        maximal = keep_maximal(agg)
        maximal.cache()
        n_max = maximal.count()

        log.info("=" * 60)
        log.info("trips                    : %s", f"{n_trips:,}")
        log.info("all sub-routes (M5)      : %s", f"{n_all:,}")
        log.info("closed sub-routes (M6)   : %s  (%.1f%% kept, %s redundant dropped)",
                 f"{n_max:,}", 100 * n_max / max(n_all, 1), f"{n_all - n_max:,}")
        log.info("=" * 60)

        rep = [f"# M6 Closed Sub-route Mining ({scale})",
               f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
               f"\ntrips: {n_trips:,} | SUPPORT_TOL={SUPPORT_TOL}",
               f"all sub-routes (M5): {n_all:,} | closed (M6): {n_max:,} "
               f"({100 * n_max / max(n_all, 1):.1f}% kept)\n",
               "| min_len_km | M5 candidates | M6 closed | reduction | top_support |",
               "|---|---|---|---|---|"]

        all_rows = []
        for L in THRESHOLDS_KM:
            m5_cand = agg.filter(F.col("length_km") >= L).count()
            cand = maximal.filter(F.col("length_km") >= L)
            m6_cand = cand.count()
            top = (cand.orderBy(F.col("support").desc(), F.col("length_km").desc())
                   .limit(TOP_N).collect())
            top_support = top[0]["support"] if top else 0
            reduction = f"{100 * (1 - m6_cand / m5_cand):.1f}%" if m5_cand else "n/a"
            rep.append(f"| {L} | {m5_cand:,} | {m6_cand:,} | {reduction} | {top_support:,} |")
            for rank, r in enumerate(top, 1):
                all_rows.append((L, rank, r["support"], round(r["length_km"], 3),
                                 r["n_cells"], r["subroute"]))

        csv_path = storage.write_csv(
            storage.out_path("routes", f"closed_top100_{scale}.csv"),
            ["min_len_km", "rank", "support", "length_km", "n_cells", "subroute"],
            all_rows)
        rp = storage.write_lines(
            storage.out_path("statistics", f"m6_suffix_mining_{scale}.md"), rep)

        log.info("\n%s", "\n".join(rep))
        log.info("wrote closed routes -> %s", csv_path)
        log.info("wrote report        -> %s", rp)
        st.update(trips=n_trips, distinct_keys=n_all, closed_routes=n_max,
                  mining_s=round(time.time() - t0, 1))

    spark.stop()
    log.info("M6 CLOSED MINING COMPLETE.")


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
