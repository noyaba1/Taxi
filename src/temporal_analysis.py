"""
temporal_analysis.py  --  is "the popular route" the same at 08:00 and 03:00?
============================================================================
Every corridor and every activity zone this project reports is aggregated over
the whole 2013-2014 period. That is the obvious reading of the task, and it may
be the wrong one: rush-hour and 3 a.m. traffic are different phenomena, and an
average over both can describe neither.

The brief asks which areas are activity hotspots. If hotspots move with the
clock, a single static list is not an answer to that question — it is a summary
statistic that happens to look like one.

WHAT THIS MEASURES
------------------
Split trips into hour-of-day buckets (config.TIME_BUCKETS), mine each bucket
with the SAME suffix array the headline deliverable uses, then compare each
bucket's corridors against the all-time corridors by cell-set Jaccard.

  low overlap   the all-time top-100 is an average describing no actual hour,
                and the per-bucket lists below are the real answer.
  high overlap  the corridors are structural — the road network, not demand,
                decides where taxis go — which is itself worth stating, and
                makes the static deliverable defensible rather than lucky.

Support floors are scaled per bucket (a bucket holds a fraction of the trips, so
a fixed absolute floor would make quiet hours look empty for arithmetic reasons
rather than real ones).

Run:
    python -m src.temporal_analysis --mid
"""
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src import cli, config, storage
from src.route_mining_exact import DELIM
from src.route_mining_suffix_array import maximal_at, mine_encoded
from src.spark_session import get_spark

log = cli.setup_logging("m18")

MATCH_JACCARD = 0.5     # same rule the cross-method comparison uses
TOP_N = 100
REPORT_LEN_KM = 3.0     # the band where every method has results


def _bucket_column():
    """Hour-of-day -> bucket name, from the unix TIMESTAMP."""
    hour = F.hour(F.from_unixtime("TIMESTAMP"))
    col = F.lit(None).cast("string")
    for name, lo, hi in config.TIME_BUCKETS:
        col = F.when((hour >= lo) & (hour < hi), F.lit(name)).otherwise(col)
    return col


def _corridors(routes_df, min_sup, min_len_km, top_n):
    """Top maximal-frequent corridors as [(cellset, support, length_km)]."""
    rows = (maximal_at(routes_df, min_sup)
            .filter(F.col("length_km") >= min_len_km)
            .orderBy(F.col("support").desc(), F.col("length_km").desc())
            .limit(top_n).collect())
    return [(frozenset(r["subroute"].split(DELIM)), r["support"],
             r["length_km"], r["subroute"]) for r in rows]


def _overlap(xs, ys):
    """Fraction of xs with a Jaccard>=MATCH_JACCARD partner in ys."""
    if not xs or not ys:
        return float("nan")
    hit = 0
    for cx, *_ in xs:
        for cy, *_ in ys:
            inter = len(cx & cy)
            if inter and inter / len(cx | cy) >= MATCH_JACCARD:
                hit += 1
                break
    return hit / len(xs)


def main(scale: str, floor_pct: float) -> None:
    spark = get_spark("temporal-analysis")

    with cli.stage("m18_temporal", scale, log) as st:
        paths = config.dataset_paths(scale)
        enc = (spark.read.parquet(paths["encoded"])
               .select("TRIP_ID", "TAXI_ID", "TIMESTAMP", "h3_seq_compact")
               .withColumn("bucket", _bucket_column())).cache()
        n_all = enc.count()

        sizes = {r["bucket"]: r["n"] for r in
                 enc.groupBy("bucket").agg(F.count(F.lit(1)).alias("n")).collect()}
        log.info("trips per hour bucket: %s",
                 {k: f"{v:,}" for k, v in sorted(sizes.items())})

        # All-time reference, mined the same way so the comparison is fair.
        ref_floor = max(2, int(round(floor_pct / 100.0 * n_all)))
        _n, _s, ref_routes = mine_encoded(
            enc.select("TRIP_ID", "TAXI_ID", "h3_seq_compact"), ref_floor)
        ref_routes = ref_routes.cache()
        reference = _corridors(ref_routes, ref_floor, REPORT_LEN_KM, TOP_N)
        log.info("all-time reference: floor=%d -> %d corridors >=%.0f km",
                 ref_floor, len(reference), REPORT_LEN_KM)

        results, all_rows = [], []
        for name, _lo, _hi in config.TIME_BUCKETS:
            n_b = sizes.get(name, 0)
            if n_b == 0:
                continue
            t0 = time.time()
            sub = enc.filter(F.col("bucket") == name).select(
                "TRIP_ID", "TAXI_ID", "h3_seq_compact")
            # Floor scales with the bucket: a fixed absolute floor would make
            # quiet hours look empty for arithmetic reasons, not real ones.
            floor = max(2, int(round(floor_pct / 100.0 * n_b)))
            _nt, _ns, routes = mine_encoded(sub, floor)
            corridors = _corridors(routes.cache(), floor, REPORT_LEN_KM, TOP_N)
            ov = _overlap(corridors, reference)
            elapsed = time.time() - t0
            results.append((name, n_b, floor, len(corridors), ov, elapsed))
            log.info("%-13s n=%-9s floor=%-5d corridors=%-4d overlap_vs_alltime=%.2f",
                     name, f"{n_b:,}", floor, len(corridors), ov)
            for rank, (_c, sup, km, route) in enumerate(corridors, 1):
                all_rows.append((name, n_b, floor, rank, sup, round(km, 3), route))

        rep = [f"# Temporal analysis: does 'popular' depend on the hour? ({scale})",
               f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
               "",
               f"trips: {n_all:,} | corridors compared at >={REPORT_LEN_KM:.0f} km "
               f"| support floor {floor_pct}% of each bucket "
               f"| match = cell-set Jaccard >= {MATCH_JACCARD}",
               "",
               "Every headline number in this project aggregates 2013-2014 whole. "
               "This asks whether that average describes any actual hour.",
               "",
               "| bucket | trips | floor | corridors | overlap vs all-time | wall_s |",
               "|---|---|---|---|---|---|"]
        for name, n_b, floor, n_c, ov, el in results:
            rep.append(f"| {name} | {n_b:,} | {floor} | {n_c} | {ov:.2f} | {el:.1f} |")

        overlaps = [r[4] for r in results if r[4] == r[4]]      # drop NaN
        mean_ov = sum(overlaps) / len(overlaps) if overlaps else float("nan")
        rep += ["", "## Verdict", ""]
        if not overlaps:
            rep.append("No bucket produced corridors at this floor — nothing to compare.")
        elif mean_ov >= 0.8:
            rep.append(
                f"Mean overlap {mean_ov:.2f}. The corridors are **structural**: "
                f"the same stretches dominate at 03:00 and at 08:00, so it is the "
                f"road network rather than time-varying demand that decides where "
                f"taxis go. The all-time top-100 is therefore a fair summary and "
                f"not an artefact of averaging — which is worth having measured "
                f"rather than assumed.")
        elif mean_ov >= 0.5:
            rep.append(
                f"Mean overlap {mean_ov:.2f}. A substantial core is shared, but a "
                f"meaningful minority of each hour's corridors do not appear in the "
                f"all-time list. The static deliverable is a reasonable summary "
                f"with a real caveat: it under-represents whatever is specific to "
                f"peak and to night.")
        else:
            rep.append(
                f"Mean overlap only {mean_ov:.2f}. **The all-time top-100 is an "
                f"average that describes no actual hour.** Corridors popular at "
                f"08:00 are largely not the ones popular at 03:00, so reporting a "
                f"single static list answers a question nobody asked. The "
                f"per-bucket lists in the CSV are the real answer, and the "
                f"activity-hotspot question should be answered per bucket too.")

        lo = min(results, key=lambda r: r[4] if r[4] == r[4] else 2)
        hi = max(results, key=lambda r: r[4] if r[4] == r[4] else -1)
        if results:
            rep += ["",
                    f"Most distinctive hour: **{lo[0]}** (overlap {lo[4]:.2f}) — "
                    f"the period whose corridors the all-time list represents "
                    f"least well. Most typical: **{hi[0]}** ({hi[4]:.2f})."]

        csv_path = storage.write_csv(
            storage.out_path("routes", f"temporal_corridors_{scale}.csv"),
            ["bucket", "bucket_trips", "min_sup", "rank", "support", "length_km",
             "subroute"],
            all_rows)
        rp = storage.write_lines(
            storage.out_path("statistics", f"temporal_analysis_{scale}.md"), rep)

        log.info("\n%s", "\n".join(rep))
        log.info("wrote corridors -> %s", csv_path)
        log.info("wrote report    -> %s", rp)
        st.update(trips=n_all, buckets=len(results),
                  mean_overlap=round(mean_ov, 3) if overlaps else None)

    spark.stop()
    log.info("TEMPORAL ANALYSIS COMPLETE.")


if __name__ == "__main__":
    ap = cli.scale_parser(__doc__)
    ap.add_argument("--floor-pct", type=float, default=0.05,
                    help="support floor as %% of each bucket's trips")
    args = ap.parse_args()
    main(cli.scale_of(args), args.floor_pct)
