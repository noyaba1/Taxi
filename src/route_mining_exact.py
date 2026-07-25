"""
route_mining_exact.py  --  PHASE 5 / Milestone M5  (EXACT baseline)
==================================================================
Exact frequent contiguous sub-route mining over the H3-encoded trips.

MODEL (see the M5 spec):
  * sub-route      = a CONTIGUOUS window cells[i..j] of `h3_seq_compact`
  * length         = Haversine sum between consecutive compact cell centres
  * support        = number of DISTINCT trips containing the sub-route
                     (deduped within a trip, so repeats inside one trip count once)
  * output         = top-100 by support for min-lengths {1,3,5,10,20,40} km

WHY CONTIGUOUS n-grams (and NOT PrefixSpan / gapped sequential patterns):
  a physical route is a continuous path; a *gap* in a sub-sequence means the taxi
  teleported between non-adjacent cells, which is not a real route. Gapped mining
  (PrefixSpan) both allows those invalid patterns and solves a strictly harder,
  more expensive problem than we need. Contiguous substring counting is the
  correct AND cheaper model. See docs/DESIGN_REVIEW.md section 5.

TWO CORRECTNESS GUARDS
----------------------
1. HOP GUARD. Length is measured between cell centres, so a window spanning a
   GPS gap measures whatever that gap was -- a 2-cell "sub-route" could report
   40 km. Trajectories are split at any consecutive-cell hop exceeding
   `config.max_cell_hop_km()` -- derived from the retained-speed limit plus cell
   quantisation, so it sits above real driving and below any GPS teleport.
2. TRUNCATION FLAG. Windows are capped at `config.MAX_SUBROUTE_KM`. A window
   sitting AT the cap has no recorded extension, so a naive maximality test would
   promote it to "maximal". Such windows are marked `truncated` and the maximal
   miners exclude them rather than trusting a missing extension.

SCALE. This is the EXACT baseline and it is quadratic in the cell count per trip.
It exists to be a ground truth for the sketches (M7) and the suffix array (M12),
so it takes `--max-trips` and refuses to silently attempt a run it cannot finish.

Run:
    python -m src.route_mining_exact --sample
"""
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F, types as T

from src import cells as cells_mod
from src import cli, config, storage
from src.spark_session import get_spark

THRESHOLDS_KM = config.ROUTE_LENGTH_THRESHOLDS_KM   # single source of truth
MIN_L = float(min(THRESHOLDS_KM))       # smallest threshold -> emit windows >= this
MAX_L_CAP = config.MAX_SUBROUTE_KM      # stop extending a window past this
TOP_N = config.TOP_K
DELIM = ">"                             # cell separator inside a sub-route key

log = cli.setup_logging("m5")

# UDF returns an array of (subroute_key, length_km, n_cells, truncated) per trip.
_SUBROUTE_SCHEMA = T.ArrayType(T.StructType([
    T.StructField("subroute", T.StringType()),
    T.StructField("length_km", T.DoubleType()),
    T.StructField("n_cells", T.IntegerType()),
    T.StructField("truncated", T.BooleanType()),
]))


def _subroutes(cells):
    """
    All CONTIGUOUS windows of `cells` with MIN_L <= length <= MAX_L_CAP, deduped
    within the trip (support counts trips, not occurrences).

    The trajectory is first split at GPS gaps, so no window can span a hole in
    the trace and report the hole's width as route length.

    Returns list of (subroute_key, length_km, n_cells, truncated), where
    `truncated` means the window stopped because it reached MAX_L_CAP -- so its
    one-cell extension was never enumerated and its maximality is unknown.
    """
    seen = set()
    out = []
    for seg in cells_mod.split_at_gaps(cells):
        n = len(seg)
        cum = cells_mod.cumulative_km(seg)   # O(1) window length via differences
        for i in range(n):
            for j in range(i + 1, n):
                length = cum[j] - cum[i]
                if length > MAX_L_CAP:       # windows only get longer -> stop here
                    break
                if length >= MIN_L:
                    key = DELIM.join(seg[i:j + 1])
                    if key not in seen:      # dedupe within trip
                        seen.add(key)
                        # Would the next cell exist but push us past the cap? Then
                        # this window's extension is unenumerated -> maximality unknown.
                        hit_cap = (j + 1 < n) and (cum[j + 1] - cum[i] > MAX_L_CAP)
                        out.append((key, float(length), j - i + 1, bool(hit_cap)))
    return out


subroutes_udf = F.udf(_subroutes, _SUBROUTE_SCHEMA)


def emit_windows(enc):
    """trips -> one row per (trip, sub-route). The shuffle input for the groupBy."""
    return enc.select(F.explode(subroutes_udf("h3_seq_compact")).alias("w")).select("w.*")


def build_support_table(enc):
    """
    (subroute -> distinct-trip support). Shared by M5/M6/M8 so every "method B"
    variant counts support identically.

    `truncated` is OR-ed across occurrences: if ANY trip's window hit the length
    cap, the route's extension is unknown and maximality cannot be claimed.
    """
    return (emit_windows(enc).groupBy("subroute")
            .agg(F.count(F.lit(1)).alias("support"),
                 F.first("length_km").alias("length_km"),
                 F.first("n_cells").alias("n_cells"),
                 F.max(F.col("truncated").cast("int")).cast("boolean").alias("truncated")))


def main(scale: str, max_trips: int | None) -> None:
    spark = get_spark("route-mining-exact")
    paths = config.dataset_paths(scale)

    with cli.stage("m5_exact", scale, log) as st:
        t0 = time.time()
        enc = spark.read.parquet(paths["encoded"]).select("TRIP_ID", "h3_seq_compact")
        n_trips = enc.count()

        if max_trips and n_trips > max_trips:
            raise SystemExit(
                f"REFUSING to run the O(n^2) exact baseline on {n_trips:,} trips "
                f"(limit {max_trips:,}).\n"
                f"  Windows grow quadratically with cells-per-trip: this miner OOMs "
                f"at ~200k trips on a 16 GB machine, and would shuffle >100 GB at "
                f"1.71M.\n"
                f"  It exists as GROUND TRUTH for the sketches (M7) and the suffix "
                f"array (M12) at small scale.\n"
                f"  For exact results at scale use route_mining_suffix_array, which "
                f"indexes n suffixes instead of n^2 windows.\n"
                f"  Override deliberately with --max-trips N if you mean it.")

        # ---- emit windows, then aggregate to (subroute -> trip support) ----
        # NOT cached: the window frame is the largest intermediate in the whole
        # pipeline (tens of millions of long string keys), and caching it is what
        # actually blows the heap. sum(support) recovers the emission count from
        # the aggregate for free, since every window row adds 1 to some key.
        agg = build_support_table(enc)
        agg.cache()
        n_subroutes = agg.count()     # shuffle output size (distinct sub-routes)
        n_windows = agg.agg(F.sum("support")).collect()[0][0] or 0  # shuffle input

        log.info("=" * 60)
        log.info("trips                : %s", f"{n_trips:,}")
        log.info("window emissions     : %s   (groupBy shuffle INPUT)", f"{n_windows:,}")
        log.info("distinct sub-routes  : %s (groupBy shuffle OUTPUT)", f"{n_subroutes:,}")
        log.info("=" * 60)

        # ---- top-100 per threshold; collect small results (<=100 rows each) ----
        rep = [f"# M5 Exact Sub-route Mining ({scale})",
               f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
               f"\ntrips: {n_trips:,} | window emissions: {n_windows:,} | "
               f"distinct sub-routes: {n_subroutes:,}\n",
               "| min_len_km | #candidates(>=L) | top_support | median_support_top100 |",
               "|---|---|---|---|"]

        all_rows = []
        for L in THRESHOLDS_KM:
            cand = agg.filter(F.col("length_km") >= L)
            n_cand = cand.count()
            top = (cand.orderBy(F.col("support").desc(), F.col("length_km").desc())
                   .limit(TOP_N).collect())
            top_support = top[0]["support"] if top else 0
            med = top[len(top) // 2]["support"] if top else 0
            rep.append(f"| {L} | {n_cand:,} | {top_support:,} | {med:,} |")
            for rank, r in enumerate(top, 1):
                all_rows.append((L, rank, r["support"], round(r["length_km"], 3),
                                 r["n_cells"], r["subroute"]))

        csv_path = storage.write_csv(
            storage.out_path("routes", f"exact_top100_{scale}.csv"),
            ["min_len_km", "rank", "support", "length_km", "n_cells", "subroute"],
            all_rows)
        rp = storage.write_lines(
            storage.out_path("statistics", f"m5_exact_mining_{scale}.md"), rep)

        log.info("\n%s", "\n".join(rep))
        log.info("wrote top routes -> %s", csv_path)
        log.info("wrote report     -> %s", rp)
        st.update(trips=n_trips, shuffle_records=n_windows,
                  distinct_keys=n_subroutes, mining_s=round(time.time() - t0, 1))

    spark.stop()
    log.info("M5 EXACT MINING COMPLETE.")


if __name__ == "__main__":
    ap = cli.scale_parser(__doc__)
    ap.add_argument("--max-trips", type=int, default=config.EXACT_MAX_TRIPS,
                    help="refuse to run above this trip count (quadratic baseline)")
    args = ap.parse_args()
    main(cli.scale_of(args), args.max_trips)
