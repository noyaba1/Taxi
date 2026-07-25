"""
route_mining_suffix_array.py  --  METHOD D  (Suffix Array + LCP intervals)
=========================================================================
The assignment requires "a method based on Suffix Tree / Suffix Array". This is
it: a distributed generalised suffix array over the H3-cell alphabet, with an
LCP array, mining frequent maximal sub-routes from its LCP intervals.

WHY A SUFFIX ARRAY BEATS WINDOW ENUMERATION
-------------------------------------------
The M5 baseline emits EVERY contiguous window of every trip -- O(n^2) rows per
trip, each a long string key -- and shuffles them into one giant groupBy. On the
5k sample that is already ~1.1M rows from 4.9k trips; at 1.71M trips it is
hundreds of millions of rows and >100 GB of shuffle.

A suffix array never materialises those windows. Each trip contributes n
SUFFIXES (not n^2 windows); sorting them puts every repeat of every substring
into one contiguous block, and the LCP array names the repeats directly. Work
drops from O(n^2) emitted rows to O(n log n) per bucket, and the answer is still
EXACT.

HOW IT IS MADE DISTRIBUTED AND STILL EXACT
------------------------------------------
Bucket every suffix by its first `SA_PREFIX_CELLS` cells.

    Every occurrence of a substring S with |S| >= SA_PREFIX_CELLS begins at some
    suffix whose first SA_PREFIX_CELLS cells ARE the first cells of S. So all
    occurrences of S land in the SAME bucket, and a bucket can be counted with no
    cross-partition merge at all.

Substrings shorter than SA_PREFIX_CELLS (3 cells, ~0.6 km at res 9) fall below
the 1 km floor of every reported configuration, so nothing we report is lost.
Suffixes are truncated to SA_MAX_CELLS (~60 km), which matches the length cap
used by the other miners.

WHAT AN LCP INTERVAL GIVES US
-----------------------------
In the sorted suffix block, a maximal run [l..r] whose adjacent LCP values are
all >= h, bounded by smaller values, means: the substring of length h occurs at
exactly those r-l+1 positions. That is the classic suffix-tree internal node, so:

  * support        = DISTINCT trips among those positions (not occurrences);
  * right-maximal  = the interval is branching (guaranteed by the enumeration);
  * left-maximal   = more than one distinct preceding cell (or a trip start).

Both-sided maximality is exactly the "maximal frequent sub-route" the assignment
asks for, obtained structurally rather than by a second extension-join pass.

Run:
    python -m src.route_mining_suffix_array --sample
"""
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F, types as T

from src import cells as cells_mod
from src import cli, config, storage
from src.route_mining_exact import DELIM
from src.route_mining_maximal import min_support_for
from src.spark_session import get_spark

THRESHOLDS = config.ROUTE_LENGTH_THRESHOLDS_KM
TOP_K = config.TOP_K
MIN_L = float(min(THRESHOLDS))

log = cli.setup_logging("m12")

_SUFFIX_SCHEMA = T.ArrayType(T.StructType([
    T.StructField("bucket", T.StringType()),
    T.StructField("prev_cell", T.StringType()),   # None at a segment start
    T.StructField("suffix", T.ArrayType(T.StringType())),
]))

_ROUTE_SCHEMA = T.StructType([
    T.StructField("subroute", T.StringType()),
    T.StructField("support", T.IntegerType()),
    T.StructField("n_cells", T.IntegerType()),
    T.StructField("length_km", T.DoubleType()),
    T.StructField("best_right", T.IntegerType()),
    T.StructField("best_left", T.IntegerType()),
    T.StructField("support_taxis", T.IntegerType()),
])


# ----------------------------------------------------------------- pure core
def lcp_intervals(lcp, n):
    """
    Enumerate the LCP intervals of a sorted suffix block.

    `lcp[i]` is the common prefix length of suffixes i-1 and i (lcp[0] unused).
    Yields (h, l, r): the substring of length h occurs in sorted positions l..r
    inclusive, and the interval is BRANCHING (right-maximal) by construction.

    This is the standard stack sweep (Abouelhoda, Kurtz & Ohlebusch): each
    interval is opened when the LCP rises and closed when it falls below its
    value, which is precisely a suffix-tree internal node.
    """
    if n < 2:
        return
    stack = [(0, 0)]                     # (lcp value, left boundary)
    for i in range(1, n):
        left = i - 1
        while lcp[i] < stack[-1][0]:
            h, l = stack.pop()
            yield h, l, i - 1
            left = l
        if lcp[i] > stack[-1][0]:
            stack.append((lcp[i], left))
    while len(stack) > 1:
        h, l = stack.pop()
        yield h, l, n - 1


def _lcp_len(a, b, cap):
    i = 0
    m = min(len(a), len(b), cap)
    while i < m and a[i] == b[i]:
        i += 1
    return i


def mine_bucket(rows, min_sup, min_len_km, max_len_km):
    """
    Mine one bucket: build the suffix array + LCP array, walk the LCP intervals,
    and emit each frequent sub-route WITH the support of its best one-cell
    extension in each direction.

    `rows` is a list of (trip_id, taxi_id, prev_cell, suffix_cells).
    Returns (subroute, support, n_cells, length_km, best_right, best_left,
             support_taxis).

    SUPPORT IS COUNTED TWICE, ON PURPOSE
    ------------------------------------
    `support` counts distinct TRIPS; `support_taxis` counts distinct TAXIS. With
    only 442 vehicles over a year, a corridor driven 200 times by one driver
    going to their own stand is not "popular" in the sense the brief means -- it
    is one person's habit. Trip-support alone cannot tell the two apart, so we
    carry both and let the report show where they disagree.

    Counted EXACTLY here rather than with HyperLogLog: a bucket holds only the
    suffixes sharing a 3-cell prefix, so the taxi id set is small and a sketch
    would trade accuracy for nothing. HLL earns its place on the activity zones
    (route_mining_graph), where distinct taxis per cell is a genuine
    large-cardinality problem.

    WHY THE EXTENSION SUPPORTS MATTER
    ---------------------------------
    "Maximal among frequent at X%" means: support >= min_sup AND no one-cell
    extension is itself frequent. Carrying `best_right`/`best_left` makes that a
    pure FILTER afterwards:

        maximal(min_sup)  <=>  support >= min_sup
                               and max(best_right, best_left) < min_sup

    So the whole X grid -- and therefore the per-length calibration and the holes
    analysis -- is answered from ONE mining pass, instead of re-deriving a
    support table per X. That is what lets this method carry the deliverable at a
    scale where the window-enumeration route cannot.
    """
    if len(rows) < min_sup:
        # Support is bounded by the number of suffixes here, so this whole bucket
        # cannot contain anything frequent. Cheapest possible prune.
        return []

    rows = sorted(rows, key=lambda r: r[3])      # <- the suffix array
    n = len(rows)
    trips = [r[0] for r in rows]
    taxis = [r[1] for r in rows]
    prevs = [r[2] for r in rows]
    sufs = [r[3] for r in rows]

    cap = max(len(s) for s in sufs)
    lcp = [0] * n
    for i in range(1, n):
        lcp[i] = _lcp_len(sufs[i - 1], sufs[i], cap)

    # Collect the surviving intervals first; extension supports need the set.
    kept = []
    for h, l, r in lcp_intervals(lcp, n):
        if h < config.SA_PREFIX_CELLS:
            continue
        if r - l + 1 < min_sup:
            # Occurrences bound support, so skip before touching the range. This
            # prune is what keeps the interval walk near-linear: the numerous
            # deep/narrow intervals never get scanned.
            continue
        support = len(set(trips[l:r + 1]))
        if support < min_sup:
            continue
        kept.append((h, l, r, support))

    out = []
    emitted = set()
    for h, l, r, support in kept:
        cand = sufs[l][:h]
        key = DELIM.join(cand)
        if key in emitted:
            continue
        length = cells_mod.path_length_km(cand)
        if length < min_len_km or length > max_len_km:
            continue

        # Best RIGHT extension: the strongest descendant interval. Descendants
        # are longer substrings sharing this prefix, i.e. exactly the one-cell
        # (and further) extensions; support is monotone decreasing with depth, so
        # the max over all descendants equals the max over direct children.
        best_right = 0
        for h2, l2, r2, sup2 in kept:
            if h2 > h and l2 >= l and r2 <= r and sup2 > best_right:
                best_right = sup2

        # Best LEFT extension: distinct trips per preceding cell. A None entry is
        # a segment start -- the route cannot be extended left there, so it is
        # not an extension and must not count towards one.
        by_prev: dict = {}
        for i in range(l, r + 1):
            if prevs[i] is not None:
                by_prev.setdefault(prevs[i], set()).add(trips[i])
        best_left = max((len(v) for v in by_prev.values()), default=0)

        emitted.add(key)
        out.append((key, int(support), int(h), float(length),
                    int(best_right), int(best_left),
                    len({taxis[i] for i in range(l, r + 1)})))
    return out


# ------------------------------------------------------------------ Spark job
@F.udf(_SUFFIX_SCHEMA)
def _suffixes(cells):
    """
    Every suffix of every continuous segment of a trip, truncated and bucketed.

    Splitting at GPS gaps first means no suffix can straddle a hole in the trace,
    so no mined sub-route can be an artefact of lost signal.
    """
    k, cap = config.SA_PREFIX_CELLS, config.SA_MAX_CELLS
    out = []
    for seg in cells_mod.split_at_gaps(cells):
        for i in range(len(seg) - k + 1):
            out.append({
                "bucket": DELIM.join(seg[i:i + k]),
                "prev_cell": seg[i - 1] if i > 0 else None,
                "suffix": seg[i:i + cap],
            })
    return out


def mine(spark, scale, min_sup):
    """Suffixes -> bucket -> per-bucket suffix array -> frequent maximal routes."""
    paths = config.dataset_paths(scale)
    enc = spark.read.parquet(paths["encoded"]).select(
        "TRIP_ID", "TAXI_ID", "h3_seq_compact")
    return mine_encoded(enc, min_sup)


def mine_encoded(enc, min_sup):
    """
    The miner, over an already-loaded encoded frame.

    Split out from `mine` so callers that need a SUBSET -- the temporal analysis
    mines each hour bucket separately -- drive the real miner instead of
    reimplementing it. `enc` must carry TRIP_ID, TAXI_ID, h3_seq_compact.
    """
    n_trips = enc.count()

    suffixes = (enc
                .select("TRIP_ID", "TAXI_ID",
                        F.explode(_suffixes("h3_seq_compact")).alias("s"))
                .select("TRIP_ID", "TAXI_ID", "s.bucket", "s.prev_cell",
                        "s.suffix"))
    n_suffixes = suffixes.count()

    max_len = config.MAX_SUBROUTE_KM

    def _mine_group(pdf):
        """One bucket, complete, in one frame -- that is what makes it exact."""
        import pandas as pd

        rows = list(zip(pdf["TRIP_ID"], pdf["TAXI_ID"], pdf["prev_cell"],
                        [list(s) for s in pdf["suffix"]]))
        return pd.DataFrame(
            mine_bucket(rows, min_sup, MIN_L, max_len),
            columns=["subroute", "support", "n_cells", "length_km",
                     "best_right", "best_left", "support_taxis"])

    # groupBy(...).applyInPandas guarantees ONE frame per bucket. (mapInPandas
    # would hand us arbitrary Arrow batches, which can split a bucket across two
    # calls and silently undercount support -- the whole exactness argument rests
    # on a bucket being seen whole.)
    routes = (suffixes.groupBy("bucket")
              .applyInPandas(_mine_group, schema=_ROUTE_SCHEMA))
    return n_trips, n_suffixes, routes


def maximal_at(routes, min_sup):
    """
    Maximal-frequent sub-routes at an absolute support floor.

    Pure filter over the mined table -- no re-mining, no shuffle -- because
    mine_bucket already carried each route's best one-cell extension in both
    directions. This is what makes the per-length X calibration affordable.
    """
    return routes.filter(
        (F.col("support") >= min_sup)
        & (F.greatest(F.col("best_right"), F.col("best_left")) < min_sup))


def calibrate(routes, n_trips, thresholds, floors, top_k):
    """
    For each length threshold L, the TIGHTEST support floor that still yields
    `top_k` routes of length >= L -- i.e. the most demanding definition of
    "popular" under which that length band is still populated.

    WHY ABSOLUTE FLOORS AND NOT PERCENTAGES
    ---------------------------------------
    A percentage floor is scale-dependent in the wrong direction. X=0.01% is 2
    trips on the 5k sample but 171 trips at 1.71M, so the bottom of a percentage
    grid gets HARDER to clear as the dataset grows and the long length bands get
    emptier the more data you have. Measured: the sample reached min_sup=2 and a
    11.99 km corridor; mid bottomed out at min_sup=19 and 11.06 km. Backwards.

    The brief asks us to experiment with X "when you are interested in maximising
    the sub-route length", so the instrument is an absolute floor, always
    reported together with the X% it corresponds to at this scale.

    Returns (chosen, per_floor):
      chosen[L]      = (min_sup, x_pct, DataFrame)
      per_floor[ms]  = (x_pct, n_routes, longest_km)
    """
    chosen, per_floor, loosest = {}, {}, None
    # Tightest first: the first floor that fills a band is the strongest claim
    # we can make about it.
    for ms in sorted(floors, reverse=True):
        if ms > n_trips:
            continue
        m = maximal_at(routes, ms).cache()
        stats = m.agg(F.count(F.lit(1)).alias("n"),
                      F.max("length_km").alias("mx")).collect()[0]
        x_pct = config.pct_of(ms, n_trips)
        per_floor[ms] = (x_pct, stats["n"], stats["mx"] or 0.0)
        loosest = (ms, x_pct, m)
        log.info("  min_sup=%-6d (X=%.4f%%)  maximal=%-9s longest=%.2f km",
                 ms, x_pct, f"{stats['n']:,}", stats["mx"] or 0.0)
        for L in thresholds:
            if L not in chosen and m.filter(F.col("length_km") >= L).count() >= top_k:
                chosen[L] = (ms, x_pct, m)

    if loosest is None:                       # dataset smaller than every floor
        return {}, {}
    for L in thresholds:
        chosen.setdefault(L, loosest)         # loosest floor reached; may be empty
    return chosen, per_floor


def _holes_section(routes, n_trips, x_pct):
    """
    Why a maximal route terminates: every continuation is below the floor.

    `best_right`/`best_left` ARE those continuations' supports, so the hole is
    visible directly -- support S carries on, but the strongest single next cell
    only carries best_right < min_sup of it.
    """
    ms = min_support_for(x_pct, n_trips)
    top = (maximal_at(routes, ms).orderBy(F.col("support").desc())
           .limit(5).collect())
    out = ["", "## Holes analysis (why maximal routes terminate = traffic forks)", "",
           f"Reference X={x_pct}% -> min_sup={ms} trips. A route ends where traffic "
           f"splits: the corridor continues, but no single next cell carries "
           f"{ms} trips, so a HOLE opens between this sub-route and the next.", "",
           "| support | cells | length_km | best right continuation | best left continuation |",
           "|---|---|---|---|---|"]
    if not top:
        out += [f"| _(no maximal-frequent route at X={x_pct}%)_ | | | | |"]
    for r in top:
        out.append(f"| {r['support']} | {r['n_cells']} | {r['length_km']:.2f} "
                   f"| {r['best_right']} | {r['best_left']} |")
    return out


def main(scale: str, x_pct: float, calibrate_x: bool) -> None:
    spark = get_spark("route-mining-suffix-array")

    with cli.stage("m12_suffix_array", scale, log) as st:
        t0 = time.time()
        paths = config.dataset_paths(scale)
        n_probe = spark.read.parquet(paths["encoded"]).count()

        floors = (config.SUPPORT_MIN_SUP_GRID if calibrate_x
                  else [min_support_for(x_pct, n_probe)])
        # Mine ONCE at the loosest floor we will ever ask about, so the whole
        # grid is answerable by filtering. The floor also prunes the interval
        # walk, so the cost scales with how strict a definition we need.
        floor_sup = min(f for f in floors if f <= n_probe) if floors else 2

        n_trips, n_suffixes, routes = mine(spark, scale, floor_sup)
        routes = routes.cache()
        n_routes = routes.count()

        log.info("=" * 62)
        log.info("trips                    : %s", f"{n_trips:,}")
        log.info("suffixes indexed         : %s   (vs O(n^2) windows in M5)",
                 f"{n_suffixes:,}")
        log.info("mining floor             : %d trips (X=%.4f%%)",
                 floor_sup, config.pct_of(floor_sup, n_trips))
        log.info("frequent branching routes: %s", f"{n_routes:,}")
        log.info("=" * 62)

        chosen, per_floor = calibrate(routes, n_trips, THRESHOLDS, floors, TOP_K)

        rep = [f"# Method D: Suffix Array Sub-route Mining ({scale})",
               f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
               "",
               f"trips: {n_trips:,} | suffixes indexed: {n_suffixes:,} | "
               f"mining floor: {floor_sup} trips | candidate routes: {n_routes:,}",
               "",
               f"Suffixes are bucketed by their first {config.SA_PREFIX_CELLS} "
               "cells, so all occurrences of any sub-route land in one partition "
               "and per-partition counting is globally exact -- no cross-partition "
               "merge, no window explosion.",
               "",
               "## Top maximal-frequent routes per length configuration",
               "",
               "The support floor is calibrated **per configuration**: the "
               "TIGHTEST floor that still fills the band, i.e. the strongest claim "
               "the data supports at that length. It is absolute (a trip count) "
               "because a percentage floor gets harder to clear as the dataset "
               "grows -- see config.SUPPORT_MIN_SUP_GRID. The X% it corresponds to "
               "at this scale is reported alongside.",
               "",
               "`support` counts distinct TRIPS; `taxis` counts distinct VEHICLES. "
               "A corridor with high support but very few taxis is one driver's "
               "habit, not a popular route.",
               "",
               "| min_len_km | min_sup | = X% | #maximal(>=L) | top_support | taxis | longest_km |",
               "|---|---|---|---|---|---|---|"]

        all_rows, empty = [], []
        for L in THRESHOLDS:
            ms, xp, m = chosen[L]
            cand = m.filter(F.col("length_km") >= L)
            n_at_l = cand.count()
            top = (cand.orderBy(F.col("support").desc(), F.col("length_km").desc())
                   .limit(TOP_K).collect())
            top_sup = top[0]["support"] if top else 0
            top_taxis = top[0]["support_taxis"] if top else 0
            longest = max((r["length_km"] for r in top), default=0.0)
            rep.append(f"| {L} | {ms:,} | {xp:.4f}% | {n_at_l:,} | {top_sup:,} "
                       f"| {top_taxis:,} | {longest:.2f} |")
            if not top:
                empty.append(L)
            for rank, r in enumerate(top, 1):
                all_rows.append((L, ms, round(xp, 5), rank, r["support"],
                                 r["support_taxis"], round(r["length_km"], 3),
                                 r["n_cells"], r["subroute"]))

        if empty:
            longest_any = max((v[2] for v in per_floor.values()), default=0.0)
            rep += ["",
                    f"> **Empty configurations: "
                    f"{', '.join(f'>={L} km' for L in empty)}.** Even at the "
                    f"loosest floor tried ({min(per_floor) if per_floor else '?'} "
                    f"trips) the longest contiguous stretch shared by that many "
                    f"trips is {longest_any:.1f} km. With {n_trips:,} trips no "
                    f"{min(empty)} km corridor is repeated, so there is nothing "
                    f"popular to report at that length -- a property of the data "
                    f"volume, not of the filter."]

        rep += ["", "## Support-floor sweep (how length trades against strictness)",
                "| min_sup | = X% | #maximal-frequent | longest_km |",
                "|---|---|---|---|"]
        for ms in sorted(per_floor, reverse=True):
            xp, n_r, longest = per_floor[ms]
            rep.append(f"| {ms:,} | {xp:.4f}% | {n_r:,} | {longest:.2f} |")

        rep += _holes_section(routes, n_trips, x_pct)

        csv_path = storage.write_csv(
            storage.out_path("routes", f"suffix_array_top100_{scale}.csv"),
            ["min_len_km", "min_sup", "x_pct", "rank", "support", "support_taxis",
             "length_km", "n_cells", "subroute"],
            all_rows)
        rp = storage.write_lines(
            storage.out_path("statistics", f"m12_suffix_array_{scale}.md"), rep)

        log.info("\n%s", "\n".join(rep))
        log.info("wrote routes -> %s", csv_path)
        log.info("wrote report -> %s", rp)
        st.update(trips=n_trips, suffixes=n_suffixes, floor_sup=floor_sup,
                  routes=n_routes, mining_s=round(time.time() - t0, 1))

    spark.stop()
    log.info("METHOD D (SUFFIX ARRAY) COMPLETE.")


if __name__ == "__main__":
    ap = cli.scale_parser(__doc__)
    ap.add_argument("--x-pct", type=float, default=config.SUPPORT_X_PCT,
                    help="reference X%% for the holes analysis")
    ap.add_argument("--no-calibrate", action="store_true",
                    help="use --x-pct alone instead of calibrating the floor")
    args = ap.parse_args()
    main(cli.scale_of(args), args.x_pct, not args.no_calibrate)
