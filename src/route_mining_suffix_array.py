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

    `rows` is a list of (trip_id, prev_cell, suffix_cells).
    Returns (subroute, support, n_cells, length_km, best_right, best_left).

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

    rows = sorted(rows, key=lambda r: r[2])      # <- the suffix array
    n = len(rows)
    trips = [r[0] for r in rows]
    prevs = [r[1] for r in rows]
    sufs = [r[2] for r in rows]

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
                    int(best_right), int(best_left)))
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
    enc = spark.read.parquet(paths["encoded"]).select("TRIP_ID", "h3_seq_compact")
    n_trips = enc.count()

    suffixes = (enc
                .select("TRIP_ID", F.explode(_suffixes("h3_seq_compact")).alias("s"))
                .select("TRIP_ID", "s.bucket", "s.prev_cell", "s.suffix"))
    n_suffixes = suffixes.count()

    max_len = config.MAX_SUBROUTE_KM

    def _mine_group(pdf):
        """One bucket, complete, in one frame -- that is what makes it exact."""
        import pandas as pd

        rows = list(zip(pdf["TRIP_ID"], pdf["prev_cell"],
                        [list(s) for s in pdf["suffix"]]))
        return pd.DataFrame(
            mine_bucket(rows, min_sup, MIN_L, max_len),
            columns=["subroute", "support", "n_cells", "length_km",
                     "best_right", "best_left"])

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


def calibrate(routes, n_trips, thresholds, grid, top_k):
    """
    For each length threshold L, the largest X whose maximal-frequent set still
    yields `top_k` routes of length >= L.

    A single global X cannot serve every configuration: maximal-frequent routes
    are ALREADY the longest stretches clearing X, so filtering them at 40 km does
    not find 40 km routes -- it asks whether the one chosen X happened to produce
    any. At X=0.5% on 1.71M trips a 40 km corridor would need ~8,500 distinct
    trips over one unbroken stretch, so that configuration comes back empty and
    the deliverable is simply missing.
    """
    steps, seen = [], set()
    for x in grid:
        ms = min_support_for(x, n_trips)
        if ms not in seen:
            seen.add(ms)
            steps.append((x, ms))

    chosen, per_x, last = {}, {}, None
    for x, ms in steps:
        m = maximal_at(routes, ms).cache()
        stats = m.agg(F.count(F.lit(1)).alias("n"),
                      F.max("length_km").alias("mx")).collect()[0]
        per_x[x] = (ms, stats["n"], stats["mx"] or 0.0)
        last = (x, ms, m)
        log.info("  X=%-6s min_sup=%-8d maximal=%-9s longest=%.2f km",
                 f"{x}%", ms, f"{stats['n']:,}", stats["mx"] or 0.0)
        for L in thresholds:
            if L not in chosen and m.filter(F.col("length_km") >= L).count() >= top_k:
                chosen[L] = (x, ms, m)

    x, ms, m = last
    for L in thresholds:
        chosen.setdefault(L, (x, ms, m))       # loosest floor; may be empty
    for gx in grid:
        per_x.setdefault(gx, per_x[next(sx for sx, sm in steps
                                        if sm == min_support_for(gx, n_trips))])
    return chosen, per_x


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

        # Mine ONCE at the loosest floor we will ever ask about, so the whole X
        # grid is answerable by filtering. The floor also prunes the interval
        # walk, and it scales with the data -- exactly the right behaviour.
        grid = config.SUPPORT_X_PCT_GRID if calibrate_x else [x_pct]
        floor_sup = min(min_support_for(x, n_probe) for x in grid)

        n_trips, n_suffixes, routes = mine(spark, scale, floor_sup)
        routes = routes.cache()
        n_routes = routes.count()

        log.info("=" * 62)
        log.info("trips                    : %s", f"{n_trips:,}")
        log.info("suffixes indexed         : %s   (vs O(n^2) windows in M5)",
                 f"{n_suffixes:,}")
        log.info("mining floor             : %d trips", floor_sup)
        log.info("frequent branching routes: %s", f"{n_routes:,}")
        log.info("=" * 62)

        chosen, per_x = calibrate(routes, n_trips, THRESHOLDS, grid, TOP_K)

        rep = [f"# Method D: Suffix Array Sub-route Mining ({scale})",
               f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
               "",
               f"trips: {n_trips:,} | suffixes indexed: {n_suffixes:,} | "
               f"mining floor: {floor_sup} trips | candidate routes: {n_routes:,}",
               "",
               f"Suffixes are bucketed by their first {config.SA_PREFIX_CELLS} cells, "
               "so all occurrences of any sub-route land in one partition and "
               "per-partition counting is globally exact -- no cross-partition "
               "merge, no window explosion.",
               "",
               "## Top maximal-frequent routes per length configuration",
               "",
               "`X%` is calibrated per configuration: the largest support floor "
               f"that still yields {TOP_K} routes at that minimum length.",
               "",
               "| min_len_km | X% used | min_sup | #maximal(>=L) | top_support | longest_km |",
               "|---|---|---|---|---|---|"]

        all_rows, empty = [], []
        for L in THRESHOLDS:
            x, ms, m = chosen[L]
            cand = m.filter(F.col("length_km") >= L)
            n_at_l = cand.count()
            top = (cand.orderBy(F.col("support").desc(), F.col("length_km").desc())
                   .limit(TOP_K).collect())
            top_sup = top[0]["support"] if top else 0
            longest = max((r["length_km"] for r in top), default=0.0)
            rep.append(f"| {L} | {x} | {ms:,} | {n_at_l:,} | {top_sup:,} "
                       f"| {longest:.2f} |")
            if not top:
                empty.append(L)
            for rank, r in enumerate(top, 1):
                all_rows.append((L, x, rank, r["support"], round(r["length_km"], 3),
                                 r["n_cells"], r["subroute"]))

        if empty:
            longest_any = max((v[2] for v in per_x.values()), default=0.0)
            rep += ["",
                    f"> **Empty configurations: "
                    f"{', '.join(f'>={L} km' for L in empty)}.** Even at the "
                    f"loosest floor the longest contiguous stretch shared by more "
                    f"than one trip is {longest_any:.1f} km. With {n_trips:,} trips "
                    f"no {min(empty)} km corridor is driven twice, so there is "
                    f"nothing popular to report at that length -- a property of "
                    f"the data volume, not a filter."]

        rep += ["", "## X% sweep (the calibration search space)",
                "| X% | min_sup | #maximal-frequent | longest_km |", "|---|---|---|---|"]
        for x in grid:
            ms, n_r, longest = per_x[x]
            rep.append(f"| {x} | {ms:,} | {n_r:,} | {longest:.2f} |")

        rep += _holes_section(routes, n_trips, x_pct)

        csv_path = storage.write_csv(
            storage.out_path("routes", f"suffix_array_top100_{scale}.csv"),
            ["min_len_km", "x_pct", "rank", "support", "length_km", "n_cells",
             "subroute"],
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
                    help="use --x-pct alone instead of calibrating X per length")
    args = ap.parse_args()
    main(cli.scale_of(args), args.x_pct, not args.no_calibrate)
