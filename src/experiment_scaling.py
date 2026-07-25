"""
experiment_scaling.py  --  how does corridor length grow with data?
==================================================================
The ">=20 km and >=40 km bands are empty" problem has two possible causes, and
they call for opposite responses:

  (a) the instrument -- a percentage support floor gets HARDER to clear as the
      dataset grows, so long bands empty out on bigger data. Fixed: calibration
      now walks ABSOLUTE floors (config.SUPPORT_MIN_SUP_GRID), and that alone
      turned >=20 km from empty into a 21.36 km corridor at 188k trips.

  (b) the data -- past some length, no corridor is genuinely repeated, however
      loose the floor.

Distinguishing them needs more than two points. This runs the real suffix-array
miner across every prepared scale at a FIXED set of absolute floors and records
how the longest maximal-frequent corridor grows with trip count. That converts
"the 40 km band may fill at 1.71M" -- an expectation the report currently states
as a caveat -- into an extrapolation with a stated basis.

It also records wall time per scale, which is what tells the group how long the
DataProc run will actually take before they pay for it.

Scales are run INDEPENDENTLY and a failure is recorded as a datapoint: 800k may
exceed a 16 GB machine, and losing the whole study to that would be silly.

Run:
    python -m src.experiment_scaling                    # every prepared scale
    python -m src.experiment_scaling --scales sample,mid,s400k
"""
import argparse
import math
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from src import cli, config, storage
from src.route_mining_suffix_array import maximal_at, mine
from src.spark_session import get_spark

log = cli.setup_logging("exp-scale")

# Floors held FIXED across scales -- that is the whole point. A percentage would
# move with n and confound the very effect being measured.
PROBE_FLOORS = [2, 5, 20, 100]
FULL_N = 1_710_670          # the target we are extrapolating to


def _prepared(spark, scale):
    """Is this scale's encoded table on disk?"""
    try:
        spark.read.parquet(config.dataset_paths(scale)["encoded"]).limit(1).count()
        return True
    except Exception:  # noqa: BLE001
        return False


def _measure(spark, scale):
    """Mine one scale once at the loosest floor, then read every floor off it."""
    t0 = time.time()
    n_trips, n_suffixes, routes = mine(spark, scale, min(PROBE_FLOORS))
    routes = routes.cache()
    n_routes = routes.count()
    mine_s = time.time() - t0

    per_floor = {}
    for f in PROBE_FLOORS:
        row = (maximal_at(routes, f)
               .agg(F.count(F.lit(1)).alias("n"),
                    F.max("length_km").alias("mx")).collect()[0])
        per_floor[f] = (row["n"], row["mx"] or 0.0)
    return {"n_trips": n_trips, "n_suffixes": n_suffixes, "n_routes": n_routes,
            "mine_s": mine_s, "per_floor": per_floor}


def _fit_loglog(xs, ys):
    """
    Least-squares fit of log(y) = a*log(x) + b, i.e. a power law y = e^b * x^a.

    A power law is the right shape to assume: corridor length grows with data but
    with strongly diminishing returns (every extra trip is less likely to extend
    an already-long shared stretch). Returns (a, b, r2) or None if degenerate.
    """
    pts = [(math.log(x), math.log(y)) for x, y in zip(xs, ys) if x > 0 and y > 0]
    if len(pts) < 3:
        return None
    n = len(pts)
    sx = sum(p[0] for p in pts)
    sy = sum(p[1] for p in pts)
    sxx = sum(p[0] * p[0] for p in pts)
    sxy = sum(p[0] * p[1] for p in pts)
    denom = n * sxx - sx * sx
    if abs(denom) < 1e-12:
        return None
    a = (n * sxy - sx * sy) / denom
    b = (sy - a * sx) / n
    ybar = sy / n
    ss_tot = sum((p[1] - ybar) ** 2 for p in pts)
    ss_res = sum((p[1] - (a * p[0] + b)) ** 2 for p in pts)
    r2 = 1 - ss_res / ss_tot if ss_tot > 1e-12 else float("nan")
    return a, b, r2


def main(scales) -> None:
    spark = get_spark("experiment-scaling")
    results, failures = {}, {}

    for scale in scales:
        if not _prepared(spark, scale):
            log.warning("scale %s has no encoded table; skipping "
                        "(run make_sample + clean + features + encoding first)", scale)
            continue
        try:
            r = _measure(spark, scale)
            results[scale] = r
            log.info("%-8s n=%-9s suffixes=%-11s mine=%5.1fs  longest@floor2=%.2f km",
                     scale, f"{r['n_trips']:,}", f"{r['n_suffixes']:,}",
                     r["mine_s"], r["per_floor"][min(PROBE_FLOORS)][1])
        except Exception as exc:  # noqa: BLE001
            # A scale that does not fit this machine IS a datapoint.
            failures[scale] = f"{type(exc).__name__}: {str(exc)[:200]}"
            log.warning("scale %s FAILED: %s", scale, failures[scale])

    if not results:
        raise SystemExit("no scale produced a measurement")

    ordered = sorted(results, key=lambda s: results[s]["n_trips"])
    lines = [f"# Scaling study: does corridor length grow with data?",
             f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
             "",
             "The support floors below are held FIXED across scales. That is the "
             "point: a percentage floor moves with the trip count and would "
             "confound the effect being measured. Each row is the real "
             "suffix-array miner, not a model of it.",
             "",
             "## Longest maximal-frequent corridor (km) by scale and support floor",
             "",
             "| scale | trips | suffixes | mine_s | " +
             " | ".join(f"floor={f}" for f in PROBE_FLOORS) + " |",
             "|---" * (4 + len(PROBE_FLOORS)) + "|"]
    for s in ordered:
        r = results[s]
        cells = " | ".join(f"{r['per_floor'][f][1]:.2f}" for f in PROBE_FLOORS)
        lines.append(f"| {s} | {r['n_trips']:,} | {r['n_suffixes']:,} "
                     f"| {r['mine_s']:.1f} | {cells} |")

    lines += ["", "## Corridors found (count) by scale and floor", "",
              "| scale | " + " | ".join(f"floor={f}" for f in PROBE_FLOORS) + " |",
              "|---" * (1 + len(PROBE_FLOORS)) + "|"]
    for s in ordered:
        r = results[s]
        lines.append(f"| {s} | " +
                     " | ".join(f"{r['per_floor'][f][0]:,}" for f in PROBE_FLOORS) + " |")

    # ---- extrapolation ----
    lines += ["", "## Extrapolation to the full dataset", "",
              f"Power-law fit `longest_km = c * trips^a` per floor, extrapolated to "
              f"{FULL_N:,} trips. A power law is the right shape to assume: length "
              f"grows with data but with strongly diminishing returns, because each "
              f"extra trip is progressively less likely to extend an ALREADY long "
              f"shared stretch.",
              "",
              "| floor | exponent a | R² | predicted longest @1.71M | reaches 20 km? | reaches 40 km? |",
              "|---|---|---|---|---|---|"]
    ns = [results[s]["n_trips"] for s in ordered]
    predictions = {}
    for f in PROBE_FLOORS:
        ys = [results[s]["per_floor"][f][1] for s in ordered]
        fit = _fit_loglog(ns, ys)
        if not fit:
            lines.append(f"| {f} | _too few points_ | | | | |")
            continue
        a, b, r2 = fit
        pred = math.exp(b) * FULL_N ** a
        predictions[f] = pred
        lines.append(f"| {f} | {a:.3f} | {r2:.3f} | **{pred:.1f} km** "
                     f"| {'yes' if pred >= 20 else 'no'} "
                     f"| {'yes' if pred >= 40 else 'no'} |")

    lines += ["", "## Reading this", ""]
    if predictions:
        loosest = min(predictions)
        p = predictions[loosest]
        lines.append(
            f"At the loosest floor ({loosest} trips) the fit predicts a longest "
            f"corridor of about **{p:.0f} km** on the full 1.71M dataset.")
        lines.append("")
        if p >= 40:
            lines.append(
                "That clears 40 km, so the >=40 km configuration should populate "
                "on the full run — the empty bands seen locally are a data-volume "
                "effect, not a limit of the method.")
        elif p >= 20:
            lines.append(
                "That clears 20 km but **not 40 km**. The honest expectation is "
                "that the >=20 km band populates on the full run and the >=40 km "
                "band stays empty — i.e. Porto simply has no 40 km stretch that "
                "two taxis repeat. That is a finding about the city, not a defect "
                "in the pipeline, and it is better to predict it now than to be "
                "surprised by it in the defence.")
        else:
            lines.append(
                "That does not reach 20 km, so both long configurations are "
                "expected to stay empty even at full scale. The corridors Porto "
                "actually repeats are shorter than the brief's longest bands "
                "assume.")
        lines += ["",
                  "**Caveat worth stating out loud:** at floor=2 'popular' means "
                  "*two trips*. Long corridors found there are near-coincidences, "
                  "not routes the city uses — see the taxi-diversity section of "
                  "the method comparison, where the longest bands are dominated by "
                  "single-vehicle repeats. The length a corridor reaches and the "
                  "confidence it deserves move in opposite directions."]

    if failures:
        lines += ["", "## Scales that did not complete", ""]
        for s, err in failures.items():
            lines.append(f"- `{s}`: {err}")
        lines.append("")
        lines.append("A scale that does not fit this machine is a datapoint about "
                     "the machine, and is recorded rather than allowed to abort "
                     "the study.")

    rp = storage.write_lines(
        storage.out_path("statistics", "experiment_scaling.md"), lines)
    log.info("\n%s", "\n".join(lines))
    log.info("wrote -> %s", rp)
    spark.stop()
    log.info("SCALING STUDY COMPLETE.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scales", type=str, default=None,
                    help="comma-separated scale names (default: all but 'full')")
    args = ap.parse_args()
    chosen = ([s.strip() for s in args.scales.split(",")] if args.scales
              else [s for s in config.SCALES if s != "full"])
    main(chosen)
