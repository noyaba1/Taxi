"""
rider_mix.py  --  who starts the trips that use each corridor?
==============================================================
The dataset ships a `CALL_TYPE` per trip and the pipeline has carried it
through cleaning from the beginning without ever asking anything of it:

    A  dispatched from a central office
    B  hailed at a taxi stand
    C  flagged down in the street

That is a demand-side question the route tables cannot answer. A corridor's
support says how many trips traversed it; `CALL_TYPE` says how those trips
*began*, which is the difference between a street that people are sent along
and a street that people step into a taxi on.

METHOD
------
Corridors come from the mined top-100 tables, so this measures the DELIVERABLE
rather than the corpus. Membership is exact containment -- a trip counts toward
a corridor when the corridor's cell string occurs as a contiguous substring of
the trip's -- reusing the Aho-Corasick automaton the miners already use, so one
pass over the trips resolves every corridor in a band at once rather than
re-scanning per corridor.

Trips are attributed to a corridor at most once (`matches` returns a set), so a
trip that drives a corridor twice does not vote twice.

Run:
    python -m src.rider_mix --sample
"""
from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import functions as F

from src import cells as cells_mod
from src import cli, config, storage
from src.ahocorasick import Automaton
from src.spark_session import get_spark

log = cli.setup_logging("rider-mix")

DELIM = ">"
CALL_TYPES = [("A", "dispatch"), ("B", "taxi stand"), ("C", "street hail")]


def _load_corridors(scale: str) -> dict:
    """{min_len_km: [cell-tuple, ...]} from Method D's top-100 table."""
    rows = storage.read_csv_rows(
        storage.out_path("routes", f"suffix_array_top100_{scale}.csv"))
    out: dict = {}
    for r in rows:
        L = int(float(r["min_len_km"]))
        out.setdefault(L, []).append(tuple(r["subroute"].split(DELIM)))
    return out


def main(scale: str) -> None:
    spark = get_spark("rider-mix")
    paths = config.dataset_paths(scale)

    with cli.stage("m19_rider_mix", scale, log) as st:
        corridors = _load_corridors(scale)
        if not corridors:
            log.warning("no corridor table for scale=%s -- run Method D first", scale)
            spark.stop()
            return

        enc = spark.read.parquet(paths["encoded"]).select("TRIP_ID", "h3_seq_compact")
        clean = spark.read.parquet(paths["clean"]).select("TRIP_ID", "CALL_TYPE")
        trips = enc.join(clean, "TRIP_ID").select("CALL_TYPE", "h3_seq_compact")
        trips.cache()
        n_trips = trips.count()

        # Baseline: the mix across ALL trips. Without it a band's mix cannot be
        # read -- 49% stand looks like a finding until you see the corpus is 49%
        # stand too, and the interesting number is the DEVIATION.
        base = {r["CALL_TYPE"]: r["n"] for r in
                trips.groupBy("CALL_TYPE").agg(F.count(F.lit(1)).alias("n")).collect()}
        base_tot = sum(base.values()) or 1
        log.info("corpus mix over %s trips: %s", f"{n_trips:,}",
                 {k: f"{100 * v / base_tot:.1f}%" for k, v in sorted(base.items())})

        rows_rdd = trips.rdd.map(lambda r: (r["CALL_TYPE"], r["h3_seq_compact"]))
        results = {}
        for L in sorted(corridors):
            pats = corridors[L]

            def count_part(part, pats=pats):
                # One automaton per partition: the corridor list is the only
                # thing that crosses the network, and every corridor in the
                # band is resolved in a single pass over the trip.
                auto = Automaton(pats)
                agg: dict = {}
                for call_type, seq in part:
                    if not seq:
                        continue
                    for seg in cells_mod.split_at_gaps(list(seq)):
                        if auto.matches(seg):
                            agg[call_type] = agg.get(call_type, 0) + 1
                            break   # a trip votes once per band, not per corridor
                yield agg

            def merge(a, b):
                out = dict(a)
                for k, v in b.items():
                    out[k] = out.get(k, 0) + v
                return out

            mix = rows_rdd.mapPartitions(count_part).reduce(merge)
            tot = sum(mix.values())
            results[L] = (mix, tot)
            log.info("  >=%2d km | %5s traversing trips | %s", L, f"{tot:,}",
                     {k: f"{100 * v / tot:.1f}%" for k, v in sorted(mix.items())} if tot else "-")

        st.update(trips=n_trips, bands=len(results))
        _report(scale, n_trips, base, base_tot, results)

    spark.stop()
    log.info("RIDER MIX COMPLETE.")


def _report(scale, n_trips, base, base_tot, results):
    lines = [f"# Rider mix per length configuration ({scale})",
             f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
             "",
             "`CALL_TYPE` records how a trip BEGAN: **A** dispatched from a central",
             "office, **B** hailed at a taxi stand, **C** flagged down in the street.",
             "Trips are attributed to a band once, however many of its corridors they",
             "traverse, so these are shares of distinct trips.",
             "",
             f"Corpus baseline over {n_trips:,} trips: " +
             " · ".join(f"**{k}** {100 * base.get(k, 0) / base_tot:.1f}%"
                        for k, _ in CALL_TYPES),
             "",
             "| min_len | traversing trips | " +
             " | ".join(f"{k} {n}" for k, n in CALL_TYPES) + " |",
             "|---" * (2 + len(CALL_TYPES)) + "|"]
    for L, (mix, tot) in sorted(results.items()):
        if not tot:
            lines.append(f"| ≥{L} km | 0 | — | — | — |")
            continue
        lines.append(f"| ≥{L} km | {tot:,} | " +
                     " | ".join(f"{100 * mix.get(k, 0) / tot:.1f}%" for k, _ in CALL_TYPES) +
                     " |")

    # State the trend the numbers show, or say that they show none.
    lines += ["", "## What the numbers say", ""]
    bands = sorted(results)
    usable = [L for L in bands if results[L][1] > 0]
    if len(usable) >= 2:
        lo, hi = usable[0], usable[-1]
        for code, name in CALL_TYPES:
            f_lo = 100 * results[lo][0].get(code, 0) / results[lo][1]
            f_hi = 100 * results[hi][0].get(code, 0) / results[hi][1]
            d = f_hi - f_lo
            verb = "rises" if d > 1 else ("falls" if d < -1 else "is flat")
            lines.append(f"- **{code} ({name})** {verb} from {f_lo:.1f}% at ≥{lo} km "
                         f"to {f_hi:.1f}% at ≥{hi} km ({d:+.1f} points).")
        lines += ["",
                  "Read against the corpus baseline above, not against each other: a",
                  "share only means something as a deviation from how trips begin in",
                  "general."]
    else:
        lines.append("- Only one length band had traversing trips, so no trend across "
                     "lengths can be read. Re-run at a scale where the long bands "
                     "populate.")
    p = storage.write_lines(
        storage.out_path("statistics", f"rider_mix_{scale}.md"), lines)
    log.info("wrote report -> %s", p)


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
