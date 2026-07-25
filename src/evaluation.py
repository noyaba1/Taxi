"""
evaluation.py  --  cross-method comparison (A vs B vs C vs D)
=============================================================
Post-processes the route outputs of every method (small CSVs -> pandas is
appropriate) and produces the comparison the assignment grades on:

  * how many routes each method finds, their popularity and longest route,
  * runtime / memory / work volume per stage (read from the timings written by
    cli.stage, so the numbers are always from the run that just happened),
  * the CROSS-METHOD OVERLAP: do the different lenses discover the SAME
    corridors? (cell-set Jaccard match) -- the key scientific question.

Methods compared:
  A = clustering       (MinHash-LSH + star clustering -> shared cell run)
  B = maximal-frequent (min-support X% + maximal, per-length X calibration)
  C = transition graph (PageRank zones + dominant-flow heavy paths)
  D = suffix array     (generalised suffix array + LCP intervals)

All four now report the SAME unit -- a contiguous sub-route with a distinct-trip
support -- so the popularity columns are directly comparable. They were not
before: Method A used to report whole trajectories counted by cluster size.

EVERY NUMBER AND EVERY CONCLUSION BELOW IS COMPUTED.
This report used to end with a hard-coded paragraph asserting which methods
agreed, and quoted fixed precision figures, regardless of what the run produced.
Re-running on different data silently produced a report that contradicted its own
tables. The interpretation is now derived from the matrix immediately above it.

Run:
    python -m src.evaluation --sample
"""
import json

import pandas as pd

from src import cli, config, storage

DELIM = ">"
MATCH_JACCARD = 0.5           # two routes "match" if cell-set Jaccard >= this

log = cli.setup_logging("m16")

# method -> (file stem, route column). Support/length columns are uniform now.
METHODS = {
    "A": ("clustering_top100", "subroute", "clustering"),
    "B": ("maximal_frequent_top100", "subroute", "maximal-frequent"),
    "C": ("graph_heavy_paths_top100", "route", "transition-graph"),
    "D": ("suffix_array_top100", "subroute", "suffix-array"),
}


def _load(method, scale):
    """[(cellset, support, length_km, min_len_km)] for a method, [] if absent."""
    fname, rcol, _label = METHODS[method]
    path = storage.out_path("routes", f"{fname}_{scale}.csv")
    rows = storage.read_csv_rows(path)      # [] when the stage was not run
    if not rows:
        log.warning("no output for method %s (%s)", method, path)
        return []
    out = []
    for r in rows:
        cells = frozenset(str(r[rcol]).split(DELIM))
        out.append((cells, int(r["support"]), float(r["length_km"]),
                    int(r["min_len_km"])))
    return out


def _match_fraction(xs, ys):
    """Fraction of routes in xs that have a Jaccard>=MATCH_JACCARD partner in ys."""
    if not xs or not ys:
        return None
    matched = 0
    for cx, *_ in xs:
        for cy, *_ in ys:
            inter = len(cx & cy)
            if inter and inter / len(cx | cy) >= MATCH_JACCARD:
                matched += 1
                break
    return matched / len(xs)


def _timings(scale):
    """Per-stage cost recorded by cli.stage during this scale's runs."""
    path = storage.out_path("statistics", "timings.jsonl")
    if not storage.exists(path):
        return []
    seen = {}
    for line in storage.read_text(path).splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if rec.get("scale") == scale and rec.get("ok"):
            seen[rec["stage"]] = rec       # keep the most recent run of a stage
    return list(seen.values())


def _interpret(dsets, overlaps, present):
    """Derive the conclusions from the numbers rather than asserting them."""
    lines = ["", "## Interpretation (derived from the tables above)"]

    pairs = [(a, b, overlaps[(a, b)]) for a in present for b in present
             if a != b and overlaps.get((a, b)) is not None]
    if pairs:
        ba, bb, bv = max(pairs, key=lambda p: p[2])
        wa, wb, wv = min(pairs, key=lambda p: p[2])
        lines.append(
            f"- Strongest agreement: **{ba}->{bb}** at {bv:.2f} -- "
            f"{bv:.0%} of {METHODS[ba][2]}'s routes have a Jaccard>="
            f"{MATCH_JACCARD} partner among {METHODS[bb][2]}'s. Two methods with "
            f"different failure modes converging on the same corridors is the "
            f"strongest evidence available that those corridors are real.")
        lines.append(
            f"- Weakest agreement: **{wa}->{wb}** at {wv:.2f}. Low overlap is not "
            f"in itself a defect -- the methods optimise different things -- but "
            f"it marks where the answers depend on the lens.")

    for m in present:
        rs = dsets[m]
        if not rs:
            continue
        longest = max(r[2] for r in rs)
        top = max(r[1] for r in rs)
        lines.append(f"- {m} ({METHODS[m][2]}): {len(rs)} distinct routes, "
                     f"top support {top}, longest {longest:.2f} km.")

    missing = [m for m in METHODS if m not in present]
    if missing:
        lines.append(f"- No output for method(s) {', '.join(missing)}; run those "
                     f"stages before quoting this comparison.")
    return lines


def main(scale: str) -> None:
    with cli.session_if_remote("evaluation"):
        _main(scale)


def _main(scale: str) -> None:
    methods = {m: _load(m, scale) for m in METHODS}
    present = [m for m, v in methods.items() if v]
    if not present:
        raise SystemExit("no method outputs found; run the mining stages first")

    lines = [f"# Cross-Method Comparison ({scale})", ""]
    lines += [f"- **{m}** = {METHODS[m][2]}" for m in METHODS]
    lines += ["",
              "All methods report contiguous sub-routes with distinct-trip support,",
              "so popularity is directly comparable across columns.", ""]

    # --- summary per method at each length config ---
    lines += ["## Routes / popularity / longest, per min-length",
              "| method | min_len | #routes | top_support | longest_km |",
              "|---|---|---|---|---|"]
    for L in config.ROUTE_LENGTH_THRESHOLDS_KM:
        for m in present:
            rs = [r for r in methods[m] if r[3] == L]
            if not rs:
                lines.append(f"| {m} | {L} | 0 | - | - |")
                continue
            lines.append(f"| {m} | {L} | {len(rs)} | {max(r[1] for r in rs)} | "
                         f"{max(r[2] for r in rs):.2f} |")

    # --- cost per stage, measured ---
    tim = _timings(scale)
    if tim:
        lines += ["", "## Cost per stage (measured this run)",
                  "| stage | wall_s | peak_rss_mb | rows_in | rows_out | shuffle_records |",
                  "|---|---|---|---|---|---|"]
        def num(rec, *keys):
            """First present key, thousands-separated; '-' when a stage has none."""
            for k in keys:
                v = rec.get(k)
                if isinstance(v, (int, float)):
                    return f"{v:,}"
            return "-"

        for rec in sorted(tim, key=lambda r: -r["wall_s"]):
            lines.append(
                f"| {rec['stage']} | {rec['wall_s']:.1f} "
                f"| {rec.get('peak_rss_mb', '-')} "
                f"| {num(rec, 'rows_in', 'trips')} "
                f"| {num(rec, 'rows_out', 'routes', 'maximal')} "
                f"| {num(rec, 'shuffle_records', 'suffixes', 'edges')} |")

    # --- cross-method overlap at a length every method reaches ---
    common_l = 3
    def distinct(m, L=common_l):
        seen, out = set(), []
        for cells, sup, length, ml in methods[m]:
            if ml == L and cells not in seen:
                seen.add(cells)
                out.append((cells, sup, length))
        return out

    dsets = {m: distinct(m) for m in present}
    overlaps = {}
    for a in present:
        for b in present:
            if a != b:
                overlaps[(a, b)] = _match_fraction(dsets[a], dsets[b])

    lines += ["", f"## Cross-method overlap at >={common_l} km",
              f"Fraction of the ROW method's routes with a cell-set "
              f"Jaccard>={MATCH_JACCARD} match in the COLUMN method.", "",
              "| row\\col | " + " | ".join(present) + " |",
              "|---" * (len(present) + 1) + "|"]
    for a in present:
        cells = []
        for b in present:
            if a == b:
                cells.append("1.00")
            else:
                v = overlaps.get((a, b))
                cells.append("-" if v is None else f"{v:.2f}")
        lines.append(f"| {a} | " + " | ".join(cells) + " |")

    lines += _interpret(dsets, overlaps, present)

    rp = storage.write_lines(
        storage.out_path("statistics", f"method_comparison_{scale}.md"), lines)
    log.info("\n%s", "\n".join(lines))
    log.info("wrote comparison -> %s", rp)
    log.info("METHOD COMPARISON COMPLETE.")


if __name__ == "__main__":
    args = cli.scale_parser(__doc__).parse_args()
    main(cli.scale_of(args))
