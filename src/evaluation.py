"""
evaluation.py  --  Milestone M16  (cross-method comparison A vs B vs C)
=====================================================================
Post-processes the route outputs of all three methods (small CSVs -> pandas is
appropriate) and produces the comparison the assignment grades on:

  * how many routes each method finds, their popularity and longest route,
  * the CROSS-METHOD OVERLAP: do the three different lenses discover the SAME
    corridors? (cell-set Jaccard match) -- the key scientific question.

Methods compared (all on the same 5k sample, H3 res 9):
  A = clustering        (clustering_top100)            popularity = cluster size
  B = maximal-frequent  (maximal_frequent_top100)      popularity = trip support
  C = transition graph  (graph_heavy_paths_top100)     popularity = trip support

Prereq: run route_mining_maximal / clustering / graph first (their CSVs live in
outputs/routes/, git-ignored). Approx-vs-exact accuracy is covered by M7.

Run:
    python -m src.evaluation --sample
"""
import argparse
import os

import pandas as pd

from src import config

DELIM = ">"
MATCH_JACCARD = 0.5           # two routes "match" if cell-set Jaccard >= this


def _load(method, suffix):
    """Return list of (cellset, popularity, length_km) for a method's routes."""
    routes_dir = os.path.join(config.OUTPUT_BASE, "routes")
    spec = {
        "A": ("clustering_top100", "rep_route", "cluster_size", "rep_len_km"),
        "B": ("maximal_frequent_top100", "subroute", "support", "length_km"),
        "C": ("graph_heavy_paths_top100", "route", "support", "length_km"),
    }[method]
    fname, rcol, pcol, lcol = spec
    path = os.path.join(routes_dir, f"{fname}_{suffix}.csv")
    df = pd.read_csv(path)
    out = []
    for _, r in df.iterrows():
        cells = frozenset(str(r[rcol]).split(DELIM))
        out.append((cells, int(r[pcol]), float(r[lcol]), int(r["min_len_km"])))
    return out


def _match_fraction(xs, ys):
    """Fraction of routes in xs that have a Jaccard>=MATCH_JACCARD partner in ys."""
    if not xs:
        return 0.0
    matched = 0
    for cx, *_ in xs:
        for cy, *_ in ys:
            inter = len(cx & cy)
            if inter and inter / len(cx | cy) >= MATCH_JACCARD:
                matched += 1
                break
    return matched / len(xs)


def main(use_sample: bool) -> None:
    suffix = "sample" if use_sample else "full"
    methods = {m: _load(m, suffix) for m in ("A", "B", "C")}

    lines = [f"# M16 Cross-Method Comparison (A vs B vs C, {suffix})",
             "",
             "A = clustering (popularity=cluster size); B = maximal-frequent, "
             "C = transition-graph (popularity=trip support).", ""]

    # --- summary per method at a few length configs ---
    lines += ["## Route counts / popularity / longest, per min-length",
              "| method | min_len | #routes | top_popularity | longest_km |",
              "|---|---|---|---|---|"]
    for L in (1, 3, 5, 10):
        for m in ("A", "B", "C"):
            rs = [r for r in methods[m] if r[3] == L]
            if not rs:
                lines.append(f"| {m} | {L} | 0 | - | - |")
                continue
            top_pop = max(r[1] for r in rs)
            longest = max(r[2] for r in rs)
            lines.append(f"| {m} | {L} | {len(rs)} | {top_pop} | {longest:.2f} |")

    # --- cross-method overlap at >=3 km (distinct routes per method) ---
    def distinct(m, L=3):
        seen, out = set(), []
        for cells, pop, length, ml in methods[m]:
            if ml == L and cells not in seen:
                seen.add(cells); out.append((cells, pop, length))
        return out

    dsets = {m: distinct(m) for m in ("A", "B", "C")}
    lines += ["", "## Cross-method overlap at >=3 km "
              f"(fraction of ROW method's routes with a cell-set Jaccard>={MATCH_JACCARD} "
              "match in COL method)",
              "| row\\col | A | B | C |", "|---|---|---|---|"]
    for mr in ("A", "B", "C"):
        cells = [f"{_match_fraction(dsets[mr], dsets[mc]):.2f}" if mr != mc else "1.00"
                 for mc in ("A", "B", "C")]
        lines.append(f"| {mr} | {cells[0]} | {cells[1]} | {cells[2]} |")

    lines += ["", "## Interpretation",
              "- Different lenses, partial overlap: B (maximal-frequent) and C "
              "(dominant-flow) agree most on busy corridors; A (whole-trajectory "
              "clustering) surfaces longer end-to-end corridors that B/C fragment.",
              "- Popularity units differ (cluster size vs trip support) so compare "
              "RANKING and geometry, not raw magnitudes.",
              "- Approx-vs-exact accuracy (Space-Saving/Count-Min) is quantified in "
              "the M7 report (precision@100 1.00/0.92/0.63 at 1/3/5 km)."]

    out_dir = os.path.join(config.OUTPUT_BASE, "statistics")
    os.makedirs(out_dir, exist_ok=True)
    rp = os.path.join(out_dir, f"m16_method_comparison_{suffix}.md")
    with open(rp, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(f"\n[m16] wrote comparison -> {rp}")
    print("M16 METHOD COMPARISON COMPLETE.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    args = ap.parse_args()
    main(use_sample=args.sample)
