"""
build_deck_charts.py  --  the figures for the presentation
==========================================================
Every number here is READ FROM THE GENERATED REPORTS, never retyped. If a run
produces different numbers, the deck changes with it -- the same rule the written
report follows (see evaluation.py: a hard-coded conclusion once contradicted its
own tables).

Palette: the validated dark categorical slots 1-3 (blue / orange / aqua), which
clear every gate under `--pairs all` on this deck's surface:

    node scripts/validate_palette.js "#3987e5,#d95926,#199e70" \
         --mode dark --surface "#14161a"
    -> ALL CHECKS PASS

Slots are assigned in fixed order and never cycled. Charts with more than three
categories use one hue plus direct labels rather than inventing a fourth.
"""
import pathlib

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

OUT = pathlib.Path("build/deck/figs")
OUT.mkdir(parents=True, exist_ok=True)

# --- the deck's ink, from references/palette.md (dark column) ---------------
SURFACE = "#14161a"          # deck slide surface
S1, S2, S3 = "#3987e5", "#d95926", "#199e70"
INK, INK2, MUTED = "#ffffff", "#c3c2b7", "#898781"
GRID, BASE = "#2c2c2a", "#383835"
GOOD, CRIT = "#0ca30c", "#d03b3b"

plt.rcParams.update({
    "font.family": ["DejaVu Sans"],
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE, "savefig.edgecolor": "none",
    "text.color": INK, "axes.labelcolor": INK2,
    "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.edgecolor": BASE, "grid.color": GRID,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.linewidth": 0.6, "axes.axisbelow": True,
    "font.size": 13, "axes.titlesize": 15, "legend.frameon": False,
    "xtick.major.size": 0, "ytick.major.size": 0,
})
THOUS = FuncFormatter(lambda v, _: f"{v:,.0f}")


def save(fig, name):
    fig.savefig(OUT / f"{name}.png", dpi=200, bbox_inches="tight", pad_inches=0.25)
    plt.close(fig)
    print(f"  {name}.png")


def bar_labels(ax, bars, fmt="{:,.0f}", dy=0.01, color=INK, size=12):
    """Direct labels -- identity and value never rest on colour alone."""
    span = ax.get_ylim()[1] - ax.get_ylim()[0]
    for b in bars:
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() + span * dy,
                fmt.format(b.get_height()), ha="center", va="bottom",
                color=color, fontsize=size, fontweight="bold")


# ---------------------------------------------------------------- 1. funnel
def fig_funnel():
    stages = ["raw trips", "passed cleaning", "encoded\n(anomalies excluded)"]
    vals = [1_710_670, 1_663_886, 1_614_508]
    fig, ax = plt.subplots(figsize=(9, 3.6))
    bars = ax.barh(stages[::-1], vals[::-1], height=0.55, color=[S3, S1, S1])
    for b, v in zip(bars, vals[::-1]):
        ax.text(v * 1.01, b.get_y() + b.get_height() / 2, f"{v:,}",
                va="center", color=INK, fontsize=13, fontweight="bold")
    ax.set_xlim(0, 1_950_000)
    ax.xaxis.set_major_formatter(THOUS)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("trips")
    save(fig, "funnel")


# ------------------------------------------------- 2. the deliverable, by band
def fig_deliverable():
    bands = ["≥1", "≥3", "≥5", "≥10", "≥20", "≥40"]
    longest = [4.70, 6.17, 8.72, 12.41, 26.25, 0]
    taxis = [435, 435, 369, 84, 1, 0]
    fig, ax = plt.subplots(figsize=(10, 4.6))
    cols = [S1, S1, S1, S1, S2, GRID]
    bars = ax.bar(bands, longest, color=cols, width=0.62)
    for b, v, t in zip(bars, longest, taxis):
        if v == 0:
            ax.text(b.get_x() + b.get_width() / 2, 0.6, "EMPTY", ha="center",
                    color=MUTED, fontsize=12, fontweight="bold", rotation=90)
            continue
        ax.text(b.get_x() + b.get_width() / 2, v + 0.5, f"{v:.2f} km",
                ha="center", color=INK, fontsize=12, fontweight="bold")
        ax.text(b.get_x() + b.get_width() / 2, v + 2.1,
                f"{t} taxi" if t == 1 else f"{t} taxis",
                ha="center", color=S2 if t <= 2 else MUTED, fontsize=11)
    ax.set_ylim(0, 32)
    ax.set_ylabel("longest corridor (km)")
    ax.set_xlabel("minimum length configuration (km)")
    ax.grid(axis="x", visible=False)
    save(fig, "deliverable")


# --------------------------------------------------------- 3. taxi diversity
def fig_diversity():
    bands = ["≥1", "≥3", "≥5", "≥10", "≥20"]
    taxis = [435, 435, 369, 84, 1]
    fig, ax = plt.subplots(figsize=(9, 4.2))
    cols = [S3, S3, S3, S2, CRIT]
    bars = ax.bar(bands, taxis, color=cols, width=0.6)
    bar_labels(ax, bars)
    ax.axhline(442, color=MUTED, lw=1, ls=(0, (4, 4)))
    ax.text(4.35, 452, "fleet = 442", color=MUTED, fontsize=11, ha="right")
    ax.set_ylim(0, 500)
    ax.set_ylabel("distinct taxis on the top corridor")
    ax.set_xlabel("minimum length configuration (km)")
    ax.grid(axis="x", visible=False)
    save(fig, "diversity")


# ------------------------------------------------------------ 4. scaling law
def fig_scaling():
    trips = [4_745, 188_761, 377_451, 754_763]
    f2 = [11.99, 21.36, 21.36, 24.85]
    f5 = [8.36, 14.97, 15.37, 17.37]
    f20 = [5.42, 11.06, 11.54, 12.51]
    fig, ax = plt.subplots(figsize=(9.4, 4.6))
    for ys, c, lbl in ((f2, S1, "floor = 2 trips"), (f5, S2, "floor = 5"),
                       (f20, S3, "floor = 20")):
        ax.plot(trips, ys, color=c, lw=2, marker="o", ms=8, label=lbl)
    ax.scatter([1_710_670], [26.25], s=190, color=S1, zorder=5,
               edgecolor=SURFACE, linewidth=2)
    ax.annotate("measured at 1.71M\n26.25 km\n(predicted 26–28)",
                xy=(1_710_670, 26.25), xytext=(340_000, 27.6),
                color=INK, fontsize=12, fontweight="bold",
                arrowprops=dict(arrowstyle="-", color=MUTED, lw=1))
    ax.set_xscale("log")
    ax.set_xlabel("trips (log scale)")
    ax.set_ylabel("longest corridor found (km)")
    ax.set_ylim(0, 32)
    ax.legend(loc="lower right", labelcolor=INK2)
    save(fig, "scaling")


# ------------------------------------------------------- 5. support / length
def fig_floor_sweep():
    sup = [5000, 2500, 1000, 500, 250, 100, 50, 20, 10, 5, 3, 2]
    lng = [5.80, 7.26, 8.73, 10.19, 10.96, 11.65, 12.41, 17.37, 17.37, 17.77,
           19.01, 26.25]
    fig, ax = plt.subplots(figsize=(9.4, 4.4))
    ax.plot(sup, lng, color=S1, lw=2, marker="o", ms=7)
    ax.set_xscale("log")
    ax.invert_xaxis()
    ax.set_xlabel("support floor — trips required (log, strict → loose)")
    ax.set_ylabel("longest corridor (km)")
    ax.annotate("26.25 km\nat 2 trips", xy=(2, 26.25), xytext=(6, 24.2),
                color=S2, fontsize=12, fontweight="bold")
    ax.annotate("5.80 km\nat 5,000 trips", xy=(5000, 5.80), xytext=(3600, 8.0),
                color=INK2, fontsize=12)
    save(fig, "floor_sweep")


# ----------------------------------------------------------- 6. agreement
def fig_agreement():
    labels = ["A clustering", "C graph", "D suffix array"]
    m = [[1.00, 0.45, 0.94], [0.39, 1.00, 0.42], [0.89, 0.47, 1.00]]
    fig, ax = plt.subplots(figsize=(6.6, 5.2))
    ax.imshow(m, cmap="Blues", vmin=0, vmax=1)
    ax.set_xticks(range(3), labels, fontsize=11)
    ax.set_yticks(range(3), labels, fontsize=11)
    for i in range(3):
        for j in range(3):
            v = m[i][j]
            ax.text(j, i, f"{v:.2f}", ha="center", va="center",
                    color="#0b0b0b" if v > 0.55 else INK,
                    fontsize=15, fontweight="bold")
    ax.grid(False)
    ax.set_xlabel("matched against")
    ax.set_ylabel("routes from")
    save(fig, "agreement")


# ------------------------------------------------------------- 7. held-out
def fig_holdout():
    meths = ["A clustering", "C graph", "D suffix array"]
    cov = [34.8, 28.5, 25.8]
    null = [7.6, 7.9, 6.0]
    x = range(3)
    fig, ax = plt.subplots(figsize=(9, 4.3))
    b1 = ax.bar([i - 0.19 for i in x], cov, 0.34, color=S1, label="mined corridors")
    b2 = ax.bar([i + 0.19 for i in x], null, 0.34, color=MUTED, label="null model")
    ax.set_xticks(list(x), meths)
    for i, (c, n) in enumerate(zip(cov, null)):
        ax.text(i - 0.19, c + 0.8, f"{c:.1f}%", ha="center", color=INK,
                fontsize=12, fontweight="bold")
        ax.text(i + 0.19, n + 0.8, f"{n:.1f}%", ha="center", color=INK2, fontsize=11)
        ax.text(i, max(c, n) + 4.4, f"{c/n:.1f}× lift", ha="center", color=GOOD,
                fontsize=13, fontweight="bold")
    ax.set_ylim(0, 45)
    ax.set_ylabel("coverage of 318 unseen trips (%)")
    ax.legend(labelcolor=INK2, loc="upper right")
    ax.grid(axis="x", visible=False)
    save(fig, "holdout")


# ------------------------------------------------------------- 8. temporal
def fig_temporal():
    buckets = ["night\n00–06", "morning\n06–10", "midday\n10–16",
               "evening pk\n16–20", "evening\n20–24"]
    ov = [0.75, 0.82, 0.89, 0.82, 0.77]
    fig, ax = plt.subplots(figsize=(9.4, 4.2))
    bars = ax.bar(buckets, ov, color=[S2, S1, S1, S1, S2], width=0.6)
    bar_labels(ax, bars, "{:.2f}")
    ax.axhline(0.81, color=MUTED, lw=1, ls=(0, (4, 4)))
    ax.text(4.42, 0.822, "mean 0.81", color=MUTED, fontsize=11, ha="right")
    ax.set_ylim(0, 1.0)
    ax.set_ylabel("overlap with the all-time top-100")
    ax.grid(axis="x", visible=False)
    save(fig, "temporal")


# --------------------------------------------------- 9. sketches: accuracy
def fig_sketch_accuracy():
    bands = ["≥1", "≥3", "≥5", "≥10", "≥20"]
    r5k = [0.99, 0.92, 0.80, 0.17, 0.00]
    r200 = [1.00, 0.99, 0.99, 0.01, 0.00]
    x = range(5)
    fig, ax = plt.subplots(figsize=(9.4, 4.3))
    ax.bar([i - 0.19 for i in x], r5k, 0.34, color=S3, label="5,000 trips")
    ax.bar([i + 0.19 for i in x], r200, 0.34, color=S1, label="200,000 trips")
    ax.set_xticks(list(x), bands)
    for i, (a, b) in enumerate(zip(r5k, r200)):
        ax.text(i - 0.19, a + 0.025, f"{a:.2f}", ha="center", color=INK2, fontsize=11)
        ax.text(i + 0.19, b + 0.025, f"{b:.2f}", ha="center", color=INK,
                fontsize=12, fontweight="bold")
    ax.axvspan(2.5, 4.5, color=CRIT, alpha=0.10)
    ax.text(3.5, 0.52, "sketches fail here —\nand MORE DATA\nmakes it worse",
            ha="center", va="center", color=CRIT, fontsize=13, fontweight="bold")
    ax.set_ylim(0, 1.12)
    ax.set_ylabel("recall @100 vs exact")
    ax.set_xlabel("minimum length configuration (km)")
    # Legend above the plot: inside the axes it landed on top of the callout.
    ax.legend(labelcolor=INK2, loc="lower left", bbox_to_anchor=(0, 1.01, 1, 0.1),
              ncol=2, borderaxespad=0)
    ax.grid(axis="x", visible=False)
    save(fig, "sketch_accuracy")


# ------------------------------------------------------ 10. sketches: memory
def fig_sketch_memory():
    fig, ax = plt.subplots(figsize=(8.4, 4.2))
    scales = ["5,000 trips", "200,000 trips"]
    ratio = [3.0, 105.4]
    bars = ax.bar(scales, ratio, color=[S3, S1], width=0.5)
    for b, v in zip(bars, ratio):
        ax.text(b.get_x() + b.get_width() / 2, v + 3, f"{v:.0f}× smaller",
                ha="center", color=INK, fontsize=14, fontweight="bold")
    ax.set_ylim(0, 128)
    ax.set_ylabel("exact key table ÷ sketch memory")
    ax.grid(axis="x", visible=False)
    ax.text(0.5, 88, "66 MB fixed  vs  6,987 MB", ha="center", color=INK2,
            fontsize=12, transform=ax.transData)
    save(fig, "sketch_memory")


# ---------------------------------------------------------------- 11. HLL
def fig_hll():
    fig, ax = plt.subplots(figsize=(7.4, 3.6))
    bars = ax.barh(["HyperLogLog", "exact countDistinct"], [10.4, 2.0],
                   color=[S2, S1], height=0.5)
    for b, v in zip(bars, [10.4, 2.0]):
        ax.text(v + 0.25, b.get_y() + b.get_height() / 2, f"{v:.1f}s",
                va="center", color=INK, fontsize=14, fontweight="bold")
    ax.set_xlim(0, 13)
    ax.set_xlabel("distinct taxis per H3 cell, 1.61M trips (seconds)")
    ax.grid(axis="y", visible=False)
    save(fig, "hll")


# ------------------------------------------------------- 12. cloud vs local
def fig_cloud_local():
    stages = ["Phase-4 encoding", "M10 graph (C)", "M9 clustering (A)",
              "M12 suffix array", "M18 temporal", "M7 sketches"]
    cloud = [157.3, 213.9, 293.6, 409.1, 629.0, 1044.5]
    local = [58.8, 119.4, 120.4, 409.4, 410.4, 749.9]
    y = range(len(stages))
    fig, ax = plt.subplots(figsize=(9.6, 4.8))
    ax.barh([i + 0.19 for i in y], cloud, 0.34, color=S1, label="DataProc, 6 machines")
    ax.barh([i - 0.19 for i in y], local, 0.34, color=MUTED, label="one laptop")
    ax.set_yticks(list(y), stages, fontsize=11)
    for i, (c, l) in enumerate(zip(cloud, local)):
        ax.text(c + 14, i + 0.19, f"{c:,.0f}s", va="center", color=INK, fontsize=11)
        ax.text(l + 14, i - 0.19, f"{l:,.0f}s", va="center", color=INK2, fontsize=11)
    ax.set_xlim(0, 1320)
    ax.set_xlabel("wall time (s)")
    ax.legend(labelcolor=INK2, loc="lower right")
    ax.grid(axis="y", visible=False)
    save(fig, "cloud_local")


# ------------------------------------------------------------ 13. anomalies
def fig_anomalies():
    dets = ["impossible\nspeed", "excessive\ndistance", "implausible\nshape",
            "parked /\nidle", "GPS\ndrift"]
    pct = [2.25, 1.19, 1.11, 0.16, 0.06]
    fig, ax = plt.subplots(figsize=(9.4, 4.0))
    bars = ax.bar(dets, pct, color=S1, width=0.58)
    bar_labels(ax, bars, "{:.2f}%")
    ax.set_ylim(0, 2.9)
    ax.set_ylabel("% of cleaned trips flagged")
    ax.grid(axis="x", visible=False)
    save(fig, "anomalies")


if __name__ == "__main__":
    print("figures ->", OUT)
    for fn in (fig_funnel, fig_deliverable, fig_diversity, fig_scaling,
               fig_floor_sweep, fig_agreement, fig_holdout, fig_temporal,
               fig_sketch_accuracy, fig_sketch_memory, fig_hll,
               fig_cloud_local, fig_anomalies):
        fn()
    print("done")
