"""
experiment_cluster_scaling.py  --  does adding machines actually make it faster?
===============================================================================
The other two experiments vary the DATA on one machine. This one varies the
MACHINE COUNT on fixed data, which is the question a cloud-computing deliverable
is actually about and the only one that cannot be answered on a laptop.

WHERE THE DATA COMES FROM
-------------------------
Nothing here runs Spark. Every cloud run already appends one row per stage to
`outputs/statistics/timings.jsonl` with its wall time, and `cli._cluster_tags`
stamps each row with the `cluster_workers` and `run_id` that produced it. So a
sweep

    for W in 2 5 10 16; do ... WORKERS=$W bash scripts/dataproc_submit.sh; done

leaves a complete strong-scaling dataset behind as a side effect. This reads it.
That matters because the measurement is the expensive part: re-deriving a
speedup curve means paying for four more clusters, so the numbers are recovered
from runs already bought rather than regenerated.

WHAT IT REPORTS
---------------
  speedup      T(baseline) / T(W). The baseline is the SMALLEST cluster in the
               data, not an assumed serial time -- nobody runs 1.71M trips on
               one core, so a "true" T(1) would be extrapolated fiction.
  efficiency   speedup / (W / W_baseline). 1.0 = perfect scaling. This is the
               number that decides whether the next cluster size is worth buying.
  Amdahl       the serial fraction implied by each measurement, solved from
               p = (1/S - 1) / (1/n - 1) where n = W/W_baseline. A pipeline with
               a fixed floor -- cluster creation, ~13 sequential job submissions,
               driver-side collects -- cannot beat 1/p however many machines it
               gets, and printing that ceiling is more useful than printing the
               speedup alone.

The conclusions are derived from the rows, never asserted: this module has no
sentence in it that survives the numbers changing.

Run:
    python -m src.experiment_cluster_scaling
    python -m src.experiment_cluster_scaling --timings ./cloud_outputs/statistics/timings.jsonl
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone

from src import cli, storage

log = cli.setup_logging("cluster-scaling")

# Per-VM on-demand rate used for the cost column. A rate, not a bill: it ignores
# sustained-use discounts and the Dataproc premium, so it is labelled an estimate
# everywhere it appears and never presented as what was actually charged.
DEFAULT_VM_USD_HR = 0.1946      # n2-standard-4, europe-west1, on demand
DATAPROC_USD_VCPU_HR = 0.01     # Dataproc surcharge
VCPU_PER_VM = 4


def load_rows(path: str) -> list[dict]:
    """Parse timings.jsonl, skipping anything malformed rather than dying."""
    text = storage.read_text(path)
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            log.warning("skipping unparseable timings line")
    return rows


def group_runs(rows: list[dict], scale: str) -> dict:
    """
    Collapse stage rows into one record per cloud run.

    Keyed by run_id when present, so two sweeps at the same worker count stay
    separate. Rows without `cluster_workers` are laptop runs and are dropped --
    a local wall time is not comparable to a cluster one and averaging them in
    would quietly corrupt the baseline.
    """
    runs: dict = defaultdict(lambda: {"stages": {}, "workers": None, "failed": 0})
    for r in rows:
        if r.get("scale") != scale or "cluster_workers" not in r:
            continue
        key = r.get("run_id") or f"w{r['cluster_workers']}"
        run = runs[key]
        run["workers"] = r["cluster_workers"]
        if not r.get("ok", True):
            run["failed"] += 1
        # A stage re-run inside one run_id overwrites: the later timing is the
        # one that produced the surviving output.
        run["stages"][r["stage"]] = float(r.get("wall_s") or 0.0)
    return dict(runs)


def amdahl_serial_fraction(speedup: float, n: float) -> float | None:
    """
    Serial fraction p implied by observing `speedup` on `n` times the machines.

    This is the Karp-Flatt metric. Inverting S = 1 / (p + (1-p)/n) gives

        p = (1/S - 1/n) / (1 - 1/n)

    NOT `(1/S - 1) / (1/n - 1)`, which is what this function computed first and
    which is wrong in a way worth recording: on a simulation seeded with a known
    3.9% serial fraction it reported **96.2%** and therefore an Amdahl ceiling of
    1.0x -- while the very same table showed a measured 6.3x speedup. A report
    that contradicts its own numbers in adjacent rows is the exact failure mode
    this project has been bitten by before, and only a test with a KNOWN answer
    catches it: every individual number looked plausible in isolation.

    Returns None where the algebra has nothing to say -- n == 1, or a superlinear
    speedup, which is real (cache and memory effects) but is not an Amdahl regime
    and must not be reported as a negative p.
    """
    if n <= 1 or speedup <= 0:
        return None
    p = (1.0 / speedup - 1.0 / n) / (1.0 - 1.0 / n)
    return p if 0.0 <= p <= 1.0 else None


def drop_partial(runs: dict) -> tuple[dict, list]:
    """
    Keep only runs that completed the full stage list.

    A run whose cluster died half way finishes FASTER than a complete one, so
    left in the table it reads as a faster cluster. Measured: an aborted
    5-worker run reported 3.01x speedup at parallel efficiency 1.20 -- superlinear,
    from having done half the work. Stage count is the discriminator because
    every complete run at a given scale runs the same stages.
    """
    if not runs:
        return runs, []
    full = max(len(r["stages"]) for r in runs.values())
    keep = {k: r for k, r in runs.items() if len(r["stages"]) == full}
    dropped = [(k, len(r["stages"]), full)
               for k, r in runs.items() if len(r["stages"]) != full]
    return keep, dropped


def build_report(runs: dict, scale: str, vm_usd_hr: float) -> list[str]:
    runs, dropped = drop_partial(runs)
    ordered = sorted(runs.items(), key=lambda kv: (kv[1]["workers"], kv[0]))
    lines = ["# Strong scaling: fixed data, varying cluster size",
             f"_generated: {datetime.now(timezone.utc).isoformat(timespec='seconds')}_",
             ""]

    if len(ordered) < 2:
        lines += [
            f"**Not enough data.** Found {len(ordered)} cluster-tagged run(s) at "
            f"scale `{scale}`; a speedup curve needs at least two different "
            "worker counts.",
            "",
            "Each cloud run appends its stages to `timings.jsonl` tagged with "
            "`cluster_workers`, so the sweep below produces everything this "
            "report needs:",
            "",
            "```bash",
            "for W in 2 5 10 16; do",
            "  SCALE=--full WORKERS=$W CLUSTER=porto-w$W \\",
            "    bash scripts/dataproc_submit.sh",
            "done",
            "```",
        ]
        return lines

    base_key, base = ordered[0]
    base_total = sum(base["stages"].values())
    base_w = base["workers"]

    lines += [f"Workload held fixed at scale `{scale}`. Baseline is the smallest "
              f"cluster measured ({base_w} workers), because a single-core time "
              f"for this dataset was never measured and extrapolating one would "
              f"invent the very number the study is about.",
              "",
              "| workers | VMs | wall (s) | wall | speedup | efficiency | "
              "implied serial % | est. $ |",
              "|---|---|---|---|---|---|---|---|"]

    rows_for_conclusion = []
    for key, run in ordered:
        w = run["workers"]
        total = sum(run["stages"].values())
        if total <= 0:
            continue
        vms = w + 1
        speedup = base_total / total
        n = w / base_w if base_w else 1.0
        eff = speedup / n if n else float("nan")
        p = amdahl_serial_fraction(speedup, n)
        usd = (total / 3600.0) * (vms * vm_usd_hr + vms * VCPU_PER_VM * DATAPROC_USD_VCPU_HR)
        mins = f"{int(total // 60)}m{int(total % 60):02d}s"
        flag = "" if not run["failed"] else f" ⚠️{run['failed']} stage(s) failed"
        lines.append(
            f"| {w} | {vms} | {total:,.0f} | {mins} | {speedup:.2f}x | {eff:.2f} | "
            f"{'—' if p is None else f'{100 * p:.1f}%'} | ${usd:.2f}{flag} |")
        rows_for_conclusion.append((w, total, speedup, eff, p, usd))

    lines += ["", "`efficiency` = speedup / (workers relative to baseline). "
                  "1.00 is perfect scaling; the cost column is an on-demand "
                  "estimate (VM rate + Dataproc surcharge), not the billed "
                  "amount.", ""]
    if dropped:
        lines += [f"Excluded {len(dropped)} incomplete run(s) — a cluster that died "
                  f"part-way finishes faster than one that did the work, so left in "
                  f"they read as fast clusters: " +
                  ", ".join(f"`{k}` ({n} of {f} stages)" for k, n, f in dropped) + ".",
                  ""]

    lines += _conclusions(rows_for_conclusion)
    lines += _per_stage(ordered, base_key)
    return lines


def _conclusions(rows: list[tuple]) -> list[str]:
    """Read the conclusion off the measurements; assert nothing they don't show."""
    if len(rows) < 2:
        return []
    out = ["## What the numbers say", ""]

    best_w, _, best_s, _, _, _ = max(rows, key=lambda r: r[2])
    out.append(f"- **Fastest measured:** {best_w} workers at **{best_s:.2f}x** the "
               f"baseline.")

    # Where efficiency crosses below half: the point where machines stop paying.
    knee = next((r for r in rows[1:] if r[3] < 0.5), None)
    if knee:
        out.append(f"- **Efficiency falls below 0.50 at {knee[0]} workers** "
                   f"({knee[3]:.2f}) — past this point each added machine returns "
                   f"less than half a machine's worth of speed.")
    else:
        out.append("- **Efficiency stayed at or above 0.50 across every size "
                   "measured**, so the sweep did not reach the point where "
                   "adding machines stops paying; the knee is beyond the "
                   "largest cluster tested.")

    ps = [r[4] for r in rows if r[4] is not None]
    if ps:
        p = sum(ps) / len(ps)
        out.append(f"- **Implied serial fraction ≈ {100 * p:.1f}%** (mean over "
                   f"{len(ps)} comparison(s)). Amdahl caps this pipeline at "
                   f"**{1 / p:.1f}x** however many machines it is given — the "
                   f"floor is cluster creation plus ~13 sequential job "
                   f"submissions, which no amount of hardware parallelises.")

    cheap = min(rows, key=lambda r: r[5])
    fast = min(rows, key=lambda r: r[1])
    if cheap[0] == fast[0]:
        out.append(f"- **{cheap[0]} workers is both the fastest and the cheapest** "
                   f"run measured (${cheap[5]:.2f}) — over this range more "
                   f"machines bought time without costing more.")
    else:
        out.append(f"- **Cheapest was {cheap[0]} workers (${cheap[5]:.2f}); "
                   f"fastest was {fast[0]} workers (${fast[5]:.2f}).** The "
                   f"${fast[5] - cheap[5]:.2f} difference is what the extra "
                   f"speed cost, which is the trade a budgeted project has to "
                   f"make explicitly.")
    out.append("")
    return out


def _per_stage(ordered: list, base_key: str) -> list[str]:
    """Per-stage speedup: which stages parallelise and which are fixed cost."""
    base = ordered[0][1]
    names = sorted({s for _k, r in ordered for s in r["stages"]})
    if not names:
        return []
    out = ["## Per-stage wall time (s)", "",
           "| stage | " + " | ".join(f"{r['workers']}w" for _k, r in ordered) +
           " | speedup |", "|---" * (len(ordered) + 2) + "|"]
    for name in names:
        cells = [r["stages"].get(name) for _k, r in ordered]
        b, last = base["stages"].get(name), cells[-1]
        sp = f"{b / last:.2f}x" if b and last else "—"
        out.append(f"| {name} | " +
                   " | ".join("—" if c is None else f"{c:,.0f}" for c in cells) +
                   f" | {sp} |")
    out += ["", "A stage whose time barely moves is a fixed cost (driver-side "
                "work, job submission latency); one that falls with the cluster "
                "is genuinely distributed. The mix of the two is what the "
                "serial fraction above is made of.", ""]
    return out


def main(timings: str, scale: str, vm_usd_hr: float) -> None:
    rows = load_rows(timings)
    runs = group_runs(rows, scale)
    log.info("read %s timing rows -> %s cluster-tagged run(s) at scale %s",
             f"{len(rows):,}", len(runs), scale)
    for key, r in sorted(runs.items(), key=lambda kv: kv[1]["workers"]):
        log.info("  %-28s workers=%-3s stages=%-3s wall=%.0fs",
                 key, r["workers"], len(r["stages"]), sum(r["stages"].values()))

    lines = build_report(runs, scale, vm_usd_hr)
    p = storage.write_lines(
        storage.out_path("statistics", f"cluster_scaling_{scale}.md"), lines)
    log.info("wrote report -> %s", p)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--timings",
                    default=storage.out_path("statistics", "timings.jsonl"),
                    help="timings.jsonl to read (local path or gs:// URI)")
    ap.add_argument("--scale", default="full",
                    help="which workload's rows to compare (default: full)")
    ap.add_argument("--vm-usd-hr", type=float, default=DEFAULT_VM_USD_HR,
                    help=f"per-VM on-demand rate (default {DEFAULT_VM_USD_HR})")
    args = ap.parse_args()
    main(args.timings, args.scale, args.vm_usd_hr)
