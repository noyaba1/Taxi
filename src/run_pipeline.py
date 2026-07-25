"""
run_pipeline.py  --  one-command pipeline orchestrator
=====================================================
Runs the whole pipeline end to end (each stage as its own `python -m src.<stage>`
process, matching how they run individually and on DataProc). Reproducibility:
one command builds every result from the raw sample onward.

    python -m src.run_pipeline --sample                # 5k trips, ~2 minutes
    python -m src.run_pipeline --sample --verify       # + every verifier
    python -m src.run_pipeline --mid                   # 200k trips, real shuffle
    python -m src.run_pipeline --sample --dry-run      # just list the stages
    python -m src.run_pipeline --sample --build-sample # also (re)build the sample

Prereq (unless --build-sample): the sample for that scale already exists. The raw
1.9 GB read (make_sample) is opt-in because it is slow.
"""
import os
import subprocess
import sys
import time

from src import cli

# (label, module, extra args, scales-it-runs-at)
#
# The window-enumeration family -- M5 exact, M6 closed, M8 maximal-frequent, and
# M7's exact baseline -- all build the same O(n^2) support table. Measured on this
# hardware: fine at 5k; at 200k M5 OOMs and M8 spilled 21 GB without finishing.
#
# They run at `sample` only, as the ground truth everything else is checked
# against. Method D (suffix array) carries the same deliverable at scale --
# including the per-length X calibration and the holes analysis -- and produces
# IDENTICAL numbers on the sample from a single pass. Running the quadratic
# family everywhere would mean the pipeline cannot finish at the scale that
# actually matters.
ALL = ("sample", "mid", "full")
STAGES = [
    ("Phase 1  clean", "src.clean_data", [], ALL),
    ("Phase 2  features", "src.feature_engineering", [], ALL),
    ("Phase 2  statistics", "src.summarize_features", [], ALL),
    ("Phase 4  H3 encoding", "src.spatial_encoding", [], ALL),
    ("M5  exact mining (baseline)", "src.route_mining_exact", [], ("sample",)),
    ("M6  closed sub-routes", "src.route_mining_closed", [], ("sample",)),
    ("M7  approx vs exact", "src.route_mining_approx", [], ("sample",)),
    ("M7  approx (sketches only)", "src.route_mining_approx", ["--approx-only"],
     ("mid", "full")),
    ("M12 suffix array (D)", "src.route_mining_suffix_array", [], ALL),
    ("M8  min-support maximal (B)", "src.route_mining_maximal", [], ("sample",)),
    ("M9  clustering (A)", "src.route_mining_clustering", [], ALL),
    ("M10 transition graph (C)", "src.route_mining_graph", [], ALL),
    ("M11 anomalies", "src.anomaly_analysis", [], ALL),
    ("M16 method comparison", "src.evaluation", [], ALL),
    # The only check that uses data the pipeline has never seen. Everything else
    # recounts against the table the mining used, which cannot distinguish a real
    # corridor from a memorised training path.
    ("M17 held-out validation", "src.validate_holdout", [], ALL),
    ("M15 visualization", "src.visualization", [], ALL),
]
VERIFIERS = [
    "src.verify_phase1", "src.verify_encoding", "src.verify_route_mining",
    "src.verify_closed", "src.verify_approx_mining", "src.verify_maximal",
    "src.verify_clustering", "src.verify_graph", "src.verify_anomaly",
]


def _run(module, args, env):
    cmd = [sys.executable, "-m", module, *args]
    t0 = time.time()
    r = subprocess.run(cmd, env=env)
    return r.returncode == 0, time.time() - t0


def main():
    ap = cli.scale_parser(__doc__)
    ap.add_argument("--verify", action="store_true", help="run verifiers after stages")
    ap.add_argument("--dry-run", action="store_true", help="list stages only")
    ap.add_argument("--build-sample", action="store_true", help="rebuild the sample first")
    args = ap.parse_args()
    scale = cli.scale_of(args)
    flag = f"--{scale}"

    stages = [(lbl, mod, extra) for lbl, mod, extra, scales in STAGES
              if scale in scales]
    skipped = [lbl for lbl, _m, _e, scales in STAGES if scale not in scales]
    if args.build_sample:
        if scale == "full":
            raise SystemExit("--build-sample is meaningless with --full")
        stages.insert(0, ("M0  build sample", "src.make_sample", []))

    if skipped:
        print(f"note: not run at scale={scale}: {', '.join(skipped)}")
    print(f"=== pipeline {flag} | {len(stages)} stages"
          f"{' + ' + str(len(VERIFIERS)) + ' verifiers' if args.verify else ''} ===")
    if args.dry_run:
        for i, (label, mod, extra) in enumerate(stages, 1):
            print(f"  {i:>2}. {label:<30} ({mod} {flag} {' '.join(extra)})".rstrip())
        if args.verify:
            print("  verifiers:", ", ".join(v.split(".")[-1] for v in VERIFIERS))
        return

    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    results, t0 = [], time.time()
    for label, mod, extra in stages:
        print(f"\n>>> {label} ({mod})")
        ok, dt = _run(mod, [flag, *extra], env)
        results.append((label, ok, dt))
        print(f"    {'OK' if ok else 'FAILED'} in {dt:.1f}s")
        if not ok:
            print("!!! stage failed; stopping.")
            break

    if args.verify and all(ok for _l, ok, _d in results):
        for mod in VERIFIERS:
            print(f"\n>>> verify {mod}")
            ok, dt = _run(mod, [flag], env)
            results.append((f"verify {mod.split('.')[-1]}", ok, dt))
            print(f"    {'OK' if ok else 'FAILED'} in {dt:.1f}s")

    print("\n" + "=" * 60)
    for label, ok, dt in results:
        print(f"  {'OK  ' if ok else 'FAIL'}  {label:<38} {dt:6.1f}s")
    print(f"  total: {time.time() - t0:.1f}s")
    print("=" * 60)
    sys.exit(0 if all(ok for _l, ok, _d in results) else 1)


if __name__ == "__main__":
    main()
