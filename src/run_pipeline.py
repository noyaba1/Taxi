"""
run_pipeline.py  --  one-command pipeline orchestrator
=====================================================
Runs the whole pipeline end to end (each stage as its own `python -m src.<stage>`
process, matching how they run individually and on DataProc). Reproducibility:
one command builds every result from the encoded data onward.

    python -m src.run_pipeline --sample                # full pipeline
    python -m src.run_pipeline --sample --verify       # + run every verifier
    python -m src.run_pipeline --sample --dry-run      # just list the stages
    python -m src.run_pipeline --sample --build-sample # also (re)build the 5k sample

Prereq (unless --build-sample): the sample/full input already exists. The raw
1.9 GB read (make_sample) is opt-in because it is slow.
"""
import argparse
import os
import subprocess
import sys
import time

# (label, module, needs_flag) executed in order.
STAGES = [
    ("Phase 1  clean", "src.clean_data", True),
    ("Phase 2  features", "src.feature_engineering", True),
    ("Phase 4  H3 encoding", "src.spatial_encoding", True),
    ("M5  exact mining", "src.route_mining_exact", True),
    ("M6  maximal (closed)", "src.route_mining_suffix", True),
    ("M7  approximate", "src.route_mining_approx", True),
    ("M8  min-support maximal", "src.route_mining_maximal", True),
    ("M9  clustering (A)", "src.route_mining_clustering", True),
    ("M10 transition graph (C)", "src.route_mining_graph", True),
    ("M11 anomalies", "src.anomaly_analysis", True),
    ("M16 comparison", "src.evaluation", True),
    ("M15 visualization", "src.visualization", True),
]
VERIFIERS = [
    "src.verify_phase1", "src.verify_encoding", "src.verify_route_mining",
    "src.verify_suffix_mining", "src.verify_approx_mining", "src.verify_maximal",
    "src.verify_clustering", "src.verify_graph", "src.verify_anomaly",
]


def _run(module, flag, env):
    cmd = [sys.executable, "-m", module, flag]
    t0 = time.time()
    r = subprocess.run(cmd, env=env)
    return r.returncode == 0, time.time() - t0


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--sample", action="store_true")
    g.add_argument("--full", action="store_true")
    ap.add_argument("--verify", action="store_true", help="run verifiers after stages")
    ap.add_argument("--dry-run", action="store_true", help="list stages only")
    ap.add_argument("--build-sample", action="store_true", help="rebuild the 5k sample first")
    args = ap.parse_args()
    flag = "--sample" if args.sample else "--full"

    stages = list(STAGES)
    if args.build_sample and args.sample:
        stages.insert(0, ("M0  build sample", "src.make_sample", False))

    print(f"=== pipeline {flag} | {len(stages)} stages"
          f"{' + ' + str(len(VERIFIERS)) + ' verifiers' if args.verify else ''} ===")
    if args.dry_run:
        for i, (label, mod, _f) in enumerate(stages, 1):
            print(f"  {i:>2}. {label:<26} ({mod})")
        if args.verify:
            print("  verifiers:", ", ".join(v.split('.')[-1] for v in VERIFIERS))
        return

    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    results, t0 = [], time.time()
    for label, mod, needs_flag in stages:
        print(f"\n>>> {label} ({mod})")
        ok, dt = _run(mod, flag if needs_flag else ("5000" if mod.endswith("make_sample") else flag), env)
        results.append((label, ok, dt))
        print(f"    {'OK' if ok else 'FAILED'} in {dt:.1f}s")
        if not ok:
            print("!!! stage failed; stopping."); break

    if args.verify and all(ok for _l, ok, _d in results):
        for mod in VERIFIERS:
            print(f"\n>>> verify {mod}")
            ok, dt = _run(mod, flag, env)
            results.append((f"verify {mod.split('.')[-1]}", ok, dt))
            print(f"    {'OK' if ok else 'FAILED'} in {dt:.1f}s")

    print("\n" + "=" * 56)
    for label, ok, dt in results:
        print(f"  {'OK ' if ok else 'FAIL'}  {label:<34} {dt:6.1f}s")
    print(f"  total: {time.time()-t0:.1f}s")
    print("=" * 56)
    sys.exit(0 if all(ok for _l, ok, _d in results) else 1)


if __name__ == "__main__":
    main()
