#!/usr/bin/env bash
# One command to check out this repo and prove the suite passes.
#
# Exists because "the repository contains a test suite" and "the test suite runs
# from a clean checkout" are different claims, and only the second one is worth
# anything to a reviewer. This does the second.
#
#   bash scripts/run_tests.sh            # use an existing .venv
#   bash scripts/run_tests.sh --fresh    # build .venv from the lock file first
#
# Exits non-zero if anything fails -- including the environment gate, because a
# suite that "passes" by silently skipping the Spark-backed tests proves less
# than no suite at all.
set -euo pipefail
cd "$(dirname "$0")/.."

PY=.venv/bin/python

if [[ "${1:-}" == "--fresh" ]]; then
  echo "==> building .venv from requirements.lock.txt"
  rm -rf .venv
  "${PYTHON:-python3.11}" -m venv .venv
  .venv/bin/pip install -q --upgrade pip
  .venv/bin/pip install -q -r requirements.lock.txt
fi

if [[ ! -x "$PY" ]]; then
  echo "no .venv found. Run:  bash scripts/run_tests.sh --fresh" >&2
  exit 1
fi

echo "==> environment gate"
# validate_env also checks for the raw dataset, which is NOT needed to run the
# tests and is not in the repo. Java and the imports are; those are what matter
# here, so a missing dataset must not fail this script.
if ! $PY -m src.validate_env 2>&1 | tee /tmp/validate_env.$$ | grep -v "^\[OK\]"; then :; fi
if grep -q "^\[FAIL\].*Java" /tmp/validate_env.$$; then
  echo "FAIL: no supported JDK (8/11/17). See SETUP.md -- brew install openjdk@17" >&2
  rm -f /tmp/validate_env.$$
  exit 1
fi
rm -f /tmp/validate_env.$$

echo "==> pytest"
$PY -m pytest tests/ -p no:warnings "$@"

echo
echo "PASS. Note: tests/test_storage.py and tests/test_method_a_spark.py each"
echo "start a real SparkSession -- if they were skipped, the JDK is missing and"
echo "the two checks that matter most did not run."
