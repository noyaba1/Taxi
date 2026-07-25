#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Local dress rehearsal for the DataProc run — free, offline, ~4 minutes.
#
# WHY THIS EXISTS
# ---------------
# The pipeline reaching gs:// is not the same code path as the pipeline reaching
# ./outputs. Anything with a URI scheme goes through the JVM's Hadoop FileSystem
# instead of Python's builtins, and a stage that quietly used open() would work
# perfectly locally and fail on the cluster — after the expensive mining had
# already been paid for.
#
# `file://` IS a Hadoop FileSystem with a URI scheme, so it exercises exactly
# that path without needing credentials, a bucket, or a cent.
#
# This is not hypothetical. Running it the first time found three real blockers:
#   * evaluation and visualization never created a SparkSession, so they had no
#     JVM to reach a remote path with — both produce deliverables;
#   * verify_anomaly read its CSV with a bare open();
#   * validate_holdout's input was never uploaded by the submit script.
#
#     bash scripts/cloud_rehearsal.sh
# ---------------------------------------------------------------------------
set -euo pipefail

PY="${PY:-.venv/bin/python}"
REH="${REH:-/tmp/porto_rehearsal}"
SCALE="${SCALE:---sample}"

echo "== cloud rehearsal: every path behind a URI scheme =="
echo "   scratch : $REH"
echo "   scale   : $SCALE"
rm -rf "$REH"; mkdir -p "$REH"

export DATA_BASE="file://$REH/data"
export OUTPUT_BASE="file://$REH/outputs"
echo "   DATA_BASE   = $DATA_BASE"
echo "   OUTPUT_BASE = $OUTPUT_BASE"
echo

"$PY" -m src.make_sample "$SCALE"
"$PY" -m src.run_pipeline "$SCALE" --verify

echo
echo "== results actually landed behind the URI? =="
ROUTES="$REH/outputs/routes"
STATS="$REH/outputs/statistics"
for d in "$ROUTES" "$STATS"; do
  if [[ ! -d "$d" ]]; then
    echo "  [FAIL] $d does not exist — writes went somewhere else" >&2
    exit 1
  fi
done
n_csv=$(find "$ROUTES" -name '*.csv' | wc -l | tr -d ' ')
n_md=$(find "$STATS" -name '*.md' | wc -l | tr -d ' ')
echo "  route CSVs : $n_csv"
echo "  reports    : $n_md"
[[ "$n_csv" -ge 5 && "$n_md" -ge 8 ]] || { echo "  [FAIL] too few artefacts" >&2; exit 1; }

# The bug this whole exercise is about: a literal 'gs:'-style directory created
# next to the project because something called open() on a URI.
if [[ -d "gs:" || -d "file:" ]]; then
  echo "  [FAIL] a literal URI-named directory was created — a stage bypassed src/storage" >&2
  exit 1
fi
echo "  [OK]   no literal URI-named directories leaked into the working tree"

echo
echo "REHEARSAL PASSED — the pipeline works end to end with URI-scheme paths."
echo "Next: DRY_RUN=1 bash scripts/dataproc_submit.sh, then the --sample cloud run."
