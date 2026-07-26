#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Porto Taxi -> GCP DataProc submit script (see docs/DATAPROC.md).
#
# Creates a 6-machine cluster (1 master + 5 workers), uploads code and data,
# runs EVERY pipeline stage against gs:// paths, and deletes the cluster.
#
# Results are written straight to gs://<bucket>/porto/outputs -- NOT to the
# master's local disk. That distinction matters: this script deletes the cluster
# on exit, so anything left on a node is destroyed with it. An earlier version
# pointed OUTPUT_BASE at /tmp on the master and lost every top-100 CSV it had
# just spent an hour computing.
#
#     bash scripts/dataproc_submit.sh
# ---------------------------------------------------------------------------
set -euo pipefail

# ---- CONFIG (edit) --------------------------------------------------------
PROJECT="${PROJECT:-your-project-id}"
BUCKET="${BUCKET:-gs://your-bucket}"
REGION="${REGION:-europe-west1}"
CLUSTER="${CLUSTER:-porto}"
SCALE="${SCALE:---full}"          # --sample for a cheap cloud rehearsal first
WORKERS="${WORKERS:-5}"           # the brief asks for >=5 machines in the cluster
DRY_RUN="${DRY_RUN:-0}"           # 1 = check everything, create and bill nothing
# Dataproc 2.2 = Spark 3.5.x + Python 3.11, which matches what this project was
# validated against locally (pyspark 3.5.1 / Python 3.11). Note 2.1 pairs with
# debian11, NOT debian12 -- `gcloud dataproc clusters create` rejects invalid
# combinations, and the accepted list changes over time, hence the override.
IMAGE="${IMAGE:-2.2-debian12}"
# Pinned deps, staged as wheels into the bucket because the cluster has no PyPI
# route (see the wheelhouse block below). PY_VER must match the image's
# interpreter -- Dataproc 2.2 is Python 3.11 -- or the wheels will not install.
PY_VER="${PY_VER:-3.11}"
H3_VER="${H3_VER:-3.7.7}"
DS_VER="${DS_VER:-5.0.2}"
GEOHASH_VER="${GEOHASH_VER:-0.8.5}"
# Default init timeout is 10m. Copying ~2 MB of wheels from GCS takes seconds,
# but a slow node should not roll back the whole cluster.
INIT_TIMEOUT="${INIT_TIMEOUT:-15m}"
# `gcloud dataproc jobs submit` runs the driver in CLIENT mode, on the master.
# DataProc's default driver heap there is ~4g, but evaluation and visualization
# collect to the driver and the local --full run needed SPARK_DRIVER_MEM=10g.
# An n2-standard-4 master has 16g, so 8g leaves room for the YARN/HDFS daemons
# while removing an OOM that would land an hour into a paid run.
DRIVER_MEM="${DRIVER_MEM:-8g}"
PREFIX="${PREFIX:-porto}"         # folder inside the bucket; "porto" is just the
                                  # city the dataset comes from. Cosmetic -- set
                                  # it to anything, or "" to use the bucket root.
# ---------------------------------------------------------------------------

DATA="$BUCKET${PREFIX:+/$PREFIX}"
OUT="$DATA/outputs"

if [[ "$PROJECT" == "your-project-id" || "$BUCKET" == "gs://your-bucket" ]]; then
  echo "ERROR: set PROJECT and BUCKET first, e.g." >&2
  echo "  PROJECT=my-proj BUCKET=gs://my-bucket bash scripts/dataproc_submit.sh" >&2
  exit 2
fi

# ---------------------------------------------------------------------------
# DRY_RUN=1 verifies every precondition that can be checked without spending a
# cent: credentials, APIs, bucket, local input files, and that the code even
# imports. Almost every failed cloud run in this project's history would have
# been caught here. Run it first, every time.
# ---------------------------------------------------------------------------
if [[ "$DRY_RUN" == "1" ]]; then
  echo "== DRY RUN: checking preconditions, creating nothing =="
  fail=0
  chk () { if eval "$2" >/dev/null 2>&1; then echo "  [OK]   $1"; else echo "  [FAIL] $1"; fail=1; fi; }

  chk "gcloud installed"                 "command -v gcloud"
  chk "gsutil installed"                 "command -v gsutil"
  chk "authenticated"                    "gcloud auth list --filter=status:ACTIVE --format='value(account)' | grep -q ."
  chk "project '$PROJECT' reachable"     "gcloud projects describe '$PROJECT'"
  chk "dataproc API enabled"             "gcloud services list --enabled --project '$PROJECT' | grep -q dataproc"
  chk "storage API enabled"              "gcloud services list --enabled --project '$PROJECT' | grep -q storage-component"
  chk "bucket '$BUCKET' exists"          "gsutil ls -b '$BUCKET'"
  # The data may live EITHER on this machine (laptop flow) OR already in the
  # bucket (Cloud Shell flow, where the repo is cloned fresh and has no data).
  # Requiring the local copy would fail the check for a perfectly good setup.
  have_train="python3 -c 'import os,sys; from src import config; sys.exit(0 if os.path.exists(config.RAW_TRAIN) else 1)' || gsutil -q stat '$DATA/raw/train.csv'"
  have_test="python3 -c 'import os,sys; from src import config; sys.exit(0 if os.path.exists(config.RAW_TEST) else 1)' || gsutil -q stat '$DATA/raw/test.csv'"
  chk "train.csv available (local or in bucket)"   "$have_train"
  chk "held-out csv available (local or in bucket)" "$have_test"
  chk "src package imports"              "python3 -c 'import src.config, src.storage, src.run_pipeline'"
  chk "zip available"                    "command -v zip"

  echo
  echo "  scale=$SCALE  workers=$WORKERS  region=$REGION"
  echo "  data  -> $DATA"
  echo "  out   -> $OUT"
  echo
  if [[ "$fail" == "1" ]]; then
    echo "DRY RUN FAILED — fix the [FAIL] lines above before spending budget." >&2
    exit 1
  fi
  echo "DRY RUN PASSED — rerun without DRY_RUN=1 to launch."
  exit 0
fi

gcloud config set project "$PROJECT"

echo "== package + upload code + raw data =="
zip -qr src.zip src -x "*/__pycache__/*"
gsutil cp src.zip "$BUCKET/code/src.zip"
# Resolve the raw file the same way config.py does, so the two cannot drift.
# Upload each input only if it is not already in the bucket. Report where the
# data is ACTUALLY coming from -- printing the local fallback path before knowing
# whether it is needed made a working run look broken.
ensure_input () {  # $1 = object name in the bucket, $2 = local fallback path
  if gsutil -q stat "$DATA/raw/$1"; then
    echo "   $1: already in bucket -> $DATA/raw/$1 (no upload needed)"
  elif [[ -f "$2" ]]; then
    echo "   $1: uploading from $2 ..."
    gsutil -m cp "$2" "$DATA/raw/$1"
  else
    echo "ERROR: $1 is neither in the bucket ($DATA/raw/$1) nor on this machine" >&2
    echo "       ($2). Upload it to the bucket, or set RAW_TRAIN / RAW_TEST." >&2
    exit 1
  fi
}

RAW_LOCAL="$(python3 -c 'from src import config; print(config.RAW_TRAIN)')"
TEST_LOCAL="$(python3 -c 'from src import config; print(config.RAW_TEST)')"
ensure_input train.csv "$RAW_LOCAL"
# The held-out split feeds validate_holdout (the only check on unseen data).
ensure_input test.csv  "$TEST_LOCAL"

# --------------------------------------------------------------------------
# Wheelhouse: the cluster cannot reach PyPI.
#
# Measured, not assumed -- the stock pip-install.sh init action died with
#   [Errno 101] Network is unreachable   ...   /simple/h3/
# on all three nodes. The VMs have no internet egress, which is normal for a
# restricted org VPC. They CAN reach GCS, so we stage platform wheels there from
# this machine (which has internet) and install with --no-index on the nodes.
#
# Wheels are built for the image's interpreter, NOT this laptop's: Dataproc 2.2
# is Python 3.11 on manylinux x86_64, and this script may well be run from an
# arm64 Mac. --no-deps keeps numpy off the list; the image already has a 1.x that
# its pandas/pyarrow are compiled against, and replacing it breaks pandas_udf.
# --------------------------------------------------------------------------
WHEELHOUSE="$DATA/wheels"
echo "== stage dependency wheels (cluster has no PyPI access) =="
if [[ "${FORCE_WHEELS:-0}" != "1" ]] && gsutil -q stat "$WHEELHOUSE/h3-$H3_VER-"*; then
  echo "   wheelhouse already present -> $WHEELHOUSE (FORCE_WHEELS=1 to rebuild)"
else
  WHTMP="$(mktemp -d)"
  python3 -m pip download --no-deps --only-binary=:all: \
    --platform manylinux2014_x86_64 --python-version "$PY_VER" --implementation cp \
    -d "$WHTMP" "h3==$H3_VER" "datasketches==$DS_VER"
  # sdist: optional, and the init action treats a build failure as non-fatal.
  python3 -m pip download --no-deps --no-binary=:all: -d "$WHTMP" \
    "python-geohash==$GEOHASH_VER" || echo "   (python-geohash sdist unavailable; optional)"
  gsutil -q -m cp "$WHTMP"/* "$WHEELHOUSE/"
  rm -rf "$WHTMP"
  echo "   staged -> $WHEELHOUSE"
fi
gsutil -q cp scripts/init_offline_deps.sh "$DATA/scripts/init_offline_deps.sh"

echo "== create cluster (1 master + $WORKERS workers) =="

# Arm the cleanup BEFORE creating anything.
#
# MEASURED: a cluster whose initialization action fails is NOT rolled back.
# Dataproc parks it in state ERROR *with its VMs still RUNNING* so you can read
# the init logs. The failed run on 2026-07-26 left 3 x n2-standard-4 billing
# until they were deleted by hand. This trap used to be installed on the line
# AFTER `clusters create`, which is precisely the case it needed to cover:
# `set -e` aborted the script before the trap ever existed.
#
# `delete` on a nonexistent cluster is a no-op error, hence the `|| true`.
trap 'echo "== deleting cluster =="; \
      gcloud dataproc clusters delete "$CLUSTER" --region "$REGION" -q || true' EXIT

# h3 + datasketches are NOT on a stock DataProc image; install on every node,
# from the GCS wheelhouse rather than PyPI (see above).
# (numpy/pandas/pyarrow ARE preinstalled, so pandas_udf works out of the box.)
gcloud dataproc clusters create "$CLUSTER" --region "$REGION" \
  --master-machine-type n2-standard-4 --num-masters 1 \
  --worker-machine-type n2-standard-4 --num-workers "$WORKERS" \
  --image-version "$IMAGE" --max-idle 30m \
  --initialization-actions "$DATA/scripts/init_offline_deps.sh" \
  --initialization-action-timeout "$INIT_TIMEOUT" \
  --metadata WHEELHOUSE_URI="$WHEELHOUSE" \
  --properties="^;^spark:spark.sql.adaptive.enabled=true;spark:spark.sql.adaptive.skewJoin.enabled=true;spark:spark.sql.shuffle.partitions=400;spark:spark.driver.memory=$DRIVER_MEM;spark:spark.driver.maxResultSize=4g;spark-env:SPARK_ENV=cloud;spark-env:DATA_BASE=$DATA;spark-env:RAW_TRAIN=$DATA/raw/train.csv;spark-env:RAW_TEST=$DATA/raw/test.csv;spark-env:OUTPUT_BASE=$OUT"

# Env for the EXECUTORS. OUTPUT_BASE is a gs:// path: every report and CSV goes
# through src/storage.py, which writes to Hadoop FS when the path has a URI
# scheme.
#
# The DRIVER is handled at cluster-creation time via `spark-env:` properties
# (above), NOT here. `spark.yarn.appMasterEnv.*` looks like the right knob and
# is not: it only reaches the driver in CLUSTER deploy mode, and
# `gcloud dataproc jobs submit` runs the driver in CLIENT mode on the master.
# Setting it here left the driver with no SPARK_ENV and no OUTPUT_BASE at all.
# `spark-env:` writes into /etc/spark/conf/spark-env.sh, which spark-submit
# sources (under `set -a`) for the client-mode driver -- so it works in both
# modes and needs no per-job repetition.
X="spark.executorEnv"
COMMON="SPARK_ENV=cloud,DATA_BASE=$DATA,RAW_TRAIN=$DATA/raw/train.csv,RAW_TEST=$DATA/raw/test.csv,OUTPUT_BASE=$OUT"
ENVPROPS=""
for kv in ${COMMON//,/ }; do
  ENVPROPS="${ENVPROPS:+$ENVPROPS,}$X.$kv"
done

submit () {  # $1 = module file under src/ ; $2.. = extra args
  local mod="$1"; shift
  echo "== submit $mod $SCALE $* =="
  gcloud dataproc jobs submit pyspark "src/$mod" \
    --cluster "$CLUSTER" --region "$REGION" \
    --py-files "$BUCKET/code/src.zip" \
    --properties "$ENVPROPS" \
    -- "$SCALE" "$@"
}

# Every stage the local pipeline runs, in the same order. Omitting any of these
# means a required deliverable is simply never produced by the cloud run:
#   summarize_features -> the stage-1 statistics table
#   route_mining_suffix / _suffix_array -> two of the required method families
#   evaluation -> the cross-method comparison report
#   visualization -> the map
# At every scale EXCEPT --full the pipeline reads a pre-built sample rather than
# the raw CSV (config.dataset_paths: raw_csv = sample_csv_dir(scale) unless the
# scale is "full"). Locally that sample comes from `run_pipeline --build-sample`;
# in the cloud nothing built it, so clean_data died on
#   [PATH_NOT_FOUND] gs://.../taxi/sample/train_sample.csv_dir
# --full reads RAW_TRAIN directly and never needs this, which is exactly why the
# gap could only ever surface in the rehearsal -- the cheap run whose job is to
# find things like this before the expensive one.
if [[ "$SCALE" != "--full" ]]; then
  submit make_sample.py
fi

submit clean_data.py
submit feature_engineering.py
submit summarize_features.py

# The grid/conflation sweep re-encodes the WHOLE dataset once per candidate
# (H3 8/9/10 + geohash 6/7) and adds a distinct-cell count and a bearing-entropy
# groupBy to each. That is five extra full passes to justify a design choice --
# and the justification is qualitatively identical on a sample, where it already
# ran locally and is reported in docs/FINAL_REPORT.md.
#
# It is off in the CLOUD at every scale, deliberately. The sweep is the only
# thing here that imports `geohash`, which is sdist-only and therefore the one
# dependency that can fail to install on an offline node. Keeping it out means
# the --sample rehearsal exercises exactly the dependency set the --full run
# needs -- a rehearsal that tests more than the real thing is a worse rehearsal.
submit spatial_encoding.py

# Deliverable-producing stages FIRST, so a failure in the expensive demonstration
# below cannot cost us the actual results.
submit route_mining_suffix_array.py      # exact + scalable: carries the deliverable
submit route_mining_clustering.py
submit route_mining_graph.py
submit anomaly_analysis.py
submit evaluation.py
submit validate_holdout.py    # generalisation check on unseen trips
submit temporal_analysis.py   # do popular corridors depend on the hour?
submit visualization.py

# Approximate structures LAST. --approx-only skips the exact groupBy, but the
# sketches still stream every enumerated window, which makes this the second most
# expensive stage at full scale. It stays because demonstrating the sketches at
# scale is an assignment requirement -- but by now the deliverables are in GCS.
submit route_mining_approx.py --approx-only

# The window-enumeration family (exact / closed / maximal-frequent) shares one
# O(n^2) support table. It is the ground truth the other methods are validated
# against, and it belongs at sample scale only -- Method D reproduces its output
# exactly, from a single pass, at a fraction of the cost.
# SAMPLE ONLY -- match run_pipeline.STAGES exactly, which lists these three as
# scales=('sample',). Gating on `!= --full` (as this did) submitted them at
# --mid / s400k / s800k too, where they are known to blow up: measured at 200k,
# M5 OOMs and M8 spilled 21 GB without finishing. That made an intermediate-scale
# cloud run cost money to fail on stages the local pipeline correctly skips.
# CI does not catch this: it only asserts that every FULL-scale stage is present.
if [[ "$SCALE" == "--sample" ]]; then
  submit route_mining_exact.py
  submit route_mining_closed.py
  submit route_mining_maximal.py
fi

echo "== done =="
echo "   parquet : $DATA/processed"
echo "   results : $OUT/routes and $OUT/statistics"
gsutil ls "$OUT/routes/" || true
echo "   fetch with: gsutil -m cp -r '$OUT' ./cloud_outputs/"
