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
# The init action pip-installs three packages on every node. The DEFAULT timeout
# is 10 minutes, and python-geohash ships as an sdist only -- it compiles a C++
# extension on each node -- so a cold pip resolve plus a build can exceed it and
# the whole cluster creation is rolled back. 20m costs nothing when pip is fast.
INIT_TIMEOUT="${INIT_TIMEOUT:-20m}"
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

echo "== create cluster (1 master + $WORKERS workers) =="
# h3 + datasketches are NOT on a stock DataProc image; install on every node.
# (numpy/pandas/pyarrow ARE preinstalled on 2.1, so pandas_udf works.)
gcloud dataproc clusters create "$CLUSTER" --region "$REGION" \
  --master-machine-type n2-standard-4 --num-masters 1 \
  --worker-machine-type n2-standard-4 --num-workers "$WORKERS" \
  --image-version "$IMAGE" --max-idle 30m \
  --initialization-actions "gs://goog-dataproc-initialization-actions-$REGION/python/pip-install.sh" \
  --initialization-action-timeout "$INIT_TIMEOUT" \
  --metadata PIP_PACKAGES="h3==3.7.7 datasketches==5.0.2 python-geohash==0.8.5" \
  --properties spark:spark.sql.adaptive.enabled=true,spark:spark.sql.adaptive.skewJoin.enabled=true,spark:spark.sql.shuffle.partitions=400

# Auto-delete the cluster on ANY exit (success, failure, Ctrl-C) -> budget-safe.
# Safe to do unconditionally now that outputs live in GCS, not on the master.
trap 'echo "== deleting cluster =="; gcloud dataproc clusters delete "$CLUSTER" --region "$REGION" -q' EXIT

# Env for driver (appMaster) AND executors. OUTPUT_BASE is a gs:// path: every
# report and CSV goes through src/storage.py, which writes to Hadoop FS when the
# path has a URI scheme.
E="spark.yarn.appMasterEnv"; X="spark.executorEnv"
COMMON="SPARK_ENV=cloud,DATA_BASE=$DATA,RAW_TRAIN=$DATA/raw/train.csv,RAW_TEST=$DATA/raw/test.csv,OUTPUT_BASE=$OUT"
ENVPROPS=""
for kv in ${COMMON//,/ }; do
  ENVPROPS="${ENVPROPS:+$ENVPROPS,}$E.$kv,$X.$kv"
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
submit clean_data.py
submit feature_engineering.py
submit summarize_features.py

# The grid/conflation sweep re-encodes the WHOLE dataset once per candidate
# (H3 8/9/10 + geohash 6/7) and adds a distinct-cell count and a bearing-entropy
# groupBy to each. That is five extra full passes to justify a design choice --
# and the justification is qualitatively identical on a sample, where it already
# ran. Paying for it at 1.71M is burning budget for no extra information.
if [[ "$SCALE" == "--full" ]]; then
  submit spatial_encoding.py
else
  submit spatial_encoding.py --compare-grids
fi

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
if [[ "$SCALE" != "--full" ]]; then
  submit route_mining_exact.py
  submit route_mining_closed.py
  submit route_mining_maximal.py
fi

echo "== done =="
echo "   parquet : $DATA/processed"
echo "   results : $OUT/routes and $OUT/statistics"
gsutil ls "$OUT/routes/" || true
echo "   fetch with: gsutil -m cp -r '$OUT' ./cloud_outputs/"
