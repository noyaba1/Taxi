#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Porto Taxi -> GCP DataProc submit script (see docs/DATAPROC.md).
# Creates a 5-machine cluster, uploads code, runs the pipeline, copies outputs,
# and DELETES the cluster (budget-safe). Edit the CONFIG block, then:
#     bash scripts/dataproc_submit.sh
# ---------------------------------------------------------------------------
set -euo pipefail

# ---- CONFIG (edit) --------------------------------------------------------
PROJECT="${PROJECT:-your-project-id}"
BUCKET="${BUCKET:-gs://your-bucket}"
REGION="${REGION:-europe-west1}"
CLUSTER="${CLUSTER:-porto}"
# ---------------------------------------------------------------------------

DATA="$BUCKET/porto"
gcloud config set project "$PROJECT"

echo "== package + upload code + raw data =="
zip -qr src.zip src -x "*/__pycache__/*"
gsutil cp src.zip "$BUCKET/code/src.zip"
# upload the raw 1.9 GB file to a clean path (once; skip if already there)
gsutil -q stat "$DATA/raw/train.csv" || gsutil -m cp "train.csv/train.csv" "$DATA/raw/train.csv"

echo "== create 5-machine cluster (1 master + 4 workers) =="
# h3 + datasketches are NOT on a stock DataProc image; install on every node.
# (numpy/pandas/pyarrow ARE preinstalled on 2.1, so pandas_udf works.)
gcloud dataproc clusters create "$CLUSTER" --region "$REGION" \
  --master-machine-type n2-standard-4 --num-masters 1 \
  --worker-machine-type n2-standard-4 --num-workers 4 \
  --image-version 2.1-debian12 --max-idle 30m \
  --initialization-actions "gs://goog-dataproc-initialization-actions-$REGION/python/pip-install.sh" \
  --metadata PIP_PACKAGES="h3==3.7.7 datasketches==5.0.2" \
  --properties spark:spark.sql.adaptive.enabled=true,spark:spark.sql.shuffle.partitions=200

# auto-delete the cluster on ANY exit (success, failure, Ctrl-C) -> budget-safe
trap 'echo "== deleting cluster =="; gcloud dataproc clusters delete "$CLUSTER" --region "$REGION" -q' EXIT

# Env for driver (appMaster) AND executors. RAW_TRAIN is REQUIRED by clean_data
# (else it reads the local default path and fails). OUTPUT_BASE is a known local
# dir on the driver; the small CSV/MD reports land there and each stage also
# PRINTS its results (captured in the Dataproc driver output on GCS).
E="spark.yarn.appMasterEnv"; X="spark.executorEnv"
ENVPROPS="$E.SPARK_ENV=cloud,$E.DATA_BASE=$DATA,$E.RAW_TRAIN=$DATA/raw/train.csv,$E.OUTPUT_BASE=/tmp/porto_out,$X.SPARK_ENV=cloud,$X.DATA_BASE=$DATA,$X.RAW_TRAIN=$DATA/raw/train.csv"

submit () {  # $1 = module file under src/
  echo "== submit $1 =="
  gcloud dataproc jobs submit pyspark "src/$1" \
    --cluster "$CLUSTER" --region "$REGION" \
    --py-files "$BUCKET/code/src.zip" \
    --properties "$ENVPROPS" \
    -- --full
}

for stage in clean_data.py feature_engineering.py spatial_encoding.py \
             route_mining_maximal.py route_mining_clustering.py \
             route_mining_graph.py anomaly_analysis.py \
             route_mining_exact.py route_mining_approx.py; do
  submit "$stage"
done

echo "== done; parquet in $DATA/processed. Cluster will be deleted by trap. =="
