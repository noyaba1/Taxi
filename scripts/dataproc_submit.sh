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

echo "== package + upload code =="
zip -qr src.zip src -x "*/__pycache__/*"
gsutil cp src.zip "$BUCKET/code/src.zip"

echo "== create 5-machine cluster (1 master + 4 workers) =="
gcloud dataproc clusters create "$CLUSTER" --region "$REGION" \
  --master-machine-type n2-standard-4 --num-masters 1 \
  --worker-machine-type n2-standard-4 --num-workers 4 \
  --image-version 2.1-debian12 --max-idle 30m \
  --properties spark:spark.sql.adaptive.enabled=true

# auto-delete the cluster on ANY exit (success, failure, Ctrl-C) -> budget-safe
trap 'echo "== deleting cluster =="; gcloud dataproc clusters delete "$CLUSTER" --region "$REGION" -q' EXIT

ENVPROPS="spark.yarn.appMasterEnv.SPARK_ENV=cloud,spark.yarn.appMasterEnv.DATA_BASE=$DATA,spark.executorEnv.SPARK_ENV=cloud,spark.executorEnv.DATA_BASE=$DATA"

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
