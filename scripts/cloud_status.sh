#!/bin/bash
#
# cloud_status.sh  --  read-only view of the cloud run, and the evidence capture
# ============================================================================
# Creates nothing, deletes nothing, bills nothing. Two jobs:
#
#   1. Answer "is anything running right now?" -- a cluster left alive is the
#      only way this project can quietly burn budget.
#   2. Capture the evidence the brief requires WHILE the cluster still exists.
#      dataproc_submit.sh deletes the cluster on exit (that is what stops the
#      billing), so `>=5 machines` cannot be evidenced after the fact. Run this
#      from a second terminal once the jobs start.
#
# Usage:
#   bash scripts/cloud_status.sh            # everything
#   bash scripts/cloud_status.sh evidence   # just the >=5-machines proof
#   bash scripts/cloud_status.sh outputs    # just what has landed in GCS
#
set -uo pipefail

PROJECT="${PROJECT:-finalproj-noyabayazi}"
BUCKET="${BUCKET:-gs://taxi-project-noyabayazi}"
PREFIX="${PREFIX:-taxi}"
REGION="${REGION:-europe-west1}"
CLUSTER="${CLUSTER:-porto}"
DATA="$BUCKET${PREFIX:+/$PREFIX}"
OUT="$DATA/outputs"
WHAT="${1:-all}"

hdr () { echo; echo "=== $* ==="; }

if [[ "$WHAT" == "all" || "$WHAT" == "clusters" ]]; then
  hdr "clusters alive in $REGION (anything listed here is being billed)"
  gcloud dataproc clusters list --region "$REGION" --project "$PROJECT" \
    --format='table(clusterName,status.state,config.workerConfig.numInstances:label=WORKERS)' \
    2>&1 || true
fi

if [[ "$WHAT" == "all" || "$WHAT" == "evidence" ]]; then
  hdr "EVIDENCE: cluster size (the brief requires >= 5 machines)"
  # Master + workers. The brief counts machines in the cluster, so report both
  # the worker count and the total, rather than leaving the reader to add them.
  DESC="$(gcloud dataproc clusters describe "$CLUSTER" --region "$REGION" \
            --project "$PROJECT" \
            --format='value(config.masterConfig.numInstances,config.workerConfig.numInstances,config.masterConfig.machineTypeUri,config.workerConfig.machineTypeUri,status.state)' 2>&1)"
  if [[ "$DESC" == *"NOT_FOUND"* || "$DESC" == *"ERROR"* ]]; then
    echo "  cluster '$CLUSTER' is not alive right now."
    echo "  This capture only works WHILE the run is in progress -- the submit"
    echo "  script deletes the cluster on exit."
  else
    read -r M W MT WT ST <<<"$DESC"
    echo "  cluster : $CLUSTER  ($ST)"
    echo "  master  : $M x ${MT##*/}"
    echo "  workers : $W x ${WT##*/}"
    echo "  TOTAL   : $((M + W)) machines"
    if (( M + W >= 5 )); then
      echo "  [OK]   >= 5 machines"
    else
      echo "  [WARN] fewer than 5 machines -- fine for a rehearsal, NOT for the"
      echo "         graded run. Use WORKERS=5 (the script's default)."
    fi
    hdr "EVIDENCE: the individual VMs"
    gcloud compute instances list --project "$PROJECT" \
      --filter="name~^${CLUSTER}-" \
      --format='table(name,machineType.basename(),zone.basename(),status)' 2>&1 || true
  fi
fi

if [[ "$WHAT" == "all" || "$WHAT" == "jobs" ]]; then
  hdr "recent jobs"
  gcloud dataproc jobs list --region "$REGION" --project "$PROJECT" --limit 25 \
    --format='table(reference.jobId.slice(-14:),status.state,pysparkJob.mainPythonFileUri.basename())' \
    2>&1 || true
fi

if [[ "$WHAT" == "all" || "$WHAT" == "outputs" ]]; then
  hdr "EVIDENCE: it read from and wrote to GCS"
  for d in processed outputs/routes outputs/statistics; do
    n="$(gsutil ls "$DATA/$d/**" 2>/dev/null | wc -l | tr -d ' ')"
    printf '  %-22s %s objects\n' "$d" "$n"
  done
  hdr "deliverable CSVs in $OUT/routes"
  gsutil ls "$OUT/routes/" 2>&1 | sed 's|.*/||' || true
fi

echo
