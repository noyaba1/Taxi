# DataProc Execution Checklist (copy-paste runbook)

Run top to bottom. Fill the three variables once; every command after that is
literal. Rationale and options are in `docs/DATAPROC.md`; this file is the
no-guessing sequence. **Budget the whole run is a few dollars if you don't leave
the cluster running.**

---

## 0. One-time variables

```bash
export PROJECT=<your-project-id>
export BUCKET=gs://<your-bucket>          # globally unique
export REGION=europe-west1                # near Portugal, cheap
gcloud config set project "$PROJECT"
gcloud services enable dataproc.googleapis.com storage.googleapis.com
```

## 1. Create the bucket

```bash
gsutil ls -b "$BUCKET" 2>/dev/null || gsutil mb -l "$REGION" "$BUCKET"
```

## 2. Upload raw data + code (once)

```bash
cd <repo-root>
# raw 1.9 GB file -> clean path (a few minutes)
gsutil -m cp "train.csv/train.csv" "$BUCKET/porto/raw/train.csv"
# code package
zip -qr src.zip src -x "*/__pycache__/*"
gsutil cp src.zip "$BUCKET/code/src.zip"
# verify
gsutil du -h "$BUCKET/porto/raw/train.csv"   # ~1.9 GiB
```

## 3. Run everything (creates cluster, submits all stages, auto-deletes)

The scripted path — **recommended**:

```bash
PROJECT=$PROJECT BUCKET=$BUCKET REGION=$REGION bash scripts/dataproc_submit.sh
```

It creates a **1 master + 4 worker = 5-machine** cluster, submits every stage with
`SPARK_ENV=cloud DATA_BASE=$BUCKET/porto RAW_TRAIN=.../raw/train.csv`, and the
`trap` deletes the cluster on any exit. Skip to step 7 if you use this.

## 4. — OR — do it manually (if you want stage-by-stage control)

```bash
gcloud dataproc clusters create porto --region "$REGION" \
  --master-machine-type n2-standard-4 --num-masters 1 \
  --worker-machine-type n2-standard-4 --num-workers 4 \
  --image-version 2.1-debian12 --max-idle 30m \
  --properties spark:spark.sql.adaptive.enabled=true,spark:spark.sql.shuffle.partitions=200

D=$BUCKET/porto; E=spark.yarn.appMasterEnv; X=spark.executorEnv
PROPS="$E.SPARK_ENV=cloud,$E.DATA_BASE=$D,$E.RAW_TRAIN=$D/raw/train.csv,$E.OUTPUT_BASE=/tmp/porto_out,$X.SPARK_ENV=cloud,$X.DATA_BASE=$D,$X.RAW_TRAIN=$D/raw/train.csv"
submit(){ gcloud dataproc jobs submit pyspark "src/$1" --cluster porto --region "$REGION" \
          --py-files "$BUCKET/code/src.zip" --properties "$PROPS" -- --full; }

submit clean_data.py           # trips_clean -> gs://.../processed
submit feature_engineering.py
submit spatial_encoding.py
submit route_mining_maximal.py       # Method B (PDF: min-support X% + maximal)
submit route_mining_exact.py         # exact baseline (for the approx comparison)
submit route_mining_approx.py        # Method B approximate (Space-Saving/Count-Min)
submit route_mining_clustering.py    # Method A (auto-samples to CLUSTERING_MAX_TRIPS)
submit route_mining_graph.py         # Method C + activity zones
submit anomaly_analysis.py
```

## 5. Monitor

```bash
gcloud dataproc jobs list --region "$REGION" --cluster porto      # states
gcloud dataproc jobs wait <JOB_ID> --region "$REGION"             # stream a job's output
# Spark UI: gcloud dataproc clusters describe porto (see the yarn/spark links)
```

## 6. Verify outputs

```bash
gsutil ls -r "$BUCKET/porto/processed/"        # parquet tables exist
# headline numbers (top routes, supports, comparison, zones) are in each job's
# DRIVER OUTPUT, captured to GCS automatically — read via `jobs wait <ID>`.
```

Optional — pull the CSV/report files off the master:

```bash
gcloud compute ssh porto-m --zone "$REGION-b" \
  --command "gsutil -m cp -r /tmp/porto_out $BUCKET/porto/outputs"
gsutil ls "$BUCKET/porto/outputs/routes/"
```

**Sanity/parity check:** the top corridors and activity zones should match the
local 5k/50k sample's *shape* (same downtown Porto areas), with larger support
counts. Long-route thresholds (10-40 km) should now have real support (they were
sparse on 5k).

## 7. Delete the cluster (do this immediately when done)

```bash
gcloud dataproc clusters delete porto --region "$REGION" -q
gcloud dataproc clusters list --region "$REGION"    # confirm empty
```

## 8. Cost-saving recommendations

- **Debug locally, run cloud once.** The pipeline is validated to 50k locally;
  the cloud run is for the final full-scale numbers, not debugging.
- `--max-idle 30m` + the delete `trap` prevent a forgotten cluster draining $50.
- 5× n2-standard-4 ≈ **$1/hour**; a full pass is well under an hour → a few dollars.
- If a stage fails, fix and resubmit **that stage only** (previous Parquet is
  already in GCS) — don't rerun the whole pipeline.
- Delete `src.zip`/raw upload only if you won't rerun; keeping them costs pennies.

## 9. If something fails — quick triage

| Symptom | Cause | Fix |
|---|---|---|
| `clean_data` FileNotFound on raw | `RAW_TRAIN` not passed | use the `PROPS` above (it sets it) |
| `maxResultSize` in approx | old code | ensure the pushed `route_mining_approx.py` uses `treeReduce` (commit `6dddce5`+) |
| clustering slow/OOM | LSH bucket skew | it auto-samples to `CLUSTERING_MAX_TRIPS`; lower it via `$E.CLUSTERING_MAX_TRIPS=30000` |
| exact-mining shuffle huge/slow | 260M window rows | raise `--num-workers`, or rely on the approx job; see DATAPROC.md §8 |
| reports not in GCS | `open()` can't write gs:// | read numbers from driver output; SSH-copy `/tmp/porto_out` (step 6) |
