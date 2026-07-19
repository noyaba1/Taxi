# DataProc Pre-Flight Checklist (one-shot, no guessing)

Assume **one efficient use of the $50 budget**. Run every section in order. Do not
create the cluster until Section A passes (it costs nothing and catches the
expensive mistakes). `[ ]` = tick before proceeding.

Two blockers were already fixed in pre-flight and MUST be in the code you upload
(commit **`1f4a557`** or later): consistent `_encoded_r9_full.parquet` naming, and
the cluster installing `h3`+`datasketches`.

---

## A. Local pre-flight (free — do this first)

### A1. Repo state
```bash
cd <repo-root>
git rev-parse --abbrev-ref HEAD          # -> Noya
git log --oneline -1                      # -> at least 1f4a557
git status --porcelain                    # -> empty (all committed)
```
- [ ] on `Noya`, clean tree, HEAD ≥ `1f4a557`.

### A2. Files that MUST exist locally (uploaded or zipped to the cluster)
```bash
ls train.csv/train.csv                    # the raw 1.9 GB file
ls src/config.py src/spark_session.py src/clean_data.py \
   src/feature_engineering.py src/spatial_encoding.py \
   src/route_mining_maximal.py src/route_mining_exact.py src/route_mining_approx.py \
   src/route_mining_clustering.py src/route_mining_graph.py src/anomaly_analysis.py \
   scripts/dataproc_submit.sh
```
- [ ] raw file present (~1.9 GB) and all 11 stage files + submit script exist.

### A3. Confirm the two fixed blockers are actually in the code
```bash
grep -c "_encoded_r{res}_full.parquet" src/spatial_encoding.py src/route_mining_exact.py   # each -> 1
grep -c "PIP_PACKAGES" scripts/dataproc_submit.sh                                            # -> 1 (h3+datasketches)
grep -c "storage_join" src/config.py                                                          # -> >=1 (gs:// safe)
grep -c "treeReduce" src/route_mining_approx.py                                               # -> 1 (no maxResultSize)
```
- [ ] all four checks non-zero.

### A4. Free end-to-end validation on the local sample
```bash
py -3.11 -m venv .venv 2>$null; .\.venv\Scripts\Activate.ps1     # if not already
$env:SPARK_DRIVER_MEM="10g"; $env:SPARK_SHUFFLE_PARTS="64"
python -m src.make_sample 5000
python -m src.run_pipeline --sample --verify
python -m pytest tests/ -q
```
- [ ] `run_pipeline` prints `OK` for every stage + every verifier `PASSED`; 11 tests pass.

> If A4 is green, the code is correct end-to-end. The cloud run only changes SCALE
> and STORAGE, both already wired. Proceed to spend budget.

---

## B. Environment variables (the complete set)

| Variable | Value | Where | Why |
|---|---|---|---|
| `PROJECT` | your GCP project id | your shell | `gcloud` target |
| `BUCKET` | `gs://<unique>` | your shell | storage root |
| `REGION` | `europe-west1` | your shell | cheap, near Portugal |
| `SPARK_ENV` | `cloud` | job (submit sets it) | YARN master, skip Windows shims |
| `DATA_BASE` | `$BUCKET/porto` | job (submit sets it) | all Parquet I/O |
| `RAW_TRAIN` | `$BUCKET/porto/raw/train.csv` | job (submit sets it) | `clean_data` input |
| `OUTPUT_BASE` | `/tmp/porto_out` | job (submit sets it) | driver-local reports |
| `CLUSTERING_MAX_TRIPS` | `50000` (default) | optional job override | bound LSH skew |

The submit script sets all `SPARK_ENV/DATA_BASE/RAW_TRAIN/OUTPUT_BASE` on both the
driver (`appMasterEnv`) and executors. You only export `PROJECT/BUCKET/REGION`.

---

## C. Cloud execution (chronological)

### C1. Auth + APIs + bucket
```bash
export PROJECT=<id>; export BUCKET=gs://<unique>; export REGION=europe-west1
gcloud config set project "$PROJECT"
gcloud services enable dataproc.googleapis.com storage.googleapis.com
gsutil ls -b "$BUCKET" 2>/dev/null || gsutil mb -l "$REGION" "$BUCKET"
```
- Expected: bucket exists (`gsutil ls "$BUCKET"` returns without error).
- **Fail:** `AccessDenied`/`billing` → enable billing + the two APIs; retry.

### C2. Upload raw + code
```bash
gsutil -q stat "$BUCKET/porto/raw/train.csv" || gsutil -m cp "train.csv/train.csv" "$BUCKET/porto/raw/train.csv"
zip -qr src.zip src -x "*/__pycache__/*"; gsutil cp src.zip "$BUCKET/code/src.zip"
gsutil du -h "$BUCKET/porto/raw/train.csv"          # ~1.9 GiB
```
- Expected: raw ≈ 1.9 GiB; `src.zip` uploaded.
- **Fail:** upload stalls → rerun (`gsutil -m cp` resumes); confirm the local path
  `train.csv/train.csv` exists.

### C3. Run the whole pipeline (scripted — creates cluster, submits all, auto-deletes)
```bash
PROJECT=$PROJECT BUCKET=$BUCKET REGION=$REGION bash scripts/dataproc_submit.sh
```
This performs C4-C6 below and deletes the cluster on exit. To watch/limit
stage-by-stage, run them manually per `docs/CLOUD_CHECKLIST.md` §4 instead.

### C4. Cluster create (inside the script)
- Expected: `Waiting for cluster creation operation... done`, 1 master + 4 workers,
  init action installs `h3`+`datasketches` (~3-5 min).
- **Fail — `Quota 'CPUS' exceeded`:** lower `--num-workers` to 3, or request quota.
- **Fail — init action error:** check the init log in the cluster's staging bucket;
  usually a transient pip network error → recreate.

### C5. Stage-by-stage expected outputs (full 1.71M; magnitudes are directional)

| # | Stage | Writes | Expected (full) | Watch for |
|---|---|---|---|---|
| 1 | `clean_data` | `…/processed/trips_clean.parquet` | total ≈ 1,710,670; valid ≈ 97% (~1.66M) | `FileNotFound` raw → RAW_TRAIN unset |
| 2 | `feature_engineering` | `…_features.parquet` | ~1.66M rows; anomaly few % | Arrow/pandas errors → pyarrow missing |
| 3 | `spatial_encoding` | `…_encoded_r9_full.parquet` | ~1.66M; avg_compact ≈ 17 | `ModuleNotFoundError h3` → init action failed |
| 4 | `route_mining_maximal` | `maximal_frequent_top100` CSV | 100s of maximal routes @ X=0.5% (min_sup ≈ 8k trips); holes shown | huge shuffle → raise workers |
| 5 | `route_mining_exact` | `exact_top100` CSV | ~300M windows → ~190M distinct; 1 km top support in the **thousands**; 10-40 km now non-trivial | `maxResultSize`? (should be fixed) |
| 6 | `route_mining_approx` | `approx_top100` CSV | precision@100 high at 1-5 km; **memory ratio ≫ 30×** | `maxResultSize` → old code (need `treeReduce`) |
| 7 | `route_mining_clustering` | `clustering_top100` CSV | "clustering a representative sample: 50,000 of ~1.66M"; ~thousands of clusters | worker timeout → lower `CLUSTERING_MAX_TRIPS` |
| 8 | `route_mining_graph` | `graph_heavy_paths` + `activity_zones` CSV | ~5-15k nodes; thousands of validated corridors; 50 zones | super-node skew → prune (`GRAPH_MIN_EDGE_SUPPORT`) |
| 9 | `anomaly_analysis` | `anomalies_top50` CSV | few % anomalous; 5 detector counts | — |

Each stage also **prints its result tables to stdout** (captured in the job's
driver output on GCS), so you can read every headline number without files.

### C6. Cluster delete
- The script's `trap` deletes it. Verify: `gcloud dataproc clusters list --region $REGION` → empty.
- **If the script died before the trap:** `gcloud dataproc clusters delete porto --region $REGION -q`.

### C7. Collect results
```bash
gsutil ls -r "$BUCKET/porto/processed/"                     # parquet tables
gcloud dataproc jobs list --region "$REGION"                # each job SUCCEEDED
# optional: pull CSV/report files off the (deleted?) master BEFORE deletion, or
# regenerate locally from the downloaded encoded parquet.
```
- **Parity check:** top corridors/zones match the local sample's downtown shape,
  with much larger supports; 10-40 km thresholds now populated.

---

## D. Global failure → recovery table

| Symptom | Root cause | Recovery |
|---|---|---|
| `clean_data` `FileNotFoundException` on `raw/train.csv` | RAW_TRAIN not set / bad upload | use the submit script (sets RAW_TRAIN); re-upload raw |
| `ModuleNotFoundError: h3` / `datasketches` | init action didn't run / wrong region URL | recreate cluster; confirm `--metadata PIP_PACKAGES` + region in the init-action URL |
| `... is bigger than spark.driver.maxResultSize` | old approx code | ensure uploaded `src.zip` is from commit ≥ `1f4a557` (uses `treeReduce`) |
| `Path ... _encoded_r9_full.parquet not found` at a miner | encoding wrote a different name (old code) | ensure `src.zip` ≥ `adf2caf`; rerun `spatial_encoding` then the miner |
| clustering hangs / worker `EOFError`/timeout | LSH bucket skew | set `$E.CLUSTERING_MAX_TRIPS=30000` and resubmit that job only |
| a mid-pipeline stage fails | any | fix it, **resubmit that stage only** — upstream Parquet is already in GCS; never rerun the whole pipeline |
| cluster idle / forgotten | — | `--max-idle 30m` auto-deletes; or delete manually |
| results not in GCS files | `open()` can't write gs:// | read numbers from driver output; SSH-copy `/tmp/porto_out` before deleting the cluster |

## E. Cost guardrails (one-shot budget)

- [ ] Section A green **before** creating any cluster (all debugging done for free).
- [ ] Cluster = 5× n2-standard-4 ≈ **$1/hr**; a full pass ≪ 1 hr → a few dollars.
- [ ] `--max-idle 30m` + delete `trap` are in the script — do not remove them.
- [ ] If a stage fails, resubmit **only that stage** (don't recompute upstream).
- [ ] Delete the cluster the moment the last job succeeds; confirm the list is empty.

**Go/no-go:** proceed to Section C only when **A1-A4 are all ticked**. That is the
single most budget-protective step — it validates the exact code the cluster will
run, for free.
