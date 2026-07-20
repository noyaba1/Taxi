# Cloud Run Playbook — First DataProc Execution

Chronological, executable runbook. Someone who has never seen this project should
be able to run the full DataProc execution from here without guessing. Do the
**Part 0 local pre-flight first — it is free and prevents almost all budget
waste.** Only create a cluster once Part 0 is green.

Legend for "Where": **[L]** local terminal · **[CS]** Google Cloud Shell (or any
shell with `gcloud`/`gsutil`) · **[UI]** browser (Cloud Console / Spark UI).

Baseline required: branch `Noya`, HEAD ≥ `1948b53`.

---

## Part 0 — Local verification (free)

### Step 1 — Local pipeline verification `[L]`
- **Goal:** prove the exact code the cluster will run is correct end-to-end.
- **Commands:**
  ```powershell
  .\.venv\Scripts\Activate.ps1
  $env:SPARK_DRIVER_MEM="10g"; $env:SPARK_SHUFFLE_PARTS="64"
  python -m src.make_sample 5000
  python -m src.run_pipeline --sample --verify
  python -m pytest tests/ -q
  ```
- **Expected / success:** every stage prints `OK`, every verifier `PASSED`, `11
  passed`. **Runtime:** ~5-10 min.
- **Failure:** any `FAILED`/traceback → fix locally; **do not touch the cloud**.
- **Reversible:** yes (local only). **Screenshot:** no. **Safe to continue:** only
  if fully green.

### Step 2 — Git verification `[L]`
- **Goal:** the cluster gets the committed, fixed code.
- **Commands:**
  ```bash
  git rev-parse --abbrev-ref HEAD      # Noya
  git log --oneline -1                 # >= 1948b53
  git status --porcelain               # empty
  grep -c "_encoded_r9_full.parquet" src/spatial_encoding.py   # 1
  grep -c "PIP_PACKAGES" scripts/dataproc_submit.sh            # 1
  grep -c "treeReduce" src/route_mining_approx.py              # 1
  ```
- **Success:** clean tree, HEAD current, all greps ≥ 1.
- **Failure:** dirty tree / old HEAD → commit or `git pull`. **Safe to continue:**
  only when clean.

---

## Part 1 — Cloud setup

### Step 3 — Environment variables `[CS]`
- **Goal:** set the three variables everything else uses.
- **Commands:**
  ```bash
  export PROJECT=<your-project-id>
  export BUCKET=gs://<globally-unique-name>
  export REGION=europe-west1
  gcloud config set project "$PROJECT"
  gcloud services enable dataproc.googleapis.com storage.googleapis.com
  ```
- **Expected:** `Updated property [core/project]`; services enable (~30 s).
- **Failure:** `PERMISSION_DENIED`/billing → enable billing on the project.
- **Reversible:** yes. **Safe to continue:** when both APIs are enabled.

### Step 4 — Bucket verification / creation `[CS]`
- **Commands:**
  ```bash
  gsutil ls -b "$BUCKET" 2>/dev/null || gsutil mb -l "$REGION" "$BUCKET"
  gsutil ls -b "$BUCKET"
  ```
- **Success:** the bucket lists without error. **Runtime:** seconds.
- **Failure:** `BucketNameUnavailable` → pick another `$BUCKET`.
- **Reversible:** yes (`gsutil rb` to delete). **Safe to continue:** yes.

### Step 5 — Upload data + code `[CS]` (or `[L]` with gsutil)
- **Goal:** raw file + code package in GCS.
- **Commands:**
  ```bash
  cd <repo-root>
  gsutil -q stat "$BUCKET/porto/raw/train.csv" || gsutil -m cp "train.csv/train.csv" "$BUCKET/porto/raw/train.csv"
  zip -qr src.zip src -x "*/__pycache__/*"
  gsutil cp src.zip "$BUCKET/code/src.zip"
  gsutil du -h "$BUCKET/porto/raw/train.csv"    # ~1.9 GiB
  ```
- **Success:** raw ≈ 1.9 GiB; `src.zip` present. **Runtime:** 2-6 min (raw upload).
- **Failure:** stall/interrupt → rerun (`gsutil -m cp` resumes).
- **Reversible:** yes. **Screenshot:** no. **Safe to continue:** when `du` shows ~1.9 GiB.

### Step 6 — Create the cluster `[CS]`
- **Goal:** a 5-machine Spark cluster (1 master + 4 workers).
- **Commands:**
  ```bash
  gcloud dataproc clusters create porto --region "$REGION" \
    --master-machine-type n2-standard-4 --num-masters 1 \
    --worker-machine-type n2-standard-4 --num-workers 4 \
    --image-version 2.1-debian12 --max-idle 30m \
    --initialization-actions gs://goog-dataproc-initialization-actions-$REGION/python/pip-install.sh \
    --metadata PIP_PACKAGES="h3==3.7.7 datasketches==5.0.2" \
    --properties spark:spark.sql.adaptive.enabled=true,spark:spark.sql.shuffle.partitions=200
  ```
- **Expected / success:** `Waiting for cluster creation operation...done`, status
  `RUNNING`. **Runtime:** 3-6 min (incl. Step 7 pip install).
- **Failure:** `Quota 'CPUS' exceeded` → `--num-workers 3` or request quota;
  init-action error → recreate (usually transient pip network).
- **Reversible:** yes (delete). **Screenshot:** **YES** — cluster config for the
  presentation (proves 5 machines). **Safe to continue:** when `RUNNING`.

### Step 7 — Dependency install (automatic, inside Step 6)
- **Goal:** `h3` + `datasketches` on every node.
- **Verify:** cluster reaches `RUNNING` (init action must succeed for that). To be
  sure: after Step 8's encoding runs without `ModuleNotFoundError`, deps are good.
- **Failure:** encoding later throws `ModuleNotFoundError: h3` → the init action
  didn't run; recreate the cluster checking the `--metadata`/region URL.

---

## Part 2 — Pipeline stages (each = one Dataproc job)

Helper (paste once in `[CS]`):
```bash
D=$BUCKET/porto; E=spark.yarn.appMasterEnv; X=spark.executorEnv
PROPS="$E.SPARK_ENV=cloud,$E.DATA_BASE=$D,$E.RAW_TRAIN=$D/raw/train.csv,$E.OUTPUT_BASE=/tmp/porto_out,$X.SPARK_ENV=cloud,$X.DATA_BASE=$D,$X.RAW_TRAIN=$D/raw/train.csv"
submit(){ gcloud dataproc jobs submit pyspark "src/$1" --cluster porto --region "$REGION" \
          --py-files "$BUCKET/code/src.zip" --properties "$PROPS" -- --full; }
```
> **Canary rule:** run Step 8 alone and confirm success before Steps 10-17.

### Step 8 — clean_data (CANARY) `[CS]`
- **Command:** `submit clean_data.py`
- **Expected:** driver prints `total trips 1,710,670`, `valid trips … (~97%)`.
- **Runtime (est.):** 3-6 min. **Success:** job `SUCCEEDED`.
- **Spark UI [UI]:** Jobs tab — one read+filter stage, low shuffle.
- **Failure:** `FileNotFoundException raw/train.csv` → RAW_TRAIN/upload; fix, resubmit.
- **Reversible:** yes (overwrite). **Screenshot:** no. **Safe to continue:** only if SUCCEEDED.

### Step 9 — Verify clean_data `[CS]`
- **Commands:**
  ```bash
  gsutil ls "$D/processed/trips_clean.parquet/_SUCCESS"
  gcloud dataproc jobs submit pyspark src/verify_phase1.py --cluster porto \
    --region "$REGION" --py-files "$BUCKET/code/src.zip" --properties "$PROPS" -- --full
  ```
- **Success:** `_SUCCESS` exists; verifier prints schema + rejection reasons; ~97% valid.
- **Failure:** no `_SUCCESS` → Step 8 didn't finish. **Safe to continue:** when both pass.

### Step 10 — feature_engineering `[CS]`
- **Command:** `submit feature_engineering.py`
- **Expected:** driver prints anomaly-flag table + avg dist/speed. **Runtime:** 3-6 min.
- **Success:** `_SUCCESS` at `…_features.parquet`. **UI:** narrow `pandas_udf` stage,
  ~no shuffle. **Failure:** Arrow/pandas error → pyarrow issue (rare on 2.1).
- **Screenshot:** no. **Reversible:** yes. **Safe to continue:** if SUCCEEDED.

### Step 11 — spatial_encoding (H3) `[CS]`
- **Command:** `submit spatial_encoding.py`
- **Expected:** driver prints `avg_compact ≈ 17` + resolution sweep. **Runtime:** 5-12 min
  (h3 over ~85M points). **Success:** `_SUCCESS` at `…_encoded_r9_full.parquet`.
- **UI:** high-CPU narrow stage, near-zero shuffle. **Failure:** `ModuleNotFoundError h3`
  → deps (Step 7). **Screenshot:** no. **Safe to continue:** if SUCCEEDED (all methods depend on this).

### Step 12 — Method B (maximal + exact + approx) `[CS]`
- **Commands:** `submit route_mining_maximal.py; submit route_mining_exact.py; submit route_mining_approx.py`
- **Expected:** maximal → `maximal-frequent routes: N` + holes; exact → `window
  emissions / distinct` (~330M/~190M); approx → `memory ratio ≫ 30×`.
- **Runtime (est.):** **the heavy trio — 10-25 min each** (biggest budget consumer).
- **Success:** each job `SUCCEEDED`; CSVs printed. **UI:** watch the groupBy shuffle
  (Stages tab) for skew/spill — see LIVE_MONITORING_GUIDE.
- **Failure:** OOM/`maxResultSize`/spill → add workers OR keep `approx` result and
  treat exact as best-effort; resubmit only the failed stage.
- **Screenshot:** **YES** — the shuffle/stage metrics + the printed top-100 tables.
- **Reversible:** yes (each overwrites its CSV). **Safe to continue:** if ≥ approx succeeded.

### Step 13 — Method A (clustering) `[CS]`
- **Command:** `submit route_mining_clustering.py`
- **Expected:** `clustering a representative sample: 50,000 of ~1.66M` then `clusters: N`.
- **Runtime:** 2-4 min. **Success:** job `SUCCEEDED`. **UI:** `approxSimilarityJoin`
  stage — one giant task = skew (should be bounded). **Failure:** worker timeout →
  set `$E.CLUSTERING_MAX_TRIPS=30000`, resubmit. **Screenshot:** optional. **Safe:** yes.

### Step 14 — Method C (graph + zones) `[CS]`
- **Command:** `submit route_mining_graph.py`
- **Expected:** `nodes/edges`, `validated corridors`, `50 zones`. **Runtime:** 5-10 min.
- **Success:** `SUCCEEDED`; `graph_heavy_paths_top100_full.csv` + `activity_zones_full.csv`.
- **UI:** 15 PageRank iteration stages (joins). **Failure:** super-node skew →
  raise `GRAPH_MIN_EDGE_SUPPORT`. **Screenshot:** **YES** (zones for the map). **Safe:** yes.

### Step 15 — anomaly_analysis `[CS]`
- **Command:** `submit anomaly_analysis.py`
- **Expected:** 5 detector counts, ~few % anomalous. **Runtime:** 2-4 min.
- **Success:** `SUCCEEDED`; `anomalies_top50_full.csv`. **Failure:** features path →
  Step 10. **Screenshot:** no. **Safe:** yes.

### Step 16 — Comparisons (LOCAL post-processing) `[L]`
- **Goal:** A-vs-B-vs-C overlap table. **Runs locally** on the downloaded CSVs.
- **Commands:**
  ```bash
  gcloud compute ssh porto-m --zone $REGION-b --command "gsutil -m cp -r /tmp/porto_out $BUCKET/porto/outputs"   # if not already
  gsutil -m cp -r "$BUCKET/porto/outputs/routes" ./outputs/     # bring CSVs local
  python -m src.evaluation --full
  ```
- **Expected:** overlap matrix (B↔C high). **Runtime:** seconds. **Success:** report
  written. **Failure:** missing CSV → the matching mining stage didn't produce it.
- **Reversible:** yes (local). **Screenshot:** **YES** (comparison table). **Safe:** yes.

### Step 17 — Visualization (LOCAL) `[L]`
- **Command:** `python -m src.visualization --full` → `outputs/maps/porto_map_full.html`
- **Expected:** interactive map (routes/zones/anomalies). **Runtime:** seconds.
- **Also:** `notebooks/porto_routes_colab.ipynb` renders the same in Colab Enterprise
  (set `BASE=gs://…/outputs/routes`, `SUFFIX=full`).
- **Screenshot:** **YES** (the map — the headline demo). **Safe:** yes.

---

## Part 3 — Teardown & cost

### Step 18 — Download results `[CS]`
- **Commands:**
  ```bash
  gsutil ls -r "$BUCKET/porto/processed/"        # parquet tables present
  gsutil -m cp -r "$BUCKET/porto/outputs" ./cloud_outputs/   # if you copied them up
  ```
- **Success:** parquet + CSVs retrieved. **Note:** headline numbers are also in each
  job's driver output (`gcloud dataproc jobs wait <ID>`). **Do this BEFORE Step 19
  if you rely on the master's `/tmp/porto_out`.**

### Step 19 — Delete the cluster `[CS]`
- **Command:** `gcloud dataproc clusters delete porto --region "$REGION" -q`
- **Expected:** `Deleted cluster [porto]`. **Runtime:** 1-2 min.
- **CRITICAL / irreversible:** deletes compute (parquet/outputs in GCS are safe).
  **Do this the moment the last job succeeds.** **Safe to continue:** always — this
  stops the meter.

### Step 20 — Verify cost cleanup `[CS]`/`[UI]`
- **Commands:**
  ```bash
  gcloud dataproc clusters list --region "$REGION"     # empty
  gcloud compute instances list                        # no porto-* VMs
  ```
- **[UI]:** Billing → check the day's Dataproc/Compute spend (expect a few $).
- **Success:** no clusters/instances; spend ≪ $50. **If a VM lingers:** delete it.

---

## Execution timeline & rerun matrix (Task 4)

Estimates are directional (≈ 34× the measured 50k run; unverified > 100k).

| Stage | Est. duration | On critical path? | Depends on | Rerun-safe? | Partial reuse? | Forces downstream rerun? |
|---|---|---|---|---|---|---|
| clean | 3-6 min | yes | raw | yes (overwrite) | n/a | yes (all) |
| features | 3-6 min | yes | clean | yes | reuse clean | yes (encode+anomaly) |
| encoding | 5-12 min | yes | features | yes | reuse features | yes (all miners) |
| **maximal** | **10-25 min** | no | encoded | yes | reuse encoded | no |
| **exact** | **10-25 min** | no | encoded | yes | reuse encoded | no |
| **approx** | **15-25 min** | no | encoded | yes | reuse encoded | no |
| clustering | 2-4 min | no | encoded | yes | reuse encoded | no |
| graph | 5-10 min | no | encoded | yes | reuse encoded | no |
| anomaly | 2-4 min | no | features | yes | reuse features | no |
| eval/viz (local) | < 1 min | no | CSVs | yes | reuse CSVs | no |

- **Critical path:** clean → features → encoding (must succeed in order; ~15-25 min).
  The 6 analysis stages then run independently.
- **Budget hot-spots:** **maximal + exact + approx** (the ~190M-key support-table
  shuffle, done three times) — expect the majority of compute here.
- **Reuse rule:** any Parquet already in GCS is reusable; if an analysis stage fails,
  **resubmit only it** (encoded parquet is untouched). Never rerun the whole pipeline.
- **Total wall-clock (serial):** ~1-1.5 h → ~$1-2 of cluster time; a few $ all-in.
