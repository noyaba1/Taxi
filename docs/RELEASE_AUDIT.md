# Release Audit — First DataProc Deployment

> ⚠️ **Superseded by [DATAPROC.md](DATAPROC.md).** This run-book predates the
> storage-layer fix: it points `OUTPUT_BASE` at the master's `/tmp`, which the
> cluster teardown then destroys, uses `--num-workers 4`, and reads the raw file
> from `train.csv/train.csv`. Kept for its narrative and monitoring detail only —
> follow DATAPROC.md for the commands.


Release-engineer sign-off document for the first (one-shot) full-scale run. No new
algorithms, no redesign. Grounded in the actual code (paths/imports verified) and
the measured 5k/50k/100k local dry runs. Full-scale figures are **directional
estimates** (≈ 34× the 50k numbers) and explicitly marked as such.

Code baseline: branch `Noya`, HEAD ≥ `d820a67` (includes the two pre-flight
blocker fixes: `_encoded_r9_full` naming, cluster `h3`+`datasketches` install).

---

## 1. Per-stage audit

Chain (cloud submit order). `D = gs://<bucket>/porto`. Encoded/feature names carry
`_full` on `--full`.

| # | Stage | Input | Output | Rows (full est.) | Files created |
|---|---|---|---|---|---|
| 1 | `clean_data` | `D/raw/train.csv` | `D/processed/trips_clean_full.parquet` | 1,710,670 → **~1.66M valid** (~97%) | parquet dir |
| 2 | `feature_engineering` | `trips_clean_full.parquet` | `…_features_full.parquet` | ~1.66M (1:1) | parquet dir |
| 3 | `spatial_encoding` | `…_features_full.parquet` | `…_encoded_r9_full.parquet` | ~1.66M (1:1) | parquet dir + 2 report `.md` |
| 4 | `route_mining_maximal` | `…_encoded_r9_full.parquet` | `maximal_frequent_top100_full.csv` | 100s of maximal routes | 1 csv + 1 md |
| 5 | `route_mining_exact` | `…_encoded_r9_full.parquet` | `exact_top100_full.csv` | ~600 rows (top-100 × 6) | 1 csv + 1 md |
| 6 | `route_mining_approx` | `…_encoded_r9_full.parquet` | `approx_top100_full.csv` | ~600 rows | 1 csv + 1 md |
| 7 | `route_mining_clustering` | `…_encoded_r9_full.parquet` (samples 50k) | `clustering_top100_full.csv` | ~600 rows | 1 csv + 1 md |
| 8 | `route_mining_graph` | `…_encoded_r9_full.parquet` | `graph_heavy_paths_top100_full.csv`, `activity_zones_full.csv` | ~600 + 50 rows | 2 csv + 1 md |
| 9 | `anomaly_analysis` | `…_features_full.parquet` | `anomalies_top50_full.csv` | ≤50 rows | 1 csv + 1 md |

**Schemas (verified from code):**
- `trips_clean`: `TRIP_ID, TAXI_ID, CALL_TYPE, TIMESTAMP, start_time, n_points,
  duration_sec, start_lon/lat, end_lon/lat, points:array<array<double>>`.
- `…_features`: clean + `total_distance_km, straight_line_km, max_seg_speed_kmh,
  min/max_lon/lat, avg_speed_kmh, sinuosity, is_teleport/idle/too_fast/too_short/
  anomalous`.
- `…_encoded_r9_full`: `TRIP_ID, TAXI_ID, n_points, duration_sec, total_distance_km,
  h3_seq_raw:array<string>, h3_seq_compact:array<string>, n_cells_raw,
  n_cells_compact, compression_ratio, encoded_len_km` (drops `points`).

**Runtime order & dependencies:** strictly linear 1→2→3, then 4-9 all depend only
on stage 3 (encoded) except 9 (depends on 2, features). Stages 4-9 are mutually
independent (could run in parallel; the script runs them serially for clarity).

**Per-stage: cause of failure · appearance · immediate success check**

| Stage | Would fail if… | Appears as… | Verify success immediately |
|---|---|---|---|
| clean | RAW_TRAIN unset / bad raw | `FileNotFoundException` at read | `gsutil ls D/processed/trips_clean_full.parquet/_SUCCESS`; driver prints `valid trips` % |
| features | pyarrow/pandas absent | Arrow/`PythonException` in UDF | `_SUCCESS`; driver prints anomaly % table |
| encoding | `h3` not installed | `ModuleNotFoundError: h3` | `_SUCCESS`; driver prints `avg_compact ≈ 17` |
| maximal | encoded name mismatch / OOM shuffle | `Path…not found` / executor lost | driver prints `maximal-frequent routes: N`; csv exists |
| exact | maxResultSize / OOM | `SparkException maxResultSize` / OOM | driver prints `window emissions / distinct` |
| approx | old code (collect) / datasketches absent | `maxResultSize` / `ModuleNotFoundError` | driver prints `memory ratio …×`; determinism not run on cloud |
| clustering | LSH skew / MLlib absent | worker `EOFError`/timeout | driver prints `representative sample: 50,000 of …` then `clusters: N` |
| graph | super-node skew | Pregel-like blowup / OOM | driver prints `validated corridors` + `zones` |
| anomaly | features name mismatch | `Path not found` | driver prints 5 detector counts |

---

## 2. Configuration consistency report

Reviewed `config.py` + every stage's path derivation.

| Check | Result |
|---|---|
| Duplicated paths | None — all derive from `DATA_BASE` via `storage_join`/`.replace` |
| Inconsistent filenames | **Fixed** (`adf2caf`): encoded now `_encoded_r9_full` everywhere |
| Inconsistent suffixes | Consistent: `--sample`→`_sample`, `--full`→`_full`; clean/features chain uses no-suffix full names consistently |
| sample/full mismatches | None remaining (13 encoded refs aligned; clean/features verified) |
| Hardcoded local paths | Only `PROJECT_ROOT` (dynamic) + `RAW_TEST` (**defined, never used** — harmless) |
| Hardcoded Windows paths | None (all via `pathlib`/env) |
| Hardcoded bucket names | None — bucket comes from env `DATA_BASE`/`RAW_TRAIN` |
| Missing env vars | None — every override has a default; cloud submit sets SPARK_ENV/DATA_BASE/RAW_TRAIN/OUTPUT_BASE |
| Missing defaults | None — all `os.environ.get(...)` have defaults |

**Minor (non-blocking) note:** `RAW_TEST` and the lecturer's `solution_*.csv` are
not referenced by any stage (future ground-truth work). No action.

---

## 3. Static dependency audit

| Package | Used by | Version (requirements) | On DataProc 2.1 | Needs install |
|---|---|---|---|---|
| `pyspark` | all stages | 3.5.1 (cluster provides 3.5.x) | ✅ (runtime) | no |
| `pyspark.ml` | clustering (HashingTF, MinHashLSH) | bundled | ✅ | no |
| `numpy` | features | 1.26.4 | ✅ | no |
| `pandas` | features, encoding (pandas_udf) | 2.2.2 | ✅ | no |
| `pyarrow` | pandas_udf transport | 15.0.2 | ✅ | no |
| `h3` | encoding, exact, graph (+transitive: maximal, suffix, clustering) | 3.7.7 | ❌ | **yes** (init action) |
| `datasketches` | approx (in-function import) | 5.0.2 | ❌ | **yes** (init action) |
| `folium` | visualization only (**not cloud-submitted**) | 0.16.0 | ❌ | not needed on cloud |
| `python-geohash` | none (H3 chosen) | — | — | not needed |
| `pytest` | local tests only | 8.2.2 | — | not needed on cloud |

**Conclusion:** the cluster must install exactly **`h3==3.7.7` + `datasketches==
5.0.2`** — done via `--metadata PIP_PACKAGES` + pip-install init action
(commit `1f4a557`). Everything else ships with DataProc 2.1. ✅

---

## 4. Dry execution analysis (command list; no cloud used)

Simulated order from `scripts/dataproc_submit.sh`:

| # | Command | Input | Output | Possible failure | Verify |
|---|---|---|---|---|---|
| 0 | `gsutil mb` / `cp raw` / `cp src.zip` | local files | GCS objects | billing/quota, upload stall | `gsutil ls D/raw/train.csv` (~1.9 GiB) |
| 1 | cluster create (+init pip) | — | 5-node cluster | CPU quota, init pip fail | `gcloud dataproc clusters list` → RUNNING |
| 2 | submit `clean_data.py` | `D/raw/train.csv` | `trips_clean_full.parquet` | RAW_TRAIN unset | `gsutil ls …/_SUCCESS` |
| 3 | submit `feature_engineering.py` | clean | `…_features` | Arrow | `_SUCCESS` |
| 4 | submit `spatial_encoding.py` | features | `…_encoded_r9_full` | h3 missing | `_SUCCESS` + report md |
| 5 | submit `route_mining_maximal.py` | encoded | csv+md | shuffle OOM | job SUCCEEDED; driver counts |
| 6 | submit `route_mining_exact.py` | encoded | csv+md | maxResultSize/OOM | job SUCCEEDED |
| 7 | submit `route_mining_approx.py` | encoded | csv+md | maxResultSize | job SUCCEEDED; `memory ratio` |
| 8 | submit `route_mining_clustering.py` | encoded | csv+md | LSH skew | job SUCCEEDED; "representative sample" |
| 9 | submit `route_mining_graph.py` | encoded | 2 csv+md | skew | job SUCCEEDED; zones |
| 10 | submit `anomaly_analysis.py` | features | csv+md | — | job SUCCEEDED |
| 11 | cluster delete (trap) | — | — | — | `clusters list` empty |

Each `submit` = `gcloud dataproc jobs submit pyspark src/<X>.py --cluster porto
--py-files src.zip --properties <env> -- --full`. Verify any job:
`gcloud dataproc jobs wait <ID> --region $REGION` (streams stdout with the counts).

---

## 5. Output validation plan

| Artifact | Validation |
|---|---|
| `trips_clean_full.parquet` | `_SUCCESS` exists; `spark.read…count()` ≈ 1.66M; schema has `points:array<array<double>>`; driver's valid-% ≈ 97 |
| `…_features_full.parquet` | 1:1 row count with clean; `avg_speed`/`sinuosity` sane (median ~24 km/h / ~1.5); anomaly % single digits |
| `…_encoded_r9_full.parquet` | 1:1 rows; **run `verify_encoding --full`**: 0 invalid H3 cells, compact ≤ raw, cells in Porto bbox |
| `exact_top100_full.csv` | **`verify_route_mining --full`**: brute-force containment support == reported; length ≥ threshold |
| `maximal_frequent_top100_full.csv` | **`verify_maximal --full`**: each route frequent (≥ min_sup) AND maximal; holes reproducible |
| `approx_top100_full.csv` | **`verify_approx_mining --full`**: SS `[lb,ub]` ⊇ exact; CMS ≥ exact; deterministic rerun identical |
| `clustering_top100_full.csv` | **`verify_clustering --full`**: coherence ≥ 0.5 to representative |
| `graph_*_full.csv` | **`verify_graph --full`**: route support re-count matches; zones valid H3, PR sorted |
| `anomalies_top50_full.csv` | **`verify_anomaly --full`**: score == Σ detectors; each detector's semantics |
| Determinism | rerun any mining stage → identical top lists (seeded sampling; sketches fixed-seed) |

> The verifiers accept `--full` and read the `_full` parquet/CSV — run them on the
> cluster (cheap) or locally against downloaded outputs. This is the authoritative
> correctness gate at full scale.

---

## 6. Spark execution expectations

| Stage | Shuffle | Memory | CPU | Network | Bottleneck |
|---|---|---|---|---|---|
| clean | low (read+filter) | low | I/O bound | GCS read 1.9 GB | CSV parse |
| features | **none** (pandas_udf, narrow) | med (Arrow batches) | high | none | UDF CPU |
| encoding | **none** (per-trip udf) | med | high (h3 ×~85M pts) | none | h3 CPU |
| **maximal** | **HIGH** (window groupBy) | **HIGH** | high | high | the ~330M-window shuffle |
| **exact** | **HIGH** (same groupBy) | **HIGH** | high | high | ~190M distinct keys |
| **approx** | **HIGH** (exact agg) + tiny sketch merge | **HIGH** for the exact part | high | med | exact groupBy inside it |
| clustering | med (LSH bands; but capped 50k) | med | high | med | LSH bucket skew (mitigated by cap) |
| graph | med (edge agg + PageRank iters) | med | med | med (Pregel-ish) | super-node skew |
| anomaly | low | low | low | none | none |

**⚠️ Stages needing attention:** `maximal`, `exact`, `approx` **each independently
rebuild the ~190M-distinct support table** (a big wide shuffle with long string
keys). That is the dominant cost and the top OOM/spill risk on 5 workers. If one
strains: raise `--num-workers`, or rely on `approx` (sketches, bounded memory) and
treat the exact list as best-effort. This is the single biggest full-scale unknown.

---

## 7. Monitoring guide (Spark UI, live)

Open the Spark UI via `gcloud dataproc clusters describe porto` (YARN RM → the
running application). Per stage type:

**Heavy mining (maximal/exact/approx) — watch closely:**
- **Shuffle Read/Write** growing to tens of GB — expected; abnormal if a *single
  task* shows shuffle ≫ the others (**skew**).
- **Spill (memory/disk)** columns > 0 and rising fast → memory pressure; some spill
  is OK, heavy spill → slowdown.
- **Task duration skew:** max task ≫ median (e.g. 10×) on the groupBy → hot key
  (downtown). Abnormal.
- **Failed/retried tasks / "ExecutorLostFailure"** → OOM; abnormal if repeated.
- **GC Time** > ~15% of task time → heap pressure.

**Clustering:** watch the `approxSimilarityJoin` stage — one giant task = bucket
skew (should be bounded by the 50k cap; if it still hangs, lower
`CLUSTERING_MAX_TRIPS`).

**Graph:** watch PageRank iteration stages (15) — each a join; steady is fine, a
single ballooning task = super-node.

**Normal vs abnormal quick table:**

| Signal | Normal | Abnormal → action |
|---|---|---|
| Executor failures | 0–1 transient | ≥2 repeated → OOM; add workers / lower parallelism |
| Max/median task time | ≤ ~3× | ≥ ~10× → skew; salt / rely on approx |
| Disk spill | small, transient | GBs and climbing → memory; fewer partitions/more mem |
| Shuffle write | tens of GB on mining | stage stuck with no progress → hung task |
| GC time | < 10–15% | > 20% → heap pressure |
| Job stage retries | 0 | ≥2 → underlying resource problem |

---

## 8. Go / No-Go checklist

Stop the run (No-Go) the moment a condition fails — before wasting budget.

| Step | Condition | GO | NO-GO → recovery |
|---|---|---|---|
| Code baseline | HEAD ≥ `d820a67`, tree clean | proceed | commit/pull; re-zip `src.zip` |
| Local pre-flight | `run_pipeline --sample --verify` all PASS + unit suite green | proceed | fix locally (free); do NOT create cluster |
| Blocker asserts | 4 greps in PREFLIGHT §A3 non-zero | proceed | wrong commit uploaded → re-zip |
| Bucket/APIs | `gsutil ls $BUCKET` ok; dataproc+storage enabled | proceed | enable billing/APIs |
| Raw upload | `gsutil ls D/raw/train.csv` ~1.9 GiB | proceed | re-upload |
| Cluster up | 5 nodes RUNNING; init pip succeeded | proceed | quota → 3 workers; init fail → recreate |
| **Canary: clean_data** | job SUCCEEDED, `_SUCCESS`, ~97% valid | proceed | RAW_TRAIN/env issue → fix props, resubmit clean only |
| encoding | `_SUCCESS`, avg_compact ~17 | proceed | h3 missing → recreate cluster with init action |
| each miner | job SUCCEEDED; counts sane | continue | OOM/skew → §6/§7 action; resubmit that stage only |
| Verifiers | `verify_*_full` PASS | accept results | investigate mismatch before trusting numbers |
| Teardown | cluster deleted; list empty | done | delete manually |

**Canary principle:** submit **`clean_data` alone first** and confirm SUCCEEDED +
`_SUCCESS` before submitting the rest. It exercises RAW_TRAIN, gs:// read/write,
and env wiring for the cost of one cheap job.

---

## 9. Final readiness verdict

**Remaining technical risks**
- The three heavy mining stages each rebuild the ~190M-key support table (wide
  shuffle, long string keys) — **unmeasured above 100k**; top OOM/spill/skew risk.
- Long string keys inflate shuffle bytes (hashed-key hardening documented, not
  applied — deferred deliberately).
- DataProc env-var delivery (`appMasterEnv`) assumed to reach the driver (standard,
  but not personally tested on this account).

**Remaining operational risks**
- First-time cloud setup (project/billing/quota) outside the code's control.
- Output CSVs land on the driver's local disk (`open()` can't write gs://) — must
  read numbers from driver stdout or SSH-copy before teardown.
- Budget discipline depends on `--max-idle` + delete trap not being removed.

**Confidence level: ~85%** that the pipeline completes correctly on the first run,
**conditional on the local pre-flight (PREFLIGHT §A) passing green**. The
prefix (clean→features→encode) and Methods A/C/anomaly are low-risk and validated
to 50k–100k; the ~15% uncertainty is concentrated in the exact/maximal/approx
support-table shuffle at 1.71M on 5 workers.

**Unknown until the first DataProc run**
- Real full-scale runtimes and whether the exact-mining shuffle fits 5 × n2-std-4
  without excessive spill.
- Actual long-route (10–40 km) support magnitudes on full data.
- That `appMasterEnv` env vars reach the driver on this project.
- Real memory/shuffle-byte figures (to replace the estimates in FINAL_REPORT).

**Would I approve spending the budget?** **Yes — conditionally.** Approve *after*
`PREFLIGHT §A` is green on the current commit, and run **`clean_data` as a canary
first**. If the canary and encoding succeed, the remaining risk is confined to the
three heavy mining stages, each of which fails safe (resubmit that stage only;
`approx` is the bounded-memory fallback). Given the two real blockers were already
caught and fixed for free, and every known failure mode has a documented
recovery, the expected budget spend is a few dollars for a high-probability
success. Do **not** proceed if the local pre-flight is red.
