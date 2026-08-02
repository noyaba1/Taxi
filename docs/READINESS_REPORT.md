# Submission & DataProc Readiness Report

> ⚠️ **Historical.** This records the state *before* the cloud run, which has
> since been executed (2026-07-26). It is kept because the dry runs described
> here found two real bugs, and that record is worth having. For current results
> read [`FINAL_REPORT.md`](FINAL_REPORT.md) §9d; for the canonical cloud
> procedure read [`DATAPROC.md`](DATAPROC.md).

Prepared before spending the $50 cloud budget. Scope: verify docs against code,
harden for scale, and run the largest safe local execution. **No new algorithms;
no architecture changes.**

---

## 1. What was reviewed

- **Every doc vs the actual code**: README roadmap, ARCHITECTURE data-flow +
  per-phase claims, DATAPROC guide, SETUP, FINAL_REPORT.
- **DataProc readiness**: `config.py` paths, the cloud env switch
  (`spark_session.py`), the submit script, Spark/partition/memory settings,
  serialization, checkpoints, temp-output handling, `.gitignore`, env vars.
- **Runtime behaviour at scale**: a real local dry run at **50k and 100k trips**
  (10-20x the validated 5k), every stage + every verifier + unit tests.

## 2. What was fixed

| Area | Issue found | Fix | Commit |
|---|---|---|---|
| Approx mining | `mapPartitions(...).collect()` of all sketch bundles blew `spark.driver.maxResultSize` (13 × ~80 MB > 1 GB at 50k) — **would crash the cloud run** | merge sketches on executors via **`treeReduce`**; driver gets one ~65 MB bundle regardless of scale | `6dddce5` |
| Clustering | LSH self-join edges grow ~quadratically; bucket **skew** crashed a Python worker at 100k | cluster a **representative sample** capped at `CLUSTERING_MAX_TRIPS` (50k, proven stable) | `6dddce5` |
| Cloud paths | `Path("gs://…")` collapsed the scheme → broken GCS URIs | `config.storage_join` preserves `gs://` | `d5b8e65` |
| Cloud submit | `RAW_TRAIN` never set → `clean_data --full` would read a local path and **fail on the cluster** | submit script/env now sets `RAW_TRAIN`, `OUTPUT_BASE`, `shuffle.partitions=200` | `3507274` |
| Local scale | fixed 4 GB driver / 16 partitions (5k-tuned) | env-overridable `SPARK_DRIVER_MEM`, `SPARK_SHUFFLE_PARTS` | `6dddce5` |
| Docs | data-flow diagram claimed `cell_seq[]/cum_km[]` (never produced); README roadmap mapped Method C to the wrong file; listed non-existent `eda.py` | corrected to actual columns/files; `cum_km` marked deferred | `6dddce5` |
| Output retrieval | `open()` can't write gs://; reports would strand on driver | stages print results (captured in driver output); SSH-copy path documented | `3507274` |

## 3. What was verified

- **All 9 verifiers PASS at 50k** (route-mining, suffix, approx, maximal,
  clustering, graph, anomaly) + **60 unit tests** green.
- **M5/M7 confirmed to 100k**: exact mining 108 s (9.6M→19M windows, no OOM);
  approx 208 s with the `treeReduce` fix holding at 96 partitions.
- **Approximate method's scaling advantage demonstrated**: memory ratio grew from
  3.8× (5k) to **32.5× (50k)** — exact grows with 5.5M keys, sketches stay fixed.
- **Long-route sparsity confirmed as a sample artifact**, not a bug: at 50k the
  10 km top-support rose to 15 (was 3 at 5k), 20 km to 2 — real support emerges
  with data, as documented.
- **Doc claims now match code** (spot-checked the encoded schema, module names,
  the cloud switch, and the run_pipeline stage list against the filesystem).

### Measured metrics — validated stable scale (50k trips, local, 10 g / 64 parts)

| Stage | Wall | Notes |
|---|---|---|
| clean / features / encode | 16 / 25 / 50 s | linear, no shuffle risk |
| M5 exact | 51 s | 9,618,058 windows → 5,465,128 distinct |
| M6 maximal(closed) | 94 s | |
| M7 approx | 129 s | **32.5× less memory**; shuffle 9.6M rows → 14 bundles |
| M8 min-support maximal | 117 s | 320 maximal-frequent @ X=0.5% |
| M9 clustering | 92 s | 243k edges → 2,914 clusters |
| M10 graph + zones | 89 s | 5,620 nodes; 1,513 validated corridors; 50 zones |

## 4. Remaining risks

| Risk | Severity | Mitigation / status |
|---|---|---|
| **Exact-mining shuffle at 1.71M** (~260M window rows, long string keys) | Med-High | biggest cloud cost; handled by 5 workers + AQE + `shuffle.partitions=200`; the **approx job is the fallback**; hashed-key hardening documented (DATAPROC §8) but **not yet applied** |
| Env-var delivery on Dataproc (`appMasterEnv`) | Low-Med | standard mechanism; triage row in CLOUD_CHECKLIST if a stage can't see `DATA_BASE/RAW_TRAIN` |
| Clustering only sees a 50k sample on cloud | Low | intentional; popular corridors are well-represented; raise `CLUSTERING_MAX_TRIPS` if the cluster has headroom |
| Report CSVs land on driver local disk | Low | results also printed to captured driver output; SSH-copy documented |
| Cost overrun | Low | `--max-idle` + delete `trap` + one-shot run |
| Full run **not yet executed** anywhere | — | this is the remaining project step, by design |

## 5. Readiness verdict

| Target | Ready? | Notes |
|---|---|---|
| **Medium local runs (≤ 50k)** | ✅ **Yes** | fully validated end-to-end; use `SPARK_DRIVER_MEM=10g SPARK_SHUFFLE_PARTS=64` |
| **Full local run (1.71M on this laptop)** | ⚠️ **Not advised** | the exact-mining groupBy (~190M distinct string keys) and LSH skew exceed a single 32 GB machine; this is expected — it's why the project targets a cluster |
| **First DataProc execution** | ✅ **Ready** | all known cloud-blockers fixed (RAW_TRAIN, gs:// paths, maxResultSize, clustering cap); follow `docs/CLOUD_CHECKLIST.md` |

## 6. Recommended actions before spending the cloud budget

1. **Push confirmed** — ensure the cluster pulls commit `3507274` or later
   (`src.zip` must be zipped from the current `src/`, which the submit script does).
2. **Dry-validate the submit script variables** (`PROJECT/BUCKET/REGION`) and that
   `gsutil ls $BUCKET` works — before creating the cluster.
3. **Run cheap-first**: consider one job (`clean_data`) end-to-end to confirm the
   env wiring (RAW_TRAIN, gs:// read/write) before submitting the whole pipeline.
4. **Keep the cluster small and delete promptly** (`--max-idle 30m` + trap).
5. **Read results from driver output**; SSH-copy CSVs only if needed.
6. After the run, **fill the real full-scale numbers** into `FINAL_REPORT.md`
   §6/§8 and re-check the long-route (10-40 km) supports — only then is the
   project complete.

**Bottom line:** the repository is consistent, the pipeline is validated and
stable to 50k locally with the two real scale-bugs fixed, and every known cloud
failure mode has been closed or documented with a triage step. It is ready for a
first, budget-safe DataProc execution.
