# Project Status & Team Brief

> ⚠️ **Partly historical. The DataProc run described below as "next" has been
> executed** — 1 master + 5 workers, 1.71M trips, all I/O on `gs://`, 2026-07-26.
> For current state read [`FINAL_REPORT.md`](FINAL_REPORT.md) §9d (results) and
> [`cloud_evidence/`](cloud_evidence/) (proof); **where this file disagrees with
> those, they win.** §1–§5 below are still accurate as orientation; §6 has been
> updated to what actually remains.

For the whole team. If you have not followed the work closely, read this first — it
explains what the project is, what is done, and what happens next. Everything is
on branch **`Noya`**.

---

## 1. Objective

Analyse the **Porto Taxi** trajectory dataset (1,710,670 trips, 442 taxis, GPS
every 15 s) with **Apache Spark**, and find the **top-100 popular long sub-routes**
for minimum lengths {1, 3, 5, 10, 20, 40} km — using **three different methods**
plus **approximate (sketch) algorithms** — and also the city's **activity zones**
and **anomalous routes**. The final run must execute on **Google Cloud DataProc
(5 machines)** over **GCS**, and results are shown on a **map**.

## 2. Architecture (one idea to remember)

After encoding, **each trip becomes a string of H3 cells**. So a "sub-route" is a
substring, "popular" = how many trips contain it, "long" = ground length ≥ L km.
That single representation feeds all four methods (A clustering, B maximal-frequent, C transition graph, D suffix array). Data flows through a medallion
pipeline (raw → clean → features → encoded → mining), every stage reading/writing
**Parquet**, and **one file (`config.py`) holds all paths** — switching to the
cloud is just two environment variables.

## 3. Pipeline overview

```
raw CSV → clean → features → H3 encode →  ┌ Method B (frequent sub-routes)
                                          ├ Method A (clustering)
                                          ├ Method C (transition graph + zones)
                                          └ anomaly detection
                              → comparison + map (local post-processing)
```

## 4. What is already completed

Everything below is **implemented and validated on samples up to 50k trips**, each
with an independent `verify_*.py` checker.

| Section | What it does | Why it exists | Status | Output |
|---|---|---|---|---|
| **Environment setup** | Java 11 / Python 3.11 / Spark 3.5.1, Windows fixes | make Spark run locally + mirror the cloud | ✅ | working env (`validate_env`) |
| **Data cleaning** | parse POLYLINE, drop corrupt trips | trustworthy input | ✅ | `trips_clean.parquet` (~97% valid) |
| **Feature engineering** | distance, speed, bbox, sinuosity, anomaly flags | trip-level signals | ✅ | `…_features.parquet` |
| **H3 encoding** | trajectory → ordered H3 cell sequence (res 9) | discretise routes for comparison | ✅ | `…_encoded_r9_*.parquet` |
| **Method B** | exact + closed + **min-support X% maximal** sub-route mining | the PDF's "popular long sub-route" (with "holes") | ✅ | `maximal_frequent_top100`, `exact_top100` |
| **Method A** | MinHash-LSH clustering of similar trips, then the longest cell run shared by >=60% of a cluster | corridors as SUB-routes (not whole trips) | ✅ | `clustering_top100` |
| **Method C** | transition graph → PageRank zones + heavy-path routes | movement-network view + **activity zones** | ✅ | `graph_heavy_paths_top100`, `activity_zones` |
| **Approximate** | Space-Saving + Count-Min vs exact | scale + the approximate-algorithms requirement | ✅ | `approx_top100` + memory/accuracy metrics |
| **Anomaly detection** | 5 detectors (speed/idle/distance/shape/drift) | the "anomalous routes" requirement | ✅ | `anomalies_top50` |
| **Visualization** | Folium map + Colab notebook | the map demo | ✅ | `porto_map_*.html`, notebook |
| **Validation** | a `verify_*` per stage + the unit suite | prove correctness independently | ✅ | all PASS at 50k |
| **Release audit** | per-stage release review | catch cloud failures before they cost money | ✅ | `RELEASE_AUDIT.md` |
| **Pre-flight** | local gate + dry runs (50k/100k) | budget protection; found & fixed 2 real bugs | ✅ | `PREFLIGHT.md`, `READINESS_REPORT.md` |
| **Documentation** | design, cloud runbooks, monitoring, workbook | run the cloud step without guessing | ✅ | see §5 |

## 5. Repository status — which document to use when

| Document | Use it when |
|---|---|
| `README.md` | first look: what/why, phase status, quick run |
| `SETUP.md` | setting up the local environment (Java/venv/winutils) |
| `docs/ARCHITECTURE.md` | understanding design + per-phase algorithm detail |
| `docs/DESIGN_REVIEW.md` | why each algorithm was chosen over alternatives |
| `docs/FINAL_REPORT.md` | the comprehensive engineering report (self-scored) |
| `docs/RELEASE_AUDIT.md` | per-stage release audit (I/O, deps, failure modes) |
| `docs/READINESS_REPORT.md` | what the dry run found and fixed |
| `docs/PREFLIGHT.md` | **do this first** before the cloud run (free local gate) |
| **`docs/CLOUD_RUN_PLAYBOOK.md`** | **the canonical step-by-step cloud runbook (20 steps)** |
| `docs/CLOUD_CHECKLIST.md` | condensed copy-paste version (superseded by the playbook) |
| `docs/DATAPROC.md` | reference/rationale + scale-hardening notes |
| `docs/LIVE_MONITORING_GUIDE.md` | what to watch in the Spark UI during the run |
| `docs/DATAPROC_RESULTS_TEMPLATE.md` | **fill this in live** during the cloud run |
| `docs/TEAM_HANDOFF_HE.md` | Hebrew onboarding for the team |
| `scripts/dataproc_submit.sh` | one command to run the whole cloud pipeline |
| `notebooks/porto_routes_colab.ipynb` | render the map in Colab Enterprise |

## 6. What remains before submission

### Done (was "next" in the original version of this file)
- ✅ **DataProc run** — 1 master + 5 workers, `n2-standard-4`, image `2.2.84-debian12`,
  1,710,670 trips, every input and output on `gs://`. 60 min wall, $2.71 total.
- ✅ **Runtime metrics + results in `FINAL_REPORT.md` §9d** — measured, not estimated.
- ✅ **Cloud correctness verified** — `verify_cloud_run`; Method D reproduced
  420/420 corridors bit-identically against the local full-scale baseline.
- ✅ **Presentation** — `scripts/build_deck.py` → 27-slide deck, every figure read
  from the generated reports rather than retyped.
- ✅ **Developer guide** — `scripts/build_devdoc.py` → `.docx`.

### Actually remaining
1. **Re-run Method A (`route_mining_clustering`)** — a cluster-internal counting
   fix landed after the graded run (FINAL_REPORT §10.9): gap-split trips could
   satisfy the "≥60% of members" test more than once. `support` was never
   affected (it is recounted globally), but Method A's published extents are
   stale until re-measured. Cheapest path: `ONLY="route_mining_clustering.py"
   bash scripts/dataproc_submit.sh`, or a local `--full` run.
2. **Re-run M7 at mid/full** to pick up populated `length_km` in `--approx-only`
   output (§10.10). Both re-runs can share one short cluster.
3. **Ship a `results/` bundle** — `outputs/` is gitignored, so the archive
   currently contains no route tables. A reviewer cannot reproduce a single
   number without bucket access. Keep the top-route CSVs + summary reports.
4. **Moodle submission** — one student submits and links all members.

## 7. How a cloud execution runs

Written before the first run and kept, because §6's two re-runs follow the same
procedure — shorter, since only one or two stages are submitted.

- **Roles:** one person drives (`gcloud`/Cloud Shell + playbook), one watches the
  **Spark UI** (monitoring guide), one records numbers in the **workbook**.
- **Duration:** roughly **1–1.5 hours** wall-clock end to end; a few dollars of budget.
- **Rhythm:** stages run one at a time; the **critical path** is clean → features →
  encode (~15–25 min), then the analysis stages. The **heavy trio**
  (maximal / exact / approx) takes the most time — this is where to watch the Spark
  UI for shuffle/skew/spill.
- **If something fails:** it's almost always recoverable by **resubmitting only that
  stage** (upstream Parquet is already saved). The approximate job is the fallback
  if the exact mining strains the workers.
- **Golden rules:** run the **canary** first; **delete the cluster** the moment the
  last job finishes; read results from the job driver output; record everything.

**Bottom line:** the pipeline has run end to end on DataProc and the results are
in `FINAL_REPORT.md` §9d. What is left is §6's four items — two re-runs that
supersede stale Method A / M7 numbers, the results bundle, and the submission.
