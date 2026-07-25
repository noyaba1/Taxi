# Live Monitoring Guide — DataProc Execution

> ⚠️ **Superseded by [DATAPROC.md](DATAPROC.md).** This run-book predates the
> storage-layer fix: it points `OUTPUT_BASE` at the master's `/tmp`, which the
> cluster teardown then destroys, uses `--num-workers 4`, and reads the raw file
> from `train.csv/train.csv`. Kept for its narrative and monitoring detail only —
> follow DATAPROC.md for the commands.


What to watch while the pipeline runs, so we spot trouble early and stop before
wasting budget. Stages cluster into four behavioural types; watch each type the
same way.

**Open the Spark UI:** Cloud Console → Dataproc → Clusters → `porto` → **Web
Interfaces** → *Spark History Server* (or the running application's *Application
Master*). Also useful: *YARN ResourceManager*.

**Tabs you will use:** **Jobs** (progress), **Stages** (shuffle/skew/spill),
**Executors** (failures/GC/memory), **SQL** (query plan), **Storage** (cache).

---

## Type 1 — Narrow prefix (clean, features, encoding)

| Metric | Expected (healthy) | Unhealthy → action |
|---|---|---|
| Executors | all alive, evenly busy | executors dying → OOM (rare here); reduce `arrow.maxRecordsPerBatch` |
| Shuffle | **≈ 0** (map-only) | any large shuffle here = wrong stage/plan |
| CPU | high on features/encoding (UDF/h3) | idle = stuck task / GCS read stall |
| Memory | moderate (Arrow batches) | steady climb + spill = batch too big |
| Network | GCS read (clean) then low | sustained high = unexpected shuffle |
| Task duration | uniform across tasks | one long straggler = a huge partition |
| Healthy sign | steady linear progress in **Jobs** | — |
| Unhealthy sign | a stage stuck at "N-1/N tasks" | one task hung → check its executor logs; kill+retry |

These are low-risk. If clean/features/encoding progress steadily, move on.

---

## Type 2 — Heavy mining shuffle (maximal, exact, approx) ⚠️ WATCH CLOSELY

This is the `groupBy(subroute)` over ~330M window rows → ~190M distinct long-string
keys. **The main budget and failure risk.**

| Metric | Expected (healthy) | Unhealthy → action |
|---|---|---|
| **Shuffle Read/Write** (Stages tab) | tens of GB, growing steadily | growth stalls with no task progress = hung; or ballooning far beyond expectation |
| **Task skew** (Stages → task table, max vs 75th pct) | max ≤ ~3× median | **max ≥ ~10× median** = hot downtown key → skew |
| **Spill (memory/disk)** columns | small/transient | **GBs and climbing** = memory pressure → will slow/OOM |
| **Executors** tab: Failed Tasks | 0-1 transient | ≥ 2, or `ExecutorLostFailure` = OOM → add workers / rely on approx |
| **GC Time** (Executors) | < 10-15% of task time | > 20% = heap pressure |
| **Stage retries** (Stages) | 0 | ≥ 1 retry of a whole stage = resource problem |
| Task duration | most similar, a few longer | a single task running many× longer = the skewed reducer |

**Immediate recovery if unhealthy:**
1. Skew only, still progressing → let it finish (AQE skew-join may split it).
2. Heavy spill / executor loss → **cancel**, delete cluster, recreate with
   `--num-workers 6-8`, resubmit just that stage.
3. If exact/maximal keep failing but **approx SUCCEEDED** → accept approx's top-100
   as the result (bounded memory by design) and move on. This is the safety valve.

---

## Type 3 — Clustering LSH (Method A)

Input is capped to `CLUSTERING_MAX_TRIPS` (50k), so this is bounded.

| Metric | Expected | Unhealthy → action |
|---|---|---|
| `approxSimilarityJoin` stage | balanced tasks, finishes in min | one giant task hanging = LSH bucket skew |
| Shuffle | moderate | one huge partition = dense bucket |
| Driver | brief collect of the edge list (small) | driver OOM = edge list too big (cap not applied → check code version) |
| Healthy sign | driver prints `representative sample: 50,000 of …` then `clusters: N` | hangs at the join with no progress |

**Recovery:** set `$E.CLUSTERING_MAX_TRIPS=30000` in `PROPS` and resubmit only this
stage.

---

## Type 4 — Graph PageRank (Method C)

| Metric | Expected | Unhealthy → action |
|---|---|---|
| Iteration stages | ~15 similar join stages, steady | one stage ballooning = super-node |
| Shuffle per iteration | small-moderate, stable | growing each iteration = message blow-up |
| `localCheckpoint` | truncates lineage (no plan growth) | plan/DAG growing unbounded = checkpoint not taking |
| Healthy sign | driver prints `validated corridors` + `50 zones` | — |

**Recovery:** raise `GRAPH_MIN_EDGE_SUPPORT` (prunes rare edges → smaller graph),
resubmit only this stage.

---

## Spark UI tabs — what to watch, and when

| Tab | Watch for | During |
|---|---|---|
| **Jobs** | overall progress bar per job; a job stuck = trouble | all stages |
| **Stages** | shuffle read/write, task duration distribution, spill, retries | mining/graph |
| **Executors** | Failed Tasks, GC Time, Storage/On-Heap used, executor deaths | mining |
| **SQL** | the physical plan (exchange/sort/hashaggregate) | to confirm one big shuffle, not many |
| **Storage** | cached DataFrames (features/windows) actually cached | mining/graph |

---

## Screenshots to capture (for the presentation & report)

| Screenshot | Where | Why it matters |
|---|---|---|
| Cluster config (5 machines) | Dataproc → cluster details | proves ≥5-machine requirement met |
| A heavy-mining **Stages** page | Spark UI Stages | shows real shuffle read/write GB — the scalability story |
| **Executors** page | Spark UI Executors | executor count, memory, GC — resource utilisation |
| Exact vs approx driver output | job output | the **32×+ memory ratio** at full scale (headline) |
| The top-100 route table(s) | job output | the actual deliverable |
| Activity-zones map + routes map | local `porto_map_full.html` | the visual headline |
| A-vs-B-vs-C comparison table | `evaluation --full` output | "which method is best" answer |
| Billing spend for the day | Console → Billing | proves budget discipline |

## Metrics to record (these go into the presentation)

- Per-stage **runtime** (from job durations) and **shuffle read/write** (Stages).
- **Exact vs approximate**: precision@100, memory ratio, shuffle rows vs sketch
  bundles — the approximate-algorithms story.
- **Row counts**: 1.71M raw → valid → encoded; window emissions → distinct routes.
- **Top-support** per length threshold (especially 10-40 km now being non-trivial).
- **Cluster spec** (5× n2-standard-4) + **wall-clock** + **estimated cost**.

Record them live in `docs/DATAPROC_RESULTS_TEMPLATE.md`.
