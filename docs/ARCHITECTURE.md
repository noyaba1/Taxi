# Architecture & Design — Porto Taxi Route Mining

> Audience: the author (for defense) and the lecturer. This document explains
> **what we build, why, how it scales, where it breaks, and why each choice beats
> the alternatives.** Code is referenced, not duplicated.

---

## 1. The problem, reframed (the key insight)

The assignment asks for the **top-100 popular long sub-routes** for minimum
lengths {1, 3, 5, 10, 20, 40} km, using three different method families plus
approximate data structures.

The unifying reframing that drives the whole design:

> **Encode each trajectory as a sequence of spatial cells (H3) → a trip is a
> STRING over an alphabet of cell-IDs.**
> - a *sub-route* = a contiguous **substring** of cells,
> - *popular* = high **support** (appears in many trips),
> - *long* = ground length ≥ L km.

So the core task is **frequent contiguous-substring mining over ~1.7M strings,
returning the top-100 by support per length threshold.** This is cheaper than
general sequential-pattern mining (e.g. PrefixSpan) because we only need
*contiguous* substrings, not gapped subsequences.

Every required method is a different lens on this one problem:

| Required method | Lens on the problem |
|-----------------|---------------------|
| A. Clustering | group *similar whole routes*, take cluster representatives |
| B. Suffix array/tree | enumerate/count *frequent substrings* directly |
| C. Original (ours: graph) | treat cells as a *weighted transition graph*, mine heavy paths |
| D. Approximate structures | make counting/dedup/similarity **cheap at scale** |

---

## 2. System architecture (medallion data flow)

We use a Bronze → Silver → Gold "medallion" layout. Each phase reads the
previous layer and writes the next as **Parquet** (columnar, compressed,
schema-on-read). Nothing recomputes upstream work.

```
 RAW CSV (1.9 GB)                                   ── Bronze (immutable input)
   │  load_data.py (explicit schema)
   ▼
 trips_clean.parquet                                ── Silver  (Phase 1)
   │  feature_engineering.py (Haversine, speed, bbox, anomaly)
   ▼
 trips_features.parquet                             ── Gold-features (Phase 2)
   │  spatial_encoding.py (H3 cell sequences, denoised)
   ▼
 trips_encoded.parquet  (TRIP_ID, cell_seq[], cum_km[])   (Phase 4)
   │            ├──────────────┬───────────────┐
   ▼            ▼              ▼               ▼
 routes_suffix  routes_cluster routes_graph   stats/  (Phases 3,5,6,7)
   └──────────────┴───────────────┴───────────► evaluation + viz (Phase 8)
```

**Partitioning strategy.** Trajectory-level work keeps **one whole trip per
row** (never split a trip across partitions — a route can cross the city).
The only large shuffle is *counting* in the mining phases; we attack that with
map-side combine, salting, and sketches (§7).

---

## 3. Local vs DataProc execution (one codebase)

| Concern | Local (now) | DataProc (final run) |
|---------|-------------|----------------------|
| Storage | `data/` folder (`DATA_BASE`) | `DATA_BASE=gs://bucket/porto` |
| Master | `local[*]` set in `spark_session.py` | `SPARK_ENV=cloud` → YARN sets it |
| Memory / shuffle parts | small, in `config.py` | from cluster config (≥5 machines) |
| Submit | `python -m src.<phase>` | `gcloud dataproc jobs submit pyspark` |
| Windows shims | non-ASCII path + winutils auto-fix | **no-op** (`os.name != 'nt'`) |

The whole cloud switch is **two env vars**. This is deliberate: all paths flow
through `config.py`, and `spark_session.py` is the only place that knows about
master/memory. No business logic mentions a path or a cluster.

**Cost discipline:** debug on the 5k sample (seconds) → run once on the full
local file → one clean DataProc run for final timings. Never debug in the cloud.

---

## 4. Phase-by-phase design

For each phase: **goal · idea · complexity · distributed notes · bottleneck ·
memory · scalability · why this over alternatives.**

### Phase 1 — Cleaning & parsing  ✅ implemented & validated
- **Goal:** raw CSV → validated trips with parsed trajectory.
- **Idea:** explicit schema; `from_json` POLYLINE → `array<array<double>>`;
  derive `n_points`, `duration=(n-1)·15s`, start/end coords; drop invalid.
- **Complexity:** O(N) single pass, no shuffle.
- **Distributed:** embarrassingly parallel; CSV split by Spark across cores.
- **Bottleneck:** CSV parse of 1.9 GB (I/O bound). Mitigated by writing Parquet
  once so later phases never re-parse.
- **Why these choices:** explicit schema avoids a full extra `inferSchema` read;
  Parquet output is 5–10× smaller and keeps the *parsed* array.
- **Validated results & the two Windows fixes** (non-ASCII path, winutils):
  see [SETUP.md](../SETUP.md) §6/§6b. Full file = 1,710,670 trips; sample
  5,000 → 4,867 valid (97.3%); drops = 101 too-few-points + 32 outside-bbox.

### Phase 2 — Feature engineering  ✅ skeleton implemented
- **Goal:** per-trip Haversine distance, avg/max speed, bbox, sinuosity, anomaly
  flags.
- **Idea & key decision:** an **Arrow-vectorized `pandas_udf`** processes each
  trajectory array in one pass.
- **Complexity:** O(total GPS points) ≈ O(85M) but **no shuffle**.
- **Alternative rejected:** `explode` points → window `lag` → re-aggregate. That
  turns 1.7M rows into ~85M and forces a **shuffle + re-group**. The `pandas_udf`
  keeps one row per trip and runs per-executor via Arrow (not driver-side
  pandas), so it parallelizes linearly on DataProc.
- **Bottleneck / memory:** Arrow batch size; bounded by `spark.sql.execution
  .arrow.maxRecordsPerBatch`. No skew (per-trip independent).

### Phase 3 — Statistics & EDA
- **Goal:** distributions of distance, duration, speed; activity by hour/day;
  defensible data-quality summary.
- **Key decision — approximate aggregates:** percentiles via **T-Digest/KLL**
  (`datasketches`) or Spark `approxQuantile` (GK-sketch) instead of a full sort;
  distinct taxis/trips per zone via **HyperLogLog** (`approx_count_distinct`).
- **Complexity:** exact quantiles need an O(N log N) sort + shuffle; sketches are
  O(N) single pass, mergeable, **O(1)-ish memory**. This is the first place we
  *demonstrate* approximate vs exact (accuracy/runtime trade-off) for the report.
- **Output:** small aggregates only → safe to `toPandas()` for plotting. We
  never `collect()` raw trajectories.

### Phase 4 — Spatial encoding (H3 vs Geohash vs S2)
- **Goal:** map each GPS trajectory to a **denoised sequence of cells**.
- **Recommendation: H3 (hexagonal).** Justification table:

  | Property | Geohash | S2 | **H3** |
  |----------|---------|----|--------|
  | Cell shape | rectangles (vary with lat) | quadrilaterals | **hexagons (uniform)** |
  | Neighbor distance | unequal (edge vs corner) | unequal | **equal to all 6 neighbors** |
  | Movement modelling | diagonal ambiguity | better | **best (a route = walk on hex grid)** |
  | Python ecosystem | ok | thinner | **excellent (`h3`)** |
  | Used for ride/flow data | — | maps | **Uber, for exactly this** |
  | Weakness | prefix edge problem | API complexity | not a strict hierarchy (pentagons) |

  Hexagons matter because a route is **movement between adjacent cells**; with
  squares, orthogonal vs diagonal neighbors have different distances and create
  ambiguous transitions. H3 removes that distortion.

- **Resolution: H3 res 9 (~174 m edge, ~0.1 km²) — confirmed empirically (M3).**
  Quantitative justification: a taxi at 50 km/h moves ~210 m per 15 s sample ≈ ~1
  res-9 cell, so consecutive GPS samples land in adjacent cells → the cell-sequence
  faithfully tracks the route. Res 8 (~461 m) loses route shape; res 10 (~65 m)
  lets GPS noise flip cells and fragments identical routes.

  **Sweep evidence (5k sample):** `len_ratio` = encoded length / GPS path length
  (1.0 = ideal); `compression` = raw cells / compact cells (denoising power).

  | res | edge | avg compact cells | compression | len_ratio |
  |----|------|------|------|------|
  | 8 | 461 m | 8.0 | 7.44x | 1.208 |
  | **9** | **174 m** | **17.5** | **3.36x** | **1.158** |
  | 10 | 66 m | 30.0 | 1.83x | 1.07 |

  **Why 9 wins (not 10, despite its better len_ratio):** for *frequent-substring
  mining* the goal is that identical real routes produce identical strings while
  distinct routes stay distinct. Res 10 barely denoises (1.83x) → GPS jitter
  fragments identical routes into different strings → support is undercounted and
  sequences are ~2x longer (bigger shuffle). Res 8 over-collapses (7.44x) → merges
  distinct streets → false support and worst length error (1.208). Res 9 is the
  balance: meaningful denoising with preserved route shape. All `len_ratio`s are
  within ~20%, so length accuracy is not the deciding factor; route-matching
  stability is. (Reports: `outputs/statistics/h3_resolution_comparison_*.md`.)
- **Denoising (critical for substring matching):** map points→cells, then
  **run-length collapse consecutive duplicates**, and **gap-fill** non-adjacent
  hops with `h3.h3_line`. Without this, jitter splits identical routes and
  support collapses.
- **Length handling:** precompute a **cumulative-km array** per route (sum of
  cell-center Haversines) so any window's km-length is O(1) — accurate, not the
  crude "k cells × 0.174 km".
- **Distributed:** pure per-trip `pandas_udf` (H3 calls vectorized per batch);
  no shuffle. Output `cell_seq: array<long>`, `cum_km: array<double>`.

### Phase 5 — Method B: suffix/n-gram frequent sub-routes
- **Goal:** count frequent contiguous sub-routes; top-100 per length threshold.
- **Two designs, both implemented & compared:**
  1. **Distributed k-gram counting (baseline):** for each L pick window length,
     emit windows whose `cum_km` span ≥ L, `reduceByKey` count, top-100.
     Embarrassingly parallel map + one shuffle.
  2. **Generalized suffix array (the "suffix" requirement):** build per-partition
     suffix arrays to extract **maximal frequent substrings** in one structure,
     avoiding re-enumeration per length. A popular 10 km route implies popular
     sub-windows at every shorter length; the suffix structure captures this
     hierarchy and yields *maximal* routes, which we then truncate per threshold.
- **Complexity:** k-gram = O(Σ(nᵢ−k)) emits per trip, dominated by the shuffle.
  Suffix array = O(n) build per partition (DC3/SA-IS), O(n) for repeats.
- **Bottleneck — KEY SKEW:** downtown cells appear in a huge fraction of trips →
  a few k-grams are massive heavy-hitters → reducer hot spots. Mitigations:
  map-side combine, **salting** hot keys (two-stage agg), or replace exact count
  with **Count-Min + heavy-hitters** (Phase 7) to sidestep per-key reducers.
- **Why both:** k-gram is the scalable workhorse; the suffix array is the
  elegant, redundancy-free method the assignment names and gives us *maximal*
  routes for free.

### Phase 6 — Method A: clustering-based route discovery
- **Goal:** group similar whole routes; cluster representatives = popular routes.
- **Design:** represent each trip as the **set of its H3 cells** → approximate
  similarity self-join with **MinHash-LSH** (Spark MLlib `MinHashLSH`, Jaccard)
  → build a trajectory similarity graph → **connected components / label
  propagation** (GraphFrames) → per-cluster **consensus cell-sequence** =
  the popular route; keep clusters whose consensus length ≥ L.
- **Complexity:** naive pairwise trajectory distance (Hausdorff/DTW/Fréchet) is
  **O(N²)** — infeasible at 1.7M. LSH makes it ~**O(N · b)** (b = bucket size),
  which is the whole point of using an approximate structure here.
- **Alternatives rejected:**
  - *KMeans on cell-membership vectors:* needs a fixed k and a metric ill-suited
    to variable-length routes; Euclidean on sparse 0/1 vectors ≠ route similarity.
  - *TraClus (segment DBSCAN):* academically classic but DBSCAN is hard to
    distribute and parameter-sensitive. We **cite it** as the inspiration and use
    the LSH+graph approach as its scalable cousin.
- **Bottleneck:** LSH bucket skew (dense areas) + GraphFrames iteration
  communication. Mitigate with bucket-size caps and bounded iterations.

### Phase 7 — Method C (original): transition-graph heavy-path mining + sketches
- **Goal:** a third method that is **neither clustering nor suffix-based**.
- **Idea:** build a **directed weighted graph** where nodes = H3 cells and edges
  = observed consecutive transitions, weighted by frequency (and by distinct
  taxis via HLL). Then:
  - **activity zones** = high-PageRank / high-degree nodes (objective: zones),
  - **popular long routes** = highest-weight paths of ground-length ≥ L, found by
    greedy/beam expansion along heavy edges (Pregel/GraphFrames), or DP over the
    near-DAG of dominant flows.
- **Why it's genuinely different:** it models the *network of movement*, not the
  set of routes (clustering) nor the string structure (suffix). It also yields
  activity zones for free.
- **Complexity:** graph build O(total transitions); PageRank O(iters · |E|);
  heavy-path beam search bounded by beam width × path length.
- **Bottleneck:** super-nodes (downtown cells) with huge degree → message
  explosion in Pregel. Mitigate: prune edges below a frequency floor (a **Bloom
  filter** of "frequent edges" lets us skip rare transitions cheaply), cap beam.

### Phase 8 — Evaluation, visualization, defense outputs
- **Goal:** the comparison the assignment grades on.
- **Experiments:**
  - **Exact vs approximate top-100:** full `groupBy` count vs Count-Min +
    heavy-hitters → measure **recall@100**, frequency error, runtime, memory.
  - **Exact vs HLL distinct-count**, **sort vs T-Digest quantiles**.
  - **Method A vs B vs C:** overlap of discovered routes (Jaccard), runtime,
    memory, route quality (do they match known Porto corridors? do they match
    the ground-truth destination/time files?).
  - **H3 resolution sweep** (8/9/10): stability of top routes.
- **Viz:** Folium maps of top routes + activity heatmaps (small aggregates only).
- **Outputs:** clean DataFrames, stats tables, maps, runtime/memory/accuracy
  charts, trade-off narrative, presentation summary.

---

## 5. Approximate data structures — where each one earns its place

Not bolted on; each maps to a real bottleneck:

| Structure | Used in | Replaces | Win |
|-----------|---------|----------|-----|
| **Count-Min Sketch** | Phase 5/7 sub-route counting | exact hash count of a huge keyspace | bounded memory, sidesteps reducer skew |
| **Heavy-hitters (Space-Saving)** | Phase 5 top-100 | full sort of counts | O(k) memory streaming top-k |
| **HyperLogLog** | zones, route "spread" | exact distinct-count | distinct *taxis* per cell/route in O(1) memory |
| **T-Digest / KLL** | Phase 3 stats | full sort for p50/p95/p99 | one-pass, mergeable quantiles |
| **MinHash-LSH** | Phase 6 similarity join | O(N²) pairwise route distance | ~O(N) approximate neighbors |
| **Bloom filter** | Phase 7 edge pruning / dedup | storing the full frequent-set | cheap membership, skips rare work |

---

## 6. Cross-cutting scalability & bottlenecks

- **Skew is the enemy.** Spatial data is Zipfian: downtown dominates. Every
  counting/grouping step risks hot keys. Tools: map-side combine, salting,
  two-stage aggregation, and sketches that avoid per-key reducers entirely.
- **Shuffle is the cost.** We minimize it: per-trip `pandas_udf` for features &
  encoding (zero shuffle); only the mining counts shuffle, and we shrink those
  with combiners/sketches.
- **AQE on** (adaptive query execution) handles partition coalescing and skewed
  joins at runtime — enabled in `spark_session.py` for both local and cloud.
- **Caching:** cache only reused, small-ish derived frames (e.g. the feature
  table during the report); never cache raw trajectories blindly.
- **Memory:** the parsed `points`/`cell_seq` arrays are the heavy columns; keep
  them out of wide shuffles — carry `TRIP_ID` keys, join arrays back only when
  needed.

---

## 7. Improvements beyond the assignment (engineering/research value)

1. **Ground-truth validation:** use `solution_challengeII.csv` (travel time) and
   `solution_fixed.csv` (destination) to quantify accuracy, not just eyeball maps.
2. **H3 resolution sensitivity study** — defensible parameter choice.
3. **Map-matching-lite** via H3 gap-filling for cleaner routes.
4. **Exact-vs-approximate trade-off curves** as a first-class deliverable.
5. **Reproducibility:** pinned versions, seeded sampling, config-driven paths,
   a one-command pipeline orchestrator, unit tests on tiny fixtures, the
   `validate_env` smoke test, and Windows shims baked into code.
6. **Run manifest:** record wall-time, shuffle bytes, peak memory per phase
   (sample / full / DataProc) → feeds the required comparisons directly.

---

## 8. Defense talking points (anticipated questions)

- **"Why H3 not Geohash/S2?"** Uniform hexagonal adjacency models movement
  without diagonal distortion; best Python tooling; Uber uses it for ride data.
  We sensitivity-tested resolution and picked res 9 from the 15 s/50 km/h
  geometry.
- **"Why is `pandas_udf` not 'just pandas'?"** It runs distributed per-executor
  via Arrow; the alternative (`explode`) would 50× the row count and shuffle.
- **"How do you get top-100 without sorting billions of counts?"** Heavy-hitters
  + Count-Min; we report recall@100 vs the exact baseline.
- **"How does this scale to DataProc?"** Two env vars; no path/master in business
  logic; skew handled by combiners/salting/sketches; AQE on.
- **"Why three methods and which is best?"** Clustering (route similarity),
  suffix (string structure), graph (movement network) — different inductive
  biases; Phase 8 compares them on runtime, memory, and route quality.
- **"What about the Windows hacks?"** They are `os.name=='nt'`-guarded no-ops on
  the cluster; the cloud run is unaffected.

---

## 9. Status

| Phase | State |
|-------|-------|
| 0 Setup & validation | ✅ done & reproducible (SETUP.md) |
| 1 Clean & parse | ✅ implemented & validated |
| 2 Features | ✅ skeleton (pandas_udf) — pending full run |
| 3–8 | 🔜 designed here, not yet implemented |

Next implementation step (when approved): Phase 4 spatial encoding, because every
mining method depends on the cell-sequence representation.
