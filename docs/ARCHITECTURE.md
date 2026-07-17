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
  no shuffle. **Actual output columns:** `h3_seq_raw`/`h3_seq_compact`
  (`array<string>` hex cells), `n_cells_raw`, `n_cells_compact`,
  `compression_ratio`, `encoded_len_km`. *Not yet stored:* a per-trip cumulative
  `cum_km: array<double>` and `int64` cell IDs — planned for **M8.1
  (scale-hardening)** so mining can drop the in-UDF H3 distance recompute and
  shrink shuffle keys. (This corrects an earlier aspirational note that listed
  `cell_seq: array<long>`/`cum_km` as if already produced.)

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
- **Why contiguous n-grams and NOT PrefixSpan (M5 decision):** a physical route
  is a *continuous* path. A gapped subsequence (what PrefixSpan/FP-growth mine)
  would allow a "route" that skips from one cell to a non-adjacent cell — a
  teleport that never happened. Gapped mining is therefore both *semantically
  wrong* here and a strictly harder, more expensive problem. Contiguous substring
  counting is the correct and cheaper model. `support` counts **distinct trips**
  containing the sub-route (deduped within a trip), not repeated occurrences.

- **M5 — exact baseline implemented & validated** (`route_mining_exact.py`,
  `verify_route_mining.py`). On the 5k sample: 1,088,976 window emissions →
  810,933 distinct sub-routes; wall time ~53 s (`local[*]`). Verified by an
  independent brute-force containment re-count (e.g. L≥1 km top route support
  285 = 285; L≥3 km top 85 = 85). Top-support by threshold:

  | min_len | candidates ≥L | top support |
  |--------|------|------|
  | 1 km | 810,933 | 285 (5.9% of trips) |
  | 3 km | 687,707 | 85 |
  | 5 km | 548,904 | 29 |
  | 10 km | 302,490 | 3 |
  | 20 km | 135,858 | 1 |
  | 40 km | 19,698 | 1 |

  **Sample-size caveat (important for defense):** support collapses to 1–3 for
  ≥10 km because 5k *random* trips are too sparse for long specific routes to
  repeat. This is a sampling property, not a bug — the ≥10 km thresholds only
  become meaningful on the full 1.7M dataset. Short thresholds already show clear
  popular corridors. The huge distinct-key count (810k, mostly support 1) is the
  cardinality/skew that **M7's Count-Min + heavy-hitters** will compress.
- **Safety bound:** windows are enumerated up to ~45 km (just above the max 40 km
  threshold) so teleport/pathological trajectories cannot blow up the O(n²)
  per-trip enumeration; irrelevant to real trips (p95 distance ≈ 13 km).

- **M6 — suffix-style MAXIMAL (closed) routes implemented & validated**
  (`route_mining_suffix.py`, `verify_suffix_mining.py`).
  - **Problem it solves:** M5 top-lists are full of overlapping fragments of the
    same corridor (a popular route makes all its sub-windows popular too).
  - **Definition:** a sub-route `s` is *maximal/closed* iff no proper contiguous
    super-route has support within `SUPPORT_TOL` of `s` (default 0.0 = classical
    closed: drop `s` only if a longer route has EQUAL support).
  - **Suffix-tree mechanism (why it's "suffix-style"):** support is monotonic
    under extension, so the highest support any super-route can reach = the best
    SINGLE-CELL extension's support. `s` is closed ⇔ every one-cell left/right
    extension has strictly lower support — exactly the suffix-tree branching-node
    (left/right-maximal repeat) property. Computed over the sub-route set as:
    `right_parent = t[:-1]`, `left_parent = t[1:]`, `groupBy(parent)→max(support)`,
    keep `s` iff `max_ext_support < support(s)·(1−TOL)`. Fully Spark-native
    (split + slice + groupBy + join), reuses the M5 support table.
  - **How it differs from M5:** M5 = count *all* contiguous n-grams; M6 = keep
    only the *closed* ones. Same support values, far fewer routes.
  - **Complexity:** M5 emit/shuffle + two extra `groupBy(parent)` + two joins over
    the ~810k-row support table (≪ the 1.09M window emit). Wall time ~65 s
    (sample), vs ~53 s for M5.
  - **Result (sample):** 810,933 → **24,323 maximal (3.0% kept, 97–99% reduction
    per threshold)**; top support preserved (1 km 285, 3 km 85, 5 km 29).
    Verified: brute-force support matches; 0 reported routes dominated by a
    longer reported route; an M5 4-cell fragment (support 122) shown collapsing
    into a 5-cell maximal route (support 122).
  - **Limitations / defense notes:** (1) closedness here means "contiguous
    super-string", not fuzzy geometric overlap — two *parallel* corridors are not
    merged (that is the clustering method's job, Phase 6). (2) With `TOL=0` a
    route whose extension drops support by just 1 is still kept; raise `TOL` to
    also collapse near-equal containments. (3) The single-cell-extension
    characterization is exact *because* support is monotonic — worth stating in
    defense. (4) Not a from-scratch suffix-array build: we exploit the closed
    property directly, which is more Spark-friendly than distributed generalized
    suffix-array construction (noted as the rejected alternative).

- **M7 — APPROXIMATE top-k implemented & validated** (`route_mining_approx.py`,
  `verify_approx_mining.py`). All parameters live in `config.py` (sketch lg /
  Count-Min depth·width / top-k / thresholds / seed).
  - **Distributed algorithm:** build a small sketch per partition
    (`mapPartitions`) and MERGE the sketches on the driver — no big key shuffle,
    only one sketch bundle per partition moves. The window stream is
    deduped-within-trip, so a sketch occurrence == distinct-trip support (repeats
    inside one trip count once).
  - **How Space-Saving works (PRIMARY, the top-k finder):** it keeps a bounded map
    of at most *m* (item → counter). On a new item: if present, increment; if
    room, insert with count 1; else **evict the current minimum**, give the new
    item that minimum's count + 1 and remember that minimum as the item's *error*.
    Heavy hitters never fall below light ones, so with *m ≫ k* the true top-k
    survive. Each item carries `[lower, upper]` support bounds
    (`upper = counter`, `lower = counter − error`). We use the mergeable
    datasketches `frequent_strings_sketch` (Misra-Gries/Space-Saving family), one
    per length threshold (~0.75·2¹⁶ ≈ 49k counters each).
  - **How Count-Min works (AUXILIARY, the frequency oracle):** a `d × w` table of
    counters with *d* independent hash functions. `update(x)` adds 1 to
    `table[i][hᵢ(x)]` for every row *i*; `estimate(x) = min_i table[i][hᵢ(x)]`.
    Collisions only ever ADD, so the estimate is an **upper bound** — Count-Min
    *never underestimates* (error ≤ e·N/w with prob 1−2⁻ᵈ). It stores **no keys**.
  - **Why Space-Saving is primary / why Count-Min alone can't discover candidates:**
    Count-Min answers "how frequent is *this* route?" but cannot list *which*
    routes are frequent — it has no key inventory, so recovering the top-k would
    need a separate candidate set (i.e. the exact key universe we are trying to
    avoid) plus a heap. Space-Saving *is* a key-retaining top-k structure, so it
    yields the candidates directly; Count-Min then cross-checks their frequencies.
  - **Guarantees:** SS `[lb,ub]` always brackets the true support; CMS estimate
    ≥ true support; both are single-pass, mergeable, fixed-memory, and
    **deterministic** under a fixed seed/config.
  - **Behavior under heavy key skew (the Porto reality):** skew is where sketches
    shine. The exact groupBy puts a few downtown "hot" sub-routes on a handful of
    overloaded reducers (straggler risk). Sketches have **no per-key reducer**: a
    hot key is just a large counter, updated locally and merged — no data lands on
    one machine because a route is popular. Skew also *helps accuracy*: Space-
    Saving's error is bounded by the *tail* mass, so genuinely heavy hitters (high
    skew) are estimated with tiny relative error, exactly as seen at 1–3 km.
  - **Results (sample, measured):** memory 388 MB exact (810k keys) → **101 MB
    fixed** sketches (3.8× here; constant regardless of input size while exact
    grows linearly). Shuffle: exact moves **1,088,976** window rows into the
    groupBy; approx merges **1 bundle/partition**. Determinism verified (two builds
    → identical top-k). Accuracy vs exact top-100:

    | min_len | overlap@100 | precision@100 | recall@100 | abs err (MAE) | rel err (MRE) | CMS MAE |
    |--------|------|------|------|------|------|------|
    | 1 km | 100 | 1.00 | 1.00 | 0.1 | 0.001 | 3.8 |
    | 3 km | 92 | 0.92 | 0.92 | 0.9 | 0.029 | 4.2 |
    | 5 km | 63 | 0.63 | 0.63 | 3.9 | 0.730 | 3.7 |
    | 10–40 km | 0 | 0.00 | 0.00 | ~ | ~ | ~4 |

  - **Tradeoffs / limitations vs exact:** where real heavy-hitters exist (1–3 km)
    approx is near-perfect; the 0.00 at ≥10 km is the **sample-sparsity tie
    artifact** (support ≈ 1, so "top-100" is arbitrary — *not* an approximation
    failure; it resolves on the full 1.7M dataset). On the 5k sample approx wall
    time can exceed exact because Python per-item updates lose to a JVM groupBy on
    tiny data; the sketch win is memory + shuffle at scale, not wall-time on 5k.
  - **Expected DataProc scalability (where approx wins):** exact cost = a groupBy
    shuffle whose key set (810k here) grows with the data → shuffle bytes + driver
    memory blow up and hot-key stragglers appear. Sketch memory and merge cost are
    **fixed by capacity**, independent of trip count; per-partition build + tiny
    merge scale linearly with machines. On the full dataset / 5-node cluster the
    approximate path is the memory- and shuffle-bounded one — its reason to exist.
  - **Defense talking points:** (1) occurrence == distinct-trip support *only
    because* we dedupe within trip before streaming; (2) SS finds keys, CMS scores
    keys — different jobs, hence both; (3) CMS one-sided error is *safe* for
    "is this route popular?" (never hides a real hot route); (4) precision is high
    exactly where it matters (heavy hitters) and the ≥10 km zeros are data
    sparsity, provable by the identical 0.00 in the exact top-100 ties; (5) sketches
    turn skew from a liability (reducer stragglers) into an asset (tight bounds on
    hot keys).

- **M8 — MIN-SUPPORT X% + MAXIMAL, the PDF's literal definition, implemented &
  validated** (`route_mining_maximal.py`, `verify_maximal.py`).
  - **Why it exists:** the PDF defines a popular long sub-route as *"≥ X% of trips
    traversed it, while maximising its length"*, and its Haifa→Ashdod example shows
    a corridor breaking into **contiguous pieces with holes** where traffic
    diverges. M5 (top-k by raw support) is a proxy; **M8 is the definition.**
  - **Criterion:** keep a contiguous sub-route iff `support ≥ min_sup`
    (`= ceil(X%·n_trips)`) **and** its best single-cell extension `< min_sup`.
    Since support is monotonic, "maximal among frequent" = the longest stretch
    still clearing X% → exactly "maximise length subject to ≥ X%". Reuses M6's
    parent-key extension machinery + the shared support table; `SUPPORT_X_PCT`
    and the sweep live in `config.py` (PDF asks us to experiment with X).
  - **The holes fall out for free:** a maximal-frequent route terminates precisely
    where its continuations each drop below X% — i.e. a **fork**. So one popular
    corridor = a *collection* of contiguous sub-routes with holes at the forks.
  - **Difference from M5/M6:** M5 = top-k by raw support (proxy); M6 = closed (no
    EQUAL-support extension, no X% floor); **M8 = frequent(≥X%) ∧ maximal** (PDF).
  - **Results (5k sample, X = 0.5% → min_sup 25):** 810,933 sub-routes → **314
    maximal-frequent**; per length ≥1 km 314 routes / top support 46 / longest
    3.95 km; ≥3 km 31 routes / longest 5.41 km; ≥5 km 2 routes; ≥10 km none.
    Sweep: X = 0.2% (min_sup 10) → 1,039 routes, longest 6.52 km (lower X ⇒ longer
    frequent routes). **Holes demo:** top route (support 46) forks into branches of
    24 and 20 trips — each below min_sup 25 — so it terminates and a hole forms.
  - **Verified (independently):** brute-force containment == reported support;
    support ≥ min_sup; best left/right extension (counted by `regexp_extract` over
    trips, independent of the aggregate) < min_sup; the top route's forks each
    < min_sup (genuine hole). All PASSED.
  - **Sample-sparsity caveat:** on 5k trips X must be small (0.2–0.5%) and long
    (≥10 km) maximal-frequent routes are absent; on the full 1.71M dataset X
    becomes meaningfully larger and long corridors appear. This is the
    canonical Method-B output that A (Phase 6) and C (Phase 7) will be compared to.

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

- **M9 — Method A implemented & validated** (`route_mining_clustering.py`,
  `verify_clustering.py`). Pipeline: directed **bigram shingles** → `HashingTF`
  (binary) → MLlib **MinHashLSH** `approxSimilarityJoin` (Jaccard dist ≤ 0.3 ⇒
  similarity ≥ 0.7) → cluster → seed representative. No GraphFrames (its Windows
  setup cost isn't worth it here; the graph after LSH pruning is small).
  - **Bug found & fixed by the verifier (important):** the first version used
    **connected-components** (iterative label propagation). The coherence check
    exposed a **chained blob** — a "137-trip" cluster whose representative was
    similar to only **3** members (coherence 0.02), exactly the single-linkage
    chaining DESIGN_REVIEW #2 predicted. Fix: replaced CC with **greedy star
    (canopy) clustering** — each cluster is a high-degree seed + trips *directly*
    similar to it, so membership can't chain. Re-verified coherence ≈ **1.0**.
  - **Also fixed:** the LSH threshold was far too loose (0.55 dist) and OOM'd the
    self-join; tightened to 0.3 with `localCheckpoint`-style lineage discipline.
  - **Results (5k sample):** 4,815 trips → 2,451 similarity edges → **183 coherent
    clusters**; top corridor 28 similar trips (rep 6.14 km); 4 clusters ≥ 10 km
    (rep 10.87 km). Verified: 0 structural violations; coherence ≥ 0.5 (actually
    ~1.0) on the top clusters.
  - **Design-review fixes honored:** bigram shingles (order+direction, not raw cell
    set); no giant blob (star clustering not CC). **KEEP** MinHashLSH (the required
    approximate structure here).
  - **How it differs from Method B:** B mines frequent sub-route *strings*; A groups
    *whole trajectories* by similarity and reads off corridors — so A's "popularity"
    is cluster size, B's is sub-route support. The M16 comparison quantifies their
    overlap.
  - **Full-scale note:** the pruned edge list is collected to the driver (capped at
    `EDGE_COLLECT_CAP`); at 1.71M a distributed community detection (GraphFrames
    LPA/Louvain) would replace the driver-side step. Documented, not yet needed.

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
