# Critical Design Review — Porto Taxi Route Mining

> Purpose: adversarially challenge every major decision in
> [ARCHITECTURE.md](ARCHITECTURE.md) *before* writing algorithm code. Each
> component is reviewed with a fixed template. Where the review changes a
> decision, it is logged in §6. Nothing here is rubber-stamped.

Template per component: **Choice · Alternatives (≥2) · Pros · Cons · Time ·
Memory · Spark scalability · Shuffle · Failure cases · Optimizations.**

---

## 1. H3 selection (cell encoding)

- **Choice:** encode each GPS point to an H3 hexagonal cell; a trip → sequence of
  cell IDs (int64).
- **Alternatives:**
  1. **Geohash** (rectangular, prefix-sortable).
  2. **S2** (spherical quadrilaterals, Hilbert order).
  3. **Road-network map-matching** (snap GPS to OSM road segments; "route" = list
     of road-segment IDs).
  4. Naive lat/lon rounding to a grid.
- **Pros:** uniform hexagon adjacency (6 equidistant neighbors) models movement
  without orthogonal/diagonal distortion; excellent `h3` Python lib; int64 IDs are
  cheap, hashable, and make identical routes identical strings.
- **Cons / honest critique:** **H3 ignores the road network.** Two trips on
  parallel streets ~150 m apart may share or split cells arbitrarily; a cell
  sequence is only a *proxy* for the true road-segment route. The semantically
  correct encoding is map-matching, which we reject only on cost (needs an OSM
  matcher like OSRM/Valhalla + a road graph; infeasible at 1.7M trips locally and
  hard to distribute). H3 is also **not a strict hierarchy** (pentagons; parent
  contains child only approximately), so multi-resolution roll-ups are lossy.
- **Time:** O(points) encoding, O(1) per point. **Memory:** cell seq ≤ points
  (shrinks after dedup).
- **Spark scalability:** per-trip `pandas_udf`, **zero shuffle**, linear.
- **Shuffle:** none in encoding.
- **Failure cases:** boundary GPS jitter flips cells (→ dedup + `h3_line`
  gap-fill); routes hugging a cell edge oscillate; pentagons (12 globally, none
  near Porto).
- **Optimizations:** store IDs as `long`; optional H3 "compaction" for storage;
  precompute neighbor sets once.

---

## 2. H3 resolution

- **Choice:** res 9 (~174 m edge, ~0.1 km²).
- **Alternatives:** res 8 (461 m), res 10 (65 m), res 7 (1.2 km), or
  **speed-adaptive** resolution.
- **Pros:** at ~50 km/h a taxi moves ~210 m / 15 s ≈ one res-9 cell ⇒ consecutive
  samples land in adjacent cells ⇒ faithful, denoised route string.
- **Cons / critique:** the 50 km/h basis is an *average*. **Highways** (100 km/h
  ≈ 420 m/15 s ≈ 2.4 cells) under-resolve → gap-fill draws a straight `h3_line`
  that may not follow a curved road. **Traffic jams** over-resolve → many samples
  per cell (harmless, just dedup). So res 9 is a fixed compromise that is too
  coarse for highways and finer-than-needed in jams. Finer res ⇒ longer strings ⇒
  bigger k-gram shuffle; coarser ⇒ shorter, less discriminative, more false route
  merges. There is a real **precision/recall vs cost** trade-off.
- **Time/Memory:** linear in string length, which grows ~4× per resolution step.
- **Spark:** resolution choice directly sizes the Phase 5 shuffle.
- **Failure cases:** highway gap-fill artifacts; over-merge at coarse res.
- **Optimization / research action:** **resolution sweep {8,9,10}** measuring
  top-route stability and runtime → pick with evidence, not assertion. Keep res
  fixed (not adaptive) so the substring alphabet stays consistent.

---

## 3. Feature engineering

- **Choice:** one Arrow-vectorized `pandas_udf` computes distance/speed/bbox in a
  single pass per trip.
- **Alternatives:**
  1. **`explode` points → window `lag` → re-aggregate** (pure SQL).
  2. **Native Spark higher-order array functions** (`transform`, `zip_with`,
     `aggregate`) — Haversine entirely in Catalyst/JVM, **no Python round-trip**.
  3. A Scala UDF.
- **Pros (current):** simple to read and defend; one row per trip; runs
  distributed via Arrow.
- **Cons / critique:** a `pandas_udf` still pays **Python+Arrow serialization**
  per batch. Option 2 (native HOF) stays in the JVM, is Catalyst-optimized and
  codegen'd, and is likely **faster** — I under-weighted it in the architecture.
  Its only downside is gnarly SQL that is harder to explain in a defense.
- **Time:** O(total points) ≈ 85M, all options. **Memory:** Arrow batch size
  (`arrow.maxRecordsPerBatch`).
- **Spark scalability:** options 1 vs 2/3 differ sharply — **option 1 shuffles**
  (explode 50×s the rows), options 2/3 do not.
- **Shuffle:** current = none (good); explode-variant = large.
- **Failure cases:** None/empty points, single-point trips, float precision.
- **Decision:** keep `pandas_udf` as the readable default; **benchmark native HOF
  in Phase 8** and switch if features become a bottleneck. (Logged §6.)

---

## 4. Clustering method (Method A)

- **Choice:** MinHash-LSH similarity self-join on the **set of cells** →
  similarity graph → connected components → consensus route.
- **Alternatives:**
  1. **TraClus** (partition trajectories into segments, DBSCAN on segment
     distance) — the classic trajectory-clustering paper.
  2. **KMeans/BisectingKMeans** on cell-membership vectors.
  3. **DBSCAN/OPTICS on full-trajectory distance** (DTW / Fréchet / Hausdorff).
- **Pros:** LSH turns an O(N²) similarity problem into ~O(N); all components are
  in MLlib/GraphFrames; uses an approximate structure meaningfully.
- **Cons / critique (two real bugs in the naive version):**
  1. **A set of cells discards order AND direction.** Two trips covering the same
     cells in opposite directions are identical under Jaccard; a route and its
     sub-route have skewed Jaccard. **Fix: shingle the sequence into directed
     consecutive cell *bigrams* `(cᵢ→cᵢ₊₁)` and MinHash those** — restores local
     order and direction. (Directly answers "would MinHash signatures improve
     clustering?" — yes, *if* applied to ordered shingles, not the raw set.)
  2. **Connected components single-linkage chaining:** A~B, B~C, A≁C still merge
     into one giant blob. **Fix: Label Propagation / Louvain community detection,
     or a higher Jaccard threshold**, instead of raw CC.
- **Time:** MinHash O(N·p) (p hashes); LSH join ~O(N·b), worst O(N²) under skew;
  CC/LPA O(iter·|E|). **Memory:** signatures p·N (small p).
- **Spark scalability:** good with capped buckets; degrades under dense-area skew.
- **Shuffle:** LSH band-bucket shuffle + per-iteration graph shuffle — the
  heaviest of the three methods.
- **Failure cases:** bucket skew downtown; over-merge; param sensitivity
  (`numHashTables`, threshold, shingle length k).
- **Optimizations:** cap bucket sizes; tune k-shingle length; LPA over CC.

---

## 5. Suffix-based method (Method B)

- **Choice:** distributed **n-gram (k-gram) counting** as the workhorse +
  **generalized suffix array** (per partition) to extract *maximal* frequent
  routes without per-length re-enumeration.
- **Alternatives:**
  1. **PrefixSpan** (MLlib) — frequent *sequential* (gapped) patterns.
  2. **FP-Growth** — frequent itemsets (no order).
  3. **Suffix automaton / FM-index** for substring stats.
- **Pros:** k-gram counting is embarrassingly parallel (map) + one reduce; maps
  perfectly to the "contiguous substring" definition; top-k via heavy-hitters.
- **Cons / critique:** distributed *global* suffix-array construction is genuinely
  hard — realistically per-partition only. Counting 6 length thresholds is 6
  passes unless the suffix structure shares work.
- **Researcher question — "Is PrefixSpan stronger than suffix arrays?" → No,
  here.** PrefixSpan finds *gapped* subsequences; a gap means a route that
  **skips cells = teleports**, which is semantically wrong for a physical route.
  It also solves a strictly harder, more expensive problem than we need.
  Contiguous-substring counting is both cheaper and *correct* for routes.
  FP-Growth discards order entirely → also wrong.
- **Time:** k-gram O(Σ(nᵢ)) emits; SA build O(n) per partition (SA-IS/DC3).
  **Memory:** long trips (n up to 1376) emit many windows.
- **Spark scalability:** linear map; the reduce is the bottleneck.
- **Shuffle:** the count **is** the shuffle; downtown k-grams are **heavy-hitter
  hot keys** → reducer skew (the central risk of the whole project).
- **Failure cases:** key skew; memory on very long trajectories.
- **Optimizations:** map-side combine; **salting** hot keys (two-stage agg);
  **Count-Min + Space-Saving** to avoid materializing per-key reducers; prune
  trips with a **Bloom filter** of frequent cells before windowing.

---

## 6-pre. Transition Graph method (Method C, approved)

- **Choice:** directed weighted graph (nodes = cells, edges = consecutive
  transitions weighted by frequency / distinct taxis) → PageRank for **activity
  zones** + **heavy-path** expansion for **popular long routes**.
- **Alternatives:**
  1. **Markov random-walk route sampling** (sample paths by transition
     probability, then count) — generates "typical" routes.
  2. **Flow decomposition** (decompose the aggregate transition flow into a small
     set of paths).
  3. **Frequent-edge + DP on the near-DAG** of dominant flows.
- **Pros:** models the *movement network*, genuinely distinct from A and B;
  yields activity zones for free; edges/PageRank are GraphFrames-native.
- **Cons / critique (important):**
  1. **Longest/heaviest path is NP-hard** → we use greedy/beam heuristics, not
     optimal.
  2. **Frankenstein-route risk:** a greedy heavy path can stitch popular edges
     into a route **no taxi ever took**. **Fix: require a minimum edge support,
     and validate every emitted path against observed trips (support ≥ τ)** — only
     report routes that actually occur. (Note the tension: heavy validation pulls
     C toward B; we keep C distinct by *discovering* candidates via the graph,
     then *validating* via support.)
- **Time:** build O(|E|); PageRank O(iter·|E|); beam O(beam·len). **Memory:**
  edge table ≈ distinct transitions (≪ raw points).
- **Spark scalability:** good except super-nodes (downtown) → message explosion
  in Pregel.
- **Shuffle:** graph aggregation + per-superstep message shuffle — communication
  heavy.
- **Failure cases:** super-node skew; Frankenstein routes; heuristic
  sub-optimality.
- **Optimizations:** **Bloom filter** of frequent edges to prune rare
  transitions; cap beam width; bound PageRank iterations; checkpoint.
- **Researcher question — "graph algorithm better than heavy-path?"** Markov
  random-walk sampling is a strong contender (naturally produces *observed-like*
  routes and is trivially parallel), but it is **stochastic** (harder to defend
  reproducibly) and needs many samples for tail routes. **Recommendation:**
  heavy-path with support-validation as primary; mention random-walk as a
  discussed alternative.

---

## 6b. Approximate data structures

- **Choice & mapping:** Count-Min (counts), Space-Saving (top-k), HLL (distinct),
  T-Digest/KLL (quantiles), MinHash-LSH (similarity), Bloom (membership/pruning).
- **Alternatives & critique:**
  - **Count-Min overestimates** (collisions inflate) → can promote false
    heavy-hitters into top-100. **Space-Saving is purpose-built for top-k** and
    usually more accurate there → prefer it for the final top-100, use Count-Min
    to show the error trade-off.
  - **T-Digest vs KLL:** KLL has **formal error guarantees**; T-Digest has better
    tail accuracy but weaker theory. For an academic defense, **KLL** is the safer
    primary; report both.
  - **Prefer Spark-native sketches where they exist:** `approx_count_distinct`
    (HLL++), `approxQuantile` (Greenwald-Khanna), `DataFrame.stat.countMinSketch`,
    `DataFrame.stat.bloomFilter` — all mergeable across partitions and need **no
    extra dependency**. Use `datasketches` only where Spark lacks it (Space-Saving,
    KLL, MinHash signatures we control).
  - **Bloom FP direction is safe:** a false positive means we *keep* a rare
    edge/trip (no correctness loss, slight extra work); a Bloom never drops a real
    frequent item.
- **Time/Memory:** all sublinear memory, single-pass, mergeable. **Spark:**
  excellent (sketches are designed for distributed merge). **Shuffle:** sketches
  reduce shuffle by replacing per-key reducers with a small mergeable summary.
- **Failure cases:** Count-Min overestimate; HLL variance for small counts; sketch
  parameter (width/depth/precision) mis-sizing.

---

## 6c. Final visualization pipeline

- **Choice:** Folium (Leaflet) maps of top routes + H3 activity heatmaps; small
  aggregates only via `toPandas`.
- **Alternatives:** **kepler.gl** (handles larger data, native H3 layer),
  **deck.gl** (most scalable, GPU), static **matplotlib/contextily**.
- **Pros:** Folium is simple, interactive, zero infra.
- **Cons / critique:** Leaflet **dies above ~10k polylines**; you **cannot** plot
  1.7M trajectories. Must aggregate first (top-N routes + H3 bin heatmap). A
  careless `toPandas` on trajectories OOMs the driver.
- **Failure cases:** driver OOM on `toPandas`; browser hang on too many layers.
- **Optimizations:** render **H3 aggregate heatmaps** (counts per cell) not raw
  points; cap to top-N routes per length; consider **kepler.gl** for the defense
  visuals (native H3, larger capacity).

---

## 6. Decisions changed by this review (the payoff)

| # | Original plan | Revised after review | Why |
|---|---------------|----------------------|-----|
| 1 | MinHash on **set of cells** | MinHash on **directed cell-bigram shingles** | restores order + direction (Jaccard on sets ignores both) |
| 2 | Cluster via **connected components** | **Label Propagation / Louvain** (or higher threshold) | avoid single-linkage chaining into one blob |
| 3 | Top-100 via **Count-Min** | **Space-Saving** primary, Count-Min for error curve | Count-Min overestimates → false heavy-hitters |
| 4 | Quantiles via **T-Digest** | **KLL** primary (formal guarantees), report both | defensible error bounds |
| 5 | Sketches via `datasketches` | **Spark-native** sketches where available | mergeable, no extra dep, less shuffle |
| 6 | Heavy-path routes | heavy-path **+ observed-support validation** | eliminate Frankenstein routes |
| 7 | `pandas_udf` only | benchmark **native HOF** alternative in Phase 8 | likely faster (no Python round-trip) |
| 8 | Folium for everything | **H3 heatmaps + top-N only**; kepler.gl option | Leaflet can't render millions |

These are cheap to adopt now and expensive to retrofit later — the reason for the
review.

---

## 7. Candidate-algorithm comparison matrix

Scores: ●●● strong · ●● medium · ● weak. "Explain" = ease of defending verbally.

| Algorithm (role) | Accuracy | Scalability | Runtime | Memory | Spark-friendly | Approx-compatible | Explain | Verdict |
|------------------|:--------:|:-----------:|:-------:|:------:|:--------------:|:-----------------:|:-------:|---------|
| **n-gram counting** (B core) | ●●● | ●●● | ●● | ●● | ●●● | ●●● (CMS/SS) | ●●● | **PRIMARY for top-100** |
| Suffix array (B maximal) | ●●● | ●● | ●● | ●● | ●● | ●● | ●● | secondary (maximal routes) |
| PrefixSpan (B alt) | ●● (gapped=wrong) | ●● | ● | ● | ●● | ● | ●● | **rejected** (semantics+cost) |
| FP-Growth (B alt) | ● (no order) | ●● | ●● | ●● | ●●● | ● | ●● | rejected (order lost) |
| MinHash-LSH + LPA (A) | ●● | ●● | ●● | ●● | ●●● | ●●● (LSH) | ●● | **chosen for A** (with shingles) |
| TraClus / DBSCAN (A alt) | ●●● | ● | ● | ● | ● | ● | ● | cite, don't implement |
| KMeans on cell vectors (A alt) | ● | ●●● | ●●● | ●● | ●●● | ● | ●●● | rejected (wrong metric) |
| Heavy-path + support (C) | ●● | ●● | ●● | ●●● | ●● | ●● (Bloom) | ●● | **chosen for C** |
| Markov random-walk (C alt) | ●● | ●●● | ●● | ●●● | ●● | ●● | ● | discussed alternative |
| PageRank (C zones) | ●●● (zones) | ●● | ●● | ●● | ●●● | ● | ●●● | **chosen for zones** |

**Evidence-based final selection:** B = n-gram (primary) + suffix array (maximal);
A = MinHash-LSH on directed bigram shingles + LPA; C = heavy-path with
observed-support validation + PageRank for zones. Approximate layer: Space-Saving
+ Count-Min, HLL, KLL, MinHash, Bloom — Spark-native first.

---

## 8. Implementation roadmap

Order is dependency-driven: encoding underpins all three miners; approximate layer
and evaluation come after a correct exact baseline exists.

| M | Goal | Expected output | Validation tests | Performance tests | Commit name |
|---|------|-----------------|------------------|-------------------|-------------|
| **M1** | Run Phase 2 features at scale | `trips_features.parquet` | speed/dist sane ranges; anomaly % plausible; read-back schema | sample vs full wall-time; shuffle = 0 | `feat: run and validate feature engineering` |
| **M2** | Phase 3 stats (exact + approx) | stats tables, plots | exact vs `approxQuantile`/HLL within tolerance | sort-quantile vs KLL runtime | `feat: add EDA statistics (exact + approximate)` |
| **M3** | Phase 4 H3 encoding + denoise | `trips_encoded.parquet` (cell_seq, cum_km) | dedup correct; gap-fill adjacency; len monotonic | encode runtime; string-length dist | `feat: add H3 spatial encoding with denoising` |
| **M4** | H3 resolution sweep | sweep report 8/9/10 | top-route stability metric | runtime per resolution | `chore: H3 resolution sensitivity study` |
| **M5** | Phase 5 B: exact n-gram top-100 | `routes_suffix` per L | counts match brute force on tiny fixture | shuffle bytes; skew profile | `feat: frequent sub-route mining (n-gram, exact)` |
| **M6** | Phase 5 B: suffix-array maximal | maximal routes | maximal ⊇ truncated n-gram top | build time per partition | `feat: suffix-array maximal route mining` |
| **M7** | Approx top-100 (CMS + Space-Saving) | approx routes + error report | recall@100 vs exact; freq error | runtime/memory vs exact | `feat: approximate top-k routes (sketches)` |
| **M8** | Phase 6 A: clustering | `routes_cluster` | shingle order check; no giant blob | LSH bucket skew; iters | `feat: clustering route discovery (MinHash-LSH+LPA)` |
| **M9** | Phase 7 C: graph + heavy-path | `routes_graph`, zones | emitted routes pass support validation | PageRank iters; super-node skew | `feat: transition-graph heavy-path mining` |
| **M10** | Phase 8 evaluation | comparison tables/charts | A∩B∩C overlap; ground-truth check | full runtime/memory matrix | `feat: evaluation and method comparison` |
| **M11** | Phase 8 visualization | maps + heatmaps | no driver OOM; top-N only | render sizes | `feat: route and activity-zone visualization` |
| **M12** | DataProc migration run | cloud timings | parity with local results | 5-machine scaling curve | `chore: DataProc run and scaling results` |

Each milestone: develop on the 5k sample → validate → run full local → commit.
DataProc only at M12.

---

## 9. Recommendation

Proceed to **M1** (run/validate Phase 2 features) under the revised decisions in
§6. The exact baselines (M5) must exist before the approximate variants (M7) so we
can *measure* the approximation error the assignment asks us to compare.
