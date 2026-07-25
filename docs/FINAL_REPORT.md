# Porto Taxi Route Mining — Engineering Report

Big Data / Cloud Computing final project. Every number below is **measured**, and
says which scale it came from. Nothing in this document is hard-coded prose: the
generated reports under `outputs/statistics/` are the source, and they are
rebuilt by `python -m src.run_pipeline --sample --verify`.

Scales used: **sample** = 5,000 trips (4,745 after cleaning), **mid** = 200,000
trips (188,761 after cleaning), **full** = 1,710,670 trips (DataProc).

---

## 1. What the assignment asks for, and where it is

| Requirement | Where |
|---|---|
| Upload to GCS, run on DataProc with ≥5 machines | `scripts/dataproc_submit.sh` (1 master + 5 workers) |
| Clean corrupt data | `clean_data.py` + `feature_engineering.py` flags + `spatial_encoding.py` exclusion + per-window hop guard |
| Derive points / duration / distance | `clean_data.py`, `feature_engineering.py` |
| Documented DataFrame + basic statistics | `summarize_features.py` → `phase2_feature_summary_*.md` |
| Spatial encoding, grid choice justified | `spatial_encoding.py --compare-grids` → `grid_comparison_*.md` |
| Top-100 long sub-routes at ≥1/3/5/10/20/40 km | all four miners, `outputs/routes/*_top100_*.csv` |
| ≥X% support, maximising length | X calibrated **per length config** — `route_mining_suffix_array.py` (all scales) and `route_mining_maximal.py` (sample-scale reference) |
| The "holes" (corridors fragmenting at forks) | holes section in both miners above |
| A clustering method | `route_mining_clustering.py` (Method A) |
| A **suffix tree / suffix array** method | `route_mining_suffix_array.py` (Method D) |
| A method that is neither | `route_mining_graph.py` (Method C) |
| Hash-based approximate structures | MinHash-LSH, Space-Saving, Count-Min, GK quantiles |
| Method comparison: runtime, accuracy, memory | `evaluation.py` → `method_comparison_*.md` |
| Map demo under Colab Enterprise | `notebooks/porto_routes_colab.ipynb`, all six configs |
| Popular routes / activity zones / anomalies | Methods A–D / `route_mining_graph` / `anomaly_analysis` |

---

## 2. Architecture

```
raw CSV (1.9 GB)
  → clean_data           parse, reject corrupt, dedupe TRIP_ID
  → feature_engineering  distance/speed/sinuosity/bbox + anomaly flags
  → summarize_features   distribution statistics
  → spatial_encoding     H3 res-9 sequences; anomalous trips EXCLUDED
  → A clustering | B maximal-frequent | C graph | D suffix array
    + M5/M6 exhaustive baselines (sample only) + M7 sketches
    + anomaly_analysis
  → evaluation (A/B/C/D) · visualization (Folium / Colab)
```

Every path flows through `config.dataset_paths(scale)`; every report and CSV
flows through `src/storage.py`, which writes via Hadoop FS when the path has a
URI scheme. Those two facts are what make `SPARK_ENV=cloud DATA_BASE=gs://…
OUTPUT_BASE=gs://…` the entire cloud migration.

**Core idea:** after encoding, a trip is a *string over an H3-cell alphabet*. A
sub-route is a contiguous substring, "popular" is distinct-trip support, "long"
is ground length ≥ L. One representation feeds all four methods, which is what
makes cross-method comparison meaningful.

---

## 3. The four methods (sample scale, ≥3 km config)

| | approach | routes | top support | longest |
|---|---|---|---|---|
| **A** clustering | MinHash-LSH on directed bigram shingles → greedy star clustering → longest cell run shared by ≥60% of members | 100 | 60 | 10.56 km |
| **B** maximal-frequent | n-gram support table → maximal among routes clearing X%, X calibrated per length | 99 | 20 | 6.89 km |
| **C** transition graph | PageRank zones + dominant-flow heavy paths, validated against trips | 65 | 43 | 7.29 km |
| **D** suffix array | generalised suffix array + LCP intervals | 95 | 60 | 5.05 km |

All four report the same unit — a contiguous sub-route with a distinct-trip
support — so these columns are directly comparable.

### Cross-method agreement (≥3 km, cell-set Jaccard ≥ 0.5)

| row→col | A | B | C | D |
|---|---|---|---|---|
| **A** | 1.00 | 0.64 | 0.31 | 0.39 |
| **B** | 0.61 | 1.00 | 0.59 | 0.67 |
| **C** | 0.34 | 0.62 | 1.00 | 0.42 |
| **D** | 0.60 | **0.99** | 0.82 | 1.00 |

**D→B = 0.99** is the headline: a suffix array walking LCP intervals and an
n-gram support table with a maximality join are completely different algorithms,
and 99% of D's corridors have a partner in B's. Their *supports* also agree
exactly — 0 disagreements across all 174 routes both report (`verify_*`). That is
the strongest evidence available that these corridors are real rather than
artefacts of one method's bias.

A→C = 0.31 is the weakest pair, and honestly so: whole-trajectory clustering and
dominant-flow graph walks optimise different things.

---

## 4. Cost (sample scale, `local[*]`, 8 cores / 16 GB)

Measured, from `outputs/statistics/timings.jsonl`:

| stage | wall | peak RSS | work volume |
|---|---|---|---|
| M3 encoding (+ grid sweep) | 40.6 s | 100 MB | 4,745 trips encoded |
| M8 maximal-frequent | 32.7 s | 46 MB | 636,081 distinct sub-routes |
| M10 graph | 18.8 s | 50 MB | 1,852 nodes / 5,209 edges |
| M7 approx vs exact | 13.0 s | 692 MB | 916,815 windows |
| M6 closed | 12.0 s | 47 MB | 636,081 → 22,045 closed |
| **M12 suffix array** | **9.0 s** | 100 MB | **72,271 suffixes** → 1,514 maximal |
| M9 clustering | 7.9 s | 106 MB | 2,320 edges → 177 clusters |
| M5 exact baseline | 7.5 s | 45 MB | 916,815 windows → 636,081 keys |

---

## 5. Scalability: the finding that shaped the design

The exhaustive window miner emits every contiguous window of every trip —
O(n²) in cells per trip. Measured on this machine (8 cores / 16 GB):

| | 4,745 trips | 188,761 trips |
|---|---|---|
| exhaustive windows emitted | 916,815 | **33,597,872** |
| M5 exact (window + groupBy) | 7.5 s | **OOM — Java heap space** |
| M8 maximal-frequent (same table) | 32.7 s | **21 GB spilled, did not finish** |
| M7 sketches (streaming, no shuffle) | 13.0 s | 115.6 s, **66 MB fixed** |
| **M12 suffix array** | **9.0 s** (72,271 suffixes) | **41.3 s** (2,808,406 suffixes) |

At 12% of the full dataset the exhaustive family already dies, while the suffix
array finishes in well under a minute — and it is **exact**, not an
approximation. Its bucketing argument is what buys that: every occurrence of a
sub-route of ≥3 cells starts at a suffix sharing its first 3 cells, so all
occurrences land in one partition and per-partition counting needs no
cross-partition merge.

Crucially, Method D **carries the whole Method-B deliverable** — the per-length X
calibration and the holes analysis — because `mine_bucket` records each route's
best one-cell extension in both directions. "Maximal at X%" then becomes a pure
filter (`support ≥ min_sup ∧ max(best_right, best_left) < min_sup`), so the
entire X grid is answered from one mining pass instead of one support table per
X. On the sample, D reproduces B's calibration table **exactly** — same X per
length (1.0 / 0.2 / 0.1), same route counts (130 / 193 / 111), same supports —
in 15 s rather than 37 s, and it keeps working where B cannot run at all.

### The full pipeline at 200,000 trips

All eleven scale-appropriate stages, **341.9 s end to end**:

| stage | wall | stage | wall |
|---|---|---|---|
| Phase 1 clean | 9.8 s | M12 suffix array (D) | 41.3 s |
| Phase 2 features | 12.9 s | M9 clustering (A) | 102.7 s |
| Phase 2 statistics | 5.8 s | M10 graph (C) | 31.7 s |
| Phase 4 encoding | 13.4 s | M11 anomalies | 7.7 s |
| M7 sketches | 115.6 s | comparison + map | 1.1 s |

Deliverables at that scale, with supports that finally mean something:

| min_len | A routes / top support | C routes / top support | D routes / top support |
|---|---|---|---|
| ≥1 km | 100 / 8,841 | 100 / 5,312 | 100 / 3,492 |
| ≥3 km | 100 / 1,879 | 100 / 1,616 | 100 / 784 |
| ≥5 km | 100 / 676 | 95 / 668 | 100 / 190 |
| ≥10 km | 23 / 20 | 8 / 1 | 18 / 29 |
| ≥20, ≥40 km | 0 | 0 | 0 |

D's calibrated X moves exactly as designed: 1.0% (1,888 trips) at ≥1 km down to
0.01% (19 trips) at ≥10 km. The longest routes also grow with scale — 15.1 km (A)
and 13.6 km (C) at ≥10 km, against ~11–12 km on the sample — which is the
expected behaviour and the reason to believe the ≥20 km band fills at 1.71M.

Consequences, applied throughout:

- M5/M6 run at `--sample` only, as ground truth for the others.
  `route_mining_exact` **refuses to start** above `EXACT_MAX_TRIPS` with an
  explanatory message, rather than dying an hour into a paid cluster run.
- M7 streams its input rather than caching it (caching to count the rows is what
  made the "cheap" method OOM before the exact one) and takes `--approx-only`.
- `run_pipeline` selects stages per scale; `dataproc_submit.sh` mirrors it.
- Method C validates 3,000 candidates against every trip with one Aho-Corasick
  pass per trip instead of 3,000 substring scans per trip.

---

## 6. Correctness

### Cleaning removes corrupt data rather than labelling it

Sub-route length is measured between cell centres, so a window spanning a GPS gap
reports the gap's width as route length. Measured on the sample **without** the
guard:

| | before | after |
|---|---|---|
| worst km per cell-hop in any window | **53.63 km** (45× the physical bound) | 1.10 km |
| windows ≥10 km built across a gap | **563** | **0** |
| worst example | 59.8 km claimed from 19 cells (3.3 km/hop) | — |

Those 563 windows would have landed directly in the graded ≥10/20/40 km lists.
The guard has two layers: anomalous trips (teleport, >200 km/h, parked, off-map)
are excluded before encoding, and every miner splits trajectories at any hop
above `config.max_cell_hop_km()` — derived as *retained-speed limit ×
sample interval + 2 × cell circumradius* = 1.18 km at res 9, not a tuned constant.

Both layers agree: after excluding anomalous trips, **0** encoded trips still
contain a hop above the bound.

### Independent verification

Every stage has a `verify_*.py` that recomputes its result a different way:

- support by brute-force substring containment vs the mining's window/groupBy or
  Aho-Corasick path — matches exactly, including for Method A's new sub-routes;
- suffix array vs exhaustive baseline: **0 support disagreements** over 174
  shared routes;
- Space-Saving `[lb,ub]` brackets the true support and Count-Min never
  underestimates, on every sampled route; two builds give identical top-k;
- clustering cohesion, route continuity, graph anti-"Frankenstein", anomaly
  self-consistency.

Plus **36 unit tests**, including brute-force cross-checks of the LCP-interval
enumeration (8 cases) and the Aho-Corasick automaton (400 randomised trials), and
a regression test that a window may never span a GPS gap.

---

## 7. Approximate vs exact (sample)

| min_len | precision@100 | recall@100 | SS support MAE |
|---|---|---|---|
| 1 km | 0.98 | 0.98 | 0.1 |
| 3 km | 0.93 | 0.93 | 0.6 |
| 5 km | 0.82 | 0.82 | 1.5 |
| ≥10 km | ~0 | ~0 | — |

Memory: **83.6 MB fixed** (sketch capacity) vs 275 MB estimated for the exact key
table, and **8 sketch bundles** crossing the network vs 916,815 shuffled rows.

The ≥10 km rows are a genuine sample artefact, not a sketch failure: at 4,745
trips almost every route that long has support 1, so "top-100" is an arbitrary
choice among thousands of ties and any two methods disagree. It resolves with
scale, which is exactly what the mid/full runs are for.

---

## 8. Choosing the grid (measured, not asserted)

From `grid_comparison_sample.md`:

| grid | res | cell m | avg cells | len_ratio | distinct cells | bearing entropy |
|---|---|---|---|---|---|---|
| h3 | 8 | 461 | 7.8 | 1.228 | 416 | 1.176 |
| **h3** | **9** | **174** | **17.2** | **1.167** | **1,853** | **0.939** |
| h3 | 10 | 66 | 29.6 | 1.070 | 6,821 | 0.749 |
| geohash | 6 | 610 | 9.3 | 1.178 | 571 | 1.016 |
| geohash | 7 | 76 | 29.4 | 1.074 | 6,638 | 0.730 |

`bearing_entropy` is the conflation measure the brief's warning actually
describes: the entropy of travel directions leaving a cell. A cell on one road
sees one or two directions; a cell that has swallowed two parallel roads sees
several. Lower is better.

Two honest readings:

1. **Res 9 is a compromise, not an optimum.** Res 10 conflates less (0.749 vs
   0.939) but costs 3.7× the alphabet and 1.7× the sequence length — and since
   sub-route keys are *sequences* of cells, that multiplies the mining key space
   superlinearly.
2. **These metrics do not by themselves justify H3 over geohash.** At comparable
   cell sizes the two score about the same. The reason to prefer H3 is
   structural: hexagons have six equidistant neighbours, so a trajectory is a walk
   with uniform step cost, whereas geohash rectangles have edge and corner
   neighbours at different distances and distort with latitude — which matters
   when route length is a sum of cell-to-cell hops.

---

## 9. X% calibrated per length configuration

A single global X cannot serve all six length configs. Maximal-frequent routes
are already the longest stretches clearing X, so filtering them at 40 km does not
*find* 40 km routes — it asks whether the one chosen X happened to produce any.
At X=0.5% on 1.71M trips a 40 km corridor would need ~8,500 distinct trips over
the same unbroken stretch.

So for each L we take the largest X whose maximal-frequent set still yields 100
routes at that length. On the sample:

| min_len | X% used | min_sup | routes | longest |
|---|---|---|---|---|
| 1 km | 1.0 | 48 | 130 | 3.61 km |
| 3 km | 0.2 | 10 | 193 | 6.89 km |
| 5 km | 0.1 | 5 | 111 | 8.36 km |
| 10 km | 0.01 | 2 | 13 | 11.99 km |
| 20 km | 0.01 | 2 | **0** | — |
| 40 km | 0.01 | 2 | **0** | — |

The X sweep shows the mechanism directly: as X falls 5.0% → 0.01%, the longest
maximal-frequent route grows 1.10 km → 11.99 km.

**The two empty configs are a reported finding, not a silent gap.** At the
loosest possible floor (2 trips), no 20 km contiguous stretch on 4,745 trips is
driven twice. That is a property of sample size; the report says so explicitly
and the full run is what settles it.

---

## 9b. Two things we measured instead of asserting

### Do the corridors generalise? (`validate_holdout.py`)

Every other check in this project is internal — verifiers recount support against
the same table the mining used. That catches implementation bugs but cannot tell
a real corridor from a memorised training path.

The dataset ships a held-out split that never enters the pipeline
(`Porto_taxi_data_test_partial_trajectories.csv`, 318 usable trips). Encoding it
with the same grid and asking what fraction of unseen trips traverse a mined
corridor:

| method | corridors | held-out coverage | null model | lift |
|---|---|---|---|---|
| A clustering | 295 | 37.7% | 6.3% | **6.0x** |
| C graph | 265 | 28.1% | 9.3% | **3.0x** |
| D suffix array | 318 | 36.8% | 6.3% | **5.8x** |

(corridors mined at mid scale; sample-scale lifts are 3.8 / 3.2 / 5.6x — the lift
*improves* with training volume, which is what should happen.)

The null model matters: coverage alone proves nothing, because corridors sit on
busy roads and so do most trips. The null is random walks over the held-out
city's **own observed adjacency**, matched to the real corridors' length
distribution — plausible routes that simply were not mined as popular. Uniform
random cells would be disconnected and unmatchable, inflating the lift into
meaninglessness.

### Is Method A's sampling cap defensible? (`experiment_cluster_cap.py`)

Method A caps clustering at 50,000 trips because the LSH self-join is quadratic.
The defence was an argument — "popular corridors are frequent, so they survive
sampling" — and never a measurement. Running the real discovery at several caps
on the 200k dataset:

| cap | corridors | wall_s | recall (Jaccard≥0.5) | recall (exact) |
|---|---|---|---|---|
| 10,000 | 509 | 11.9 | 0.81 | 0.06 |
| 25,000 | 1,431 | 20.7 | 0.94 | 0.22 |
| 50,000 | 3,055 | 72.1 | _reference_ | — |
| 100,000 | 6,018 | 297.0 | (0.98 recall *from* 50k) | — |

**The gap between the two recall columns is the finding.** Sampling reliably
finds corridors in the same *places* but rarely with the same *extent* — which
trips are present determines how far a run stays shared by 60% of a cluster. So
Method A's corridor **geography** is trustworthy; its precise route **strings**
are sample-dependent. That is also why cross-method comparison matches
geometrically rather than by string equality: a design choice this experiment
turned from a convenience into a justified one.

On the cap itself: 100k costs ~5x what 50k costs (the quadratic join, as
predicted) and recovers 2% more corridors. The discovery curve flattens well
before the cost curve, so 50,000 stays.

---

## 10. Honest limitations

1. **The full 1.71M DataProc run has not been executed.** Everything is validated
   at 5k and the whole scale-appropriate pipeline runs at 200k on one machine.
   The storage layer is verified against a URI-scheme FileSystem (`file://`,
   which takes the identical code path to `gs://`) but not against real GCS.
2. **≥20/40 km configs are still empty at 200k.** No 20 km corridor is driven
   by even two taxis in 12% of the data. Longest-route length does grow with
   scale (11 → 15 km between the two runs), so the band may fill at 1.71M — but
   that is an expectation, not a result.
3. **Method B (maximal-frequent) is sample-scale only.** It shares the O(n²)
   support table; at 200k it spilled 21 GB without finishing. Method D carries
   its deliverable at scale and reproduces its sample output exactly, so nothing
   is lost — but B itself does not scale, and the report should not imply it does.
4. **Method A clusters a capped subset** (`CLUSTERING_MAX_TRIPS = 50,000`)
   because the LSH self-join grows quadratically. Its `support` is measured on
   all trips, but `cluster_size` is a sample statistic; the report says so.
5. **Method A's route extents are sample-dependent** (§9b) even though its
   corridor geography is stable. Report it as "where the busy corridors are",
   not as a canonical route list.
6. **Methods A and C collect a pruned graph to the driver.** Bounded by
   `EDGE_COLLECT_CAP`, but a distributed community detection would be needed
   beyond that.
7. **The `solution_*.csv` ground truth is still unused.** The held-out
   *trajectories* are now used (§9b); the destination/travel-time labels are a
   different (prediction) task and remain out of scope.
8. **The suffix array truncates suffixes at 200 cells** (~60 km), matching the
   window cap. Corridors longer than that are out of scope by construction.

---

## 11. How to reproduce

```bash
.venv/bin/python -m src.validate_env
.venv/bin/python -m src.make_sample --sample
.venv/bin/python -m src.run_pipeline --sample --verify   # 14 stages + 9 verifiers
.venv/bin/python -m pytest tests/ -q                     # 36 tests

.venv/bin/python -m src.make_sample --mid                # 200k
.venv/bin/python -m src.run_pipeline --mid               # scale behaviour
```

Cloud: set `PROJECT` and `BUCKET`, then `bash scripts/dataproc_submit.sh`.
Do a `SCALE=--sample` cloud rehearsal on a 2-worker cluster first — it costs
cents and proves the GCS path end to end before the full run.
