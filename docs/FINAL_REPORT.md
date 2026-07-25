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
| ≥X% support, maximising length | `route_mining_maximal.py` — X calibrated **per length config** |
| The "holes" (corridors fragmenting at forks) | `route_mining_maximal.py` holes section |
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
O(n²) in cells per trip. Measured:

| | 4,745 trips | 188,761 trips |
|---|---|---|
| exhaustive windows emitted | 916,815 | **33,597,872** |
| M5 exact (window + groupBy) | 7.5 s | **OOM — Java heap space** |
| M7 sketches (streaming, no shuffle) | 13.0 s | **71.9 s, 66 MB fixed** |
| **M12 suffix array** | **9.0 s** (72,271 suffixes) | **26.0 s** (2,808,406 suffixes) |

So at 12% of the full dataset, on this machine, the exhaustive baseline already
dies while the suffix array finishes in 26 seconds — and the suffix array is
**exact**, not an approximation. Its bucketing argument is what buys that: every
occurrence of a sub-route of ≥3 cells starts at a suffix sharing its first 3
cells, so all occurrences land in one partition and per-partition counting needs
no cross-partition merge.

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

## 10. Honest limitations

1. **The full 1.71M DataProc run has not been executed.** Everything is validated
   at 5k and exercised at 200k on one machine. The cloud path is scripted and
   the storage layer is `gs://`-aware, but it is untested against real GCS.
2. **≥20/40 km configs are empty at sample scale** (§9) and need the full data.
3. **Method A clusters a capped subset** (`CLUSTERING_MAX_TRIPS = 50,000`)
   because the LSH self-join grows quadratically. Its `support` is measured on
   all trips, but `cluster_size` is a sample statistic; the report says so.
4. **Methods A and C collect a pruned graph to the driver.** Bounded by
   `EDGE_COLLECT_CAP`, but a distributed community detection would be needed
   beyond that.
5. **The ground-truth files** (`solution_*.csv`) are resolved by `config.py` but
   unused — no destination/ETA accuracy study.
6. **The suffix array truncates suffixes at 200 cells** (~60 km), matching the
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
