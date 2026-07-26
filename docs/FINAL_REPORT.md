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

Plus **56 unit tests**, including brute-force cross-checks of the LCP-interval
enumeration (8 cases) and the Aho-Corasick automaton (400 randomised trials), and
a regression test that a window may never span a GPS gap.

---

## 7. Approximate vs exact (measured at two scales)

Run at 5k and at 200k trips. The second scale was added specifically to test a
claim this section used to make, and it falsified it.

| min_len | recall@100 (5k) | **recall@100 (200k)** | rel. error (200k) |
|---|---|---|---|
| 1 km | 0.99 | **1.00** | 0.000 |
| 3 km | 0.92 | **0.99** | 0.001 |
| 5 km | 0.80 | **0.99** | 0.024 |
| ≥10 km | 0.17 | **0.01** | 110.06 |
| ≥20 km | 0.00 | **0.00** | 24.0 |

Cost at 200k (33,597,872 window rows into the exact `groupBy`):

| | sketch | exact | ratio |
|---|---|---|---|
| time | 86.5 s | 89.0 s | ~1× |
| memory | **66.3 MB** (fixed by capacity) | **6,986.6 MB** (14,627,639 keys) | **105× smaller** |

**The case for the sketches is memory, and it strengthens with scale.** At 5k the
memory ratio was only ~3×; at 200k it is 105×. Runtime is a wash — the sketch
does not save time, it saves the key table. That is the honest form of the
argument, and it needed two scales to make.

### A claim this section used to make, now disproved

It previously read: *"The ≥10 km rows are a genuine sample artefact, not a sketch
failure… it resolves with scale."* It does not. Recall at ≥10 km went from 0.17
at 5k to **0.01 at 200k** — worse, not better, with 40× the data.

The reason is structural, not statistical. Space-Saving retains **heavy hitters**;
a corridor is long *because* few trips repeat it, so long corridors sit in the
tail by construction. Adding data adds more short frequent corridors that compete
for the same retained slots, so long ones are evicted harder. No amount of extra
data fixes this, because the sketch is answering "what is frequent?" and the
deliverable asks "what is long *and* frequent?"

This is why Method D carries the deliverable and the sketches do not: the exact
suffix array finds a 26.25 km corridor with support 2, which a top-k sketch
cannot see in principle.

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

## 9c. Four things measured this round

### "Popular" now means drivers, not trips

Support counted distinct TRIPS. With only **442 taxis** over a year, a corridor
driven 200 times by one driver going to their own stand is one person's habit,
not a popular route — and trip-support cannot tell the two apart. Method D now
carries `support_taxis` (counted exactly; the suffix-array buckets are small).

The result at sample scale, from the method comparison:

| min_len | routes | median trips/taxi | routes with ≤2 taxis |
|---|---|---|---|
| ≥1 km | 100 | 1.12 | 0 |
| ≥3 km | 100 | 1.00 | 0 |
| ≥5 km | 100 | 1.00 | 0 |
| ≥10 km | 13 | 1.00 | **11** |

Short corridors are genuinely public (≈1 trip per vehicle). The long band is
**dominated by single-vehicle repeats** — 11 of 13, and the top ≥10 km entry is
2 trips from *one* taxi. Corridor length and the confidence it deserves move in
opposite directions, and the deliverable should be read that way.

**HyperLogLog is used where it is actually warranted** — distinct taxis per H3
cell for the activity zones (~85M (cell, taxi) pairs at full scale), not on the
corridors. The report states honestly that at sample scale HLL is *slower* than
exact (0.4 s vs 0.2 s, 0.80% mean error): its argument is O(1) memory per group,
not speed, and claiming otherwise would be an unearned win.

### The empty ≥20/40 km bands were the instrument

`longest_km` tracks the **absolute** support floor, and the old grid was a
**percentage** — so 0.01% meant 2 trips on the sample but 172 at 1.71M. The
bottom of the grid got *harder to clear as the data grew*, which is backwards.
Switching calibration to absolute floors turned ≥20 km from empty into a
**21.36 km corridor** at mid scale, with no change to the data. The brief
sanctions exactly this: tune X "when you are interested in maximising the
sub-route length".

### A scaling law instead of a shrug

Four scales, the real miner, floors held fixed (`experiment_scaling.md`):

| trips | floor=2 | floor=5 | floor=20 | floor=100 | mine_s |
|---|---|---|---|---|---|
| 4,745 | 11.99 | 8.36 | 5.42 | 2.87 | 5.8 |
| 188,761 | 21.36 | 14.97 | 11.06 | 8.73 | 23.3 |
| 377,451 | 21.36 | 15.37 | 11.54 | 10.20 | 47.7 |
| 754,763 | 24.85 | 17.37 | 12.51 | 10.92 | 114.6 |

Power-law fits, **R² 0.98–0.99**, extrapolated to 1,710,670 trips:

| floor | exponent | predicted longest | ≥20 km? | ≥40 km? |
|---|---|---|---|---|
| 2 | 0.141 | **27.7 km** | yes | no |
| 5 | 0.144 | 19.7 km | no | no |

**Prediction: the ≥20 km band populates on the full run; ≥40 km does not.** Porto
appears to have no 40 km stretch that even two taxis repeat. That is a finding
about the city, not a defect — and predicting it beats being surprised by it.
The `mine_s` column also sizes the cloud run: ~115 s at 755k extrapolates to
roughly 4–5 minutes for the suffix array at full scale.

### "Popular" is only mildly time-dependent

Corridors mined per hour-of-day bucket and compared with the all-time list
(`temporal_analysis_full.md`, cell-set Jaccard ≥ 0.5), at the full 1.71M scale:

| bucket | trips | overlap vs all-time |
|---|---|---|
| night | 296,575 | **0.75** |
| morning peak | 271,960 | 0.82 |
| midday | 506,599 | **0.89** |
| evening peak | 314,049 | 0.82 |
| evening | 225,325 | 0.77 |

Mean **0.81**. The corridors are **structural**: the same stretches dominate at
03:00 and at 08:00, so it is the road network rather than time-varying demand
that decides where taxis go. The all-time top-100 is a fair summary and not an
artefact of averaging. Night remains the least well represented bucket, but at
0.75 the gap is a caveat, not a refutation.

**These numbers are a correction.** An earlier version of this table was computed
on the wrong clock. `F.hour(F.from_unixtime(TIMESTAMP))` renders in
`spark.sql.session.timeZone`, which defaults to the JVM's machine timezone and
was never set — so the same 1,614,508 trips bucketed one way on a laptop
(UTC+3) and another on DataProc (UTC), with identical totals and different
assignments. Neither is Porto. `config.DATASET_TIMEZONE` now pins
`Europe/Lisbon` and `spark_session` applies it in both local and cloud mode.

The bug was worth the trouble it caused: because this section's verdict is
*derived* from the computed mean rather than written by hand, the wrong clock
produced a mean of 0.78 and the wrong conclusion — "a fair summary **with a real
caveat**" instead of "structural". The report was arguing against its own
headline deliverable, and only a cross-machine comparison exposed it.

---

## 9d. THE FULL 1.71M RUN — executed locally

All 13 scale-appropriate stages, **1,994 s (33 min)** on one 8-core / 16 GB
laptop, `SPARK_SHUFFLE_PARTS=200`, 10 GB driver:

| stage | wall | stage | wall |
|---|---|---|---|
| Phase 1 clean | 56.6 s | M9 clustering (A) | 120.4 s |
| Phase 2 features | 38.5 s | M10 graph (C) | 119.4 s |
| Phase 2 statistics | 6.9 s | M11 anomalies | 15.8 s |
| Phase 4 encoding | 58.8 s | M16 comparison | 0.6 s |
| **M7 sketches** | **749.9 s** | M17 held-out | 6.5 s |
| **M12 suffix array** | **409.4 s** | **M18 temporal** | **410.4 s** |
| | | M15 map | 1.1 s |

The quadratic family (M5/M6/M8) is excluded at this scale by design; Method D
carries their deliverable.

### The same run, on a DataProc cluster

The brief requires ≥5 machines reading from cloud storage. The identical code ran
unchanged on **1 master + 5 workers** (`n2-standard-4`, image `2.2.84-debian12`,
`europe-west1`), reading `gs://taxi-project-noyabayazi/taxi/raw/train.csv` and
writing every parquet table and every report back to `gs://`. Nothing touched a
local disk. Evidence captured while the cluster was alive is in
`docs/cloud_evidence/`.

**60 min wall, 50.5 min of job time.** Slower than the laptop's 33 min, which is
worth stating rather than hiding: at 1.71M trips this problem still fits in one
machine's memory, so distribution buys fault tolerance and headroom, not speed —
the coordination and shuffle-over-network costs are real and the dataset is not
big enough to amortise them.

| stage | cluster | laptop |
|---|---|---|
| M7 sketches | 1044.5 s | 749.9 s |
| M18 temporal | 629.0 s | 410.4 s |
| M12 suffix array | 409.1 s | 409.4 s |
| M9 clustering (A) | 293.6 s | 120.4 s |
| M10 graph (C) | 213.9 s | 119.4 s |
| Phase 4 encoding | 157.3 s | 58.8 s |

M12 is the interesting row: the suffix array is the one stage that costs the same
on both, because it is a single pass whose work is already partitioned by 3-cell
prefix. The stages that got slower are the ones that shuffle.

**Correctness.** `verify_cloud_run` compares the cluster against the local run in
tiers, demanding equality only where the algorithm is deterministic:

```
trip count matches the baseline -> cloud=1,614,508 baseline=1,614,508
Method D corridor set matches   -> cloud=420 baseline=420 shared=420
Method D supports identical     -> 420 identical
```

420 corridors, bit-identical across a different machine count, a different
partitioning and a different filesystem. Method D has no sampling, no seeds and
no hash-order dependence, so identical output is evidence of identical input —
the strongest correctness claim available here. Activity zones matched 50/50
including PageRank floats; Phase-1 cleaning matched on all five rejection counts.
Methods A and M7 differ slightly and are reported rather than failed: A samples
trips and uses unseeded MinHash-LSH, and sketch merge order is
partition-dependent.

**Cost: $2.71** for the whole exercise — $1.77 for the graded run and $0.94 for
the rehearsals that found the bugs described in §10.

### The scaling law was right

Predicted from four smaller scales (5k / 189k / 377k / 755k) at R²≈0.98:

| | predicted @1.71M | **measured** |
|---|---|---|
| longest corridor, floor=2 | 27.7 km | **26.25 km** (within 5%) |
| ≥20 km band populates? | yes | **yes** — 20 routes |
| ≥40 km band populates? | no | **no** — 0 routes |

A power law fitted on data up to 755k predicted 1.71M behaviour to within 5%.
Porto has no 40 km stretch that even two taxis repeat — a fact about the city.

### The deliverable, and the honest reading of it

| min_len | min_sup | = X% | routes | top support | distinct taxis | longest |
|---|---|---|---|---|---|---|
| ≥1 km | 5,000 | 0.310% | 639 | 14,330 | **435** | 4.70 km |
| ≥3 km | 2,500 | 0.155% | 292 | 4,701 | **435** | 6.17 km |
| ≥5 km | 1,000 | 0.062% | 195 | 1,945 | 369 | 8.72 km |
| ≥10 km | 50 | 0.003% | 217 | 107 | 84 | 12.41 km |
| ≥20 km | 2 | 0.0001% | 20 | 2 | **1** | 26.25 km |
| ≥40 km | 2 | 0.0001% | **0** | — | — | — |

The top ≥1 km and ≥3 km corridors are driven by **435 of the 442 taxis in the
fleet** — essentially every vehicle in Porto. Those are unambiguously public
routes.

**And then the band that "fills" turns out to be hollow.** All 20 routes at
≥20 km have ≤2 taxis, and the longest — 26.25 km — is **2 trips from a single
vehicle**. It satisfies the letter of the deliverable and means nothing as
popularity. This is the length-versus-confidence trade-off stated numerically:
median trips-per-taxi falls 17.9 → 8.5 → 4.0 → 1.2 → 1.0 as the length
requirement rises. Without the distinct-taxi column this would have been
reported as a 26 km popular corridor.

### The other findings held at full scale

- **Held-out generalisation:** lift **3.5–5.6x** over the null on 318 unseen
  trips (A 5.6x, C 4.5x, D 3.5x). Corridors mined from all 1.71M trips still
  describe how taxis move on data the pipeline never saw.
- **Temporal:** mean overlap **0.78**, night lowest at **0.69**, midday highest
  at 0.87 — near-identical to the 200k result, so the finding is stable in
  scale, not an artefact of sample size.
- **Cross-method:** A↔D agree strongly (A→D 0.93, D→A 0.84); C is the outlier
  (0.31–0.47), as it has been at every scale.

### One claim the full run disproved

The HyperLogLog justification was **wrong, and the measurement says so**: HLL is
**4.6x slower** than exact `countDistinct` even at 1.71M trips. The argument had
been "~85M (cell, taxi) pairs", but that is the *input* size; what decides
whether a distinct-count sketch pays is *cardinality per group*, and with only
442 taxis no cell can exceed 442 distinct values. There is no crossover to find.
The table is kept as a measured negative result — the shape to look for before
reaching for a cardinality sketch is an *unbounded* group, which this is not.

---

## 10. Honest limitations

1. **The DataProc run is done** (§9d) and verified against the local baseline.
   What it cost to get there is worth recording, because every one of these was
   invisible to local testing and none was a logic error:
   * the cluster VMs have **no route to PyPI** (`[Errno 101] Network is
     unreachable`). `gcloud` reports this as "initialization action timed out",
     which sends you to the timeout. Dependencies are now staged as
     platform-correct wheels in GCS and installed with `--no-index`.
   * **the driver received none of its environment.** `spark.yarn.appMasterEnv`
     reaches the driver only in *cluster* deploy mode; `gcloud dataproc jobs
     submit` uses *client* mode. The run crashed on a temp-dir path — which was
     luck, because the same gap left `OUTPUT_BASE` unset, and a run that got one
     line further would have written every result to a disk that is deleted with
     the cluster and exited 0. `storage.py` cannot catch that: it would be
     correctly writing to a correctly-resolved local path. `spark_session` now
     refuses to start a local-mode session on a node with `/etc/google-dataproc`.
   * **a failed cluster is not rolled back.** DataProc parks it in state ERROR
     with its VMs running so the logs can be read; the cleanup trap was armed on
     the line *after* `clusters create`, so `set -e` aborted before it existed.
     3 VMs billed unattended until deleted by hand.
   * **the hour-of-day buckets were machine-dependent** (§9b) — found only
     because the same code ran in two places.

   The lesson is the one the storage boundary was built for, arriving through a
   different door: the dangerous cloud failures are the ones that still exit 0.
2. **The ≥40 km configuration is empty, and ≥20 km is hollow** (§9d). Both are
   findings rather than gaps, but they should be presented as such rather than
   as a top-100 list the reader will assume is meaningful.
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
