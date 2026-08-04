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
| Top-100 long sub-routes at ≥1/3/5/10/20/40 km | all four miners → **`results/routes/*_top100_full.csv`** (the submitted bundle, 1.71M trips; `outputs/` is gitignored and holds only local runs) |
| ≥X% support, maximising length | X calibrated **per length config** — `route_mining_suffix_array.py` (all scales) and `route_mining_maximal.py` (sample-scale reference) |
| The "holes" (corridors fragmenting at forks) | holes section in both miners above |
| A clustering method | `route_mining_clustering.py` (Method A) |
| A **suffix tree / suffix array** method | `route_mining_suffix_array.py` (Method D) |
| A method that is neither | `route_mining_graph.py` (Method C) |
| Hash-based approximate structures | MinHash-LSH (A), Space-Saving + Count-Min (M7), HyperLogLog (zones — measured negative, §9), GK quantiles |
| Method comparison: runtime, accuracy, memory | `evaluation.py` → `method_comparison_*.md` |
| Map demo under Colab Enterprise | `notebooks/porto_routes_colab.ipynb`, all six configs |
| Popular routes / activity zones / anomalies | Methods A–D / `route_mining_graph` / `anomaly_analysis` |
| Cluster scalability (speedup / parallel efficiency) | 2/5/10/16 workers on the same 1.71M workload → `experiment_cluster_scaling.py` → `results/statistics/cluster_scaling_full.md` (§9f) |

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

## 2b. What "popular long sub-route" means here, exactly

Every method answers the same question, and the question has more degrees of
freedom than the phrase does. This is the operative definition; it is not
re-litigated per method.

| decision | this project | why |
|---|---|---|
| unit | a **contiguous** cell substring | a route with holes means the taxi teleported; see DESIGN_REVIEW §5 |
| support | **distinct trips** containing it, deduped within a trip | a trip driving a loop twice is one trip's worth of evidence |
| direction | **matters** — A→B and B→A are different routes | shingles are directed bigrams; the graph is directed |
| deviation | same route iff the **same H3 res-9 cells**, in order | resolution *is* the tolerance: ~174 m edge (§8) |
| length | Haversine between cell **centres**, gap-split first | §6, and the hop guard that makes it trustworthy |
| maximality | required for **B and D**, not for A and C | why `top_support` is not comparable across methods — §3 |
| second signal | **`support_taxis`**, distinct vehicles | 442 taxis total, so trips ≠ drivers — §9c |

**Ranking policy — the one the deliverable answers.** Two readings of "top-100
popular long sub-routes at ≥ L km" are defensible:

1. *(used)* among all sub-routes of length ≥ L, the **100 most popular**, ties
   broken by length — `orderBy(support desc, length_km desc)` in every miner.
2. among all sub-routes clearing the support floor, the **100 longest**.

Reading 1 is the primary answer, because "popular" is the noun the brief ranks on
and "long" is stated as a *minimum* (≥ L), which is a filter, not an objective.

Reading 2 is answered too, in the **support-floor sweep** that
`route_mining_suffix_array.py` publishes alongside the deliverable: for each
floor it reports the longest maximal route clearing it — `max(length_km)` over
the whole qualifying set, not over the top-100 cut. That is literally "the
longest route above the popularity threshold", swept across thresholds instead of
fixed at one. Read the two together: reading 1 gives the graded list, the sweep
gives the length/strictness frontier, and §9d shows where they diverge — at
≥20 km, where the longest qualifying route (26.25 km) was 2 trips from one taxi.
(That divergence was an artifact of the encoder starving the long bands; §9e
re-measures it after the fix, and the same band now holds 100 routes at a
median of 41 distinct taxis.)

Note the `longest_km` column in the per-band tables is the longest route *within
the top-100 by support*, not the global longest above the floor; those coincide
only when a band holds fewer than 100 routes.

**"Popular" is a claim about drivers, not only trips.** A corridor clearing the
floor on two trips from one vehicle is *frequent under the configured floor* and
is not an operationally popular city route. The two are separated by the
`support_taxis` column and reported separately — §9c and §9d.

---

## 3. The four methods (sample scale, ≥3 km config)

| | approach | routes | top support | longest |
|---|---|---|---|---|
| **A** clustering | MinHash-LSH on directed bigram shingles → greedy star clustering → longest cell run shared by ≥60% of members | 100 | 60 | 10.56 km |
| **B** maximal-frequent | n-gram support table → maximal among routes clearing X%, X calibrated per length | 99 | 20 | 6.89 km |
| **C** transition graph | PageRank zones + dominant-flow heavy paths, validated against trips | 65 | 43 | 7.29 km |
| **D** suffix array | generalised suffix array + LCP intervals | 95 | 60 | 5.05 km |

All four report the same unit — a contiguous sub-route with a distinct-trip
support — so `routes` and `longest` are directly comparable.

**`top support` is not.** The difference is definitional, not a defect. B and D
emit only *maximal* sub-routes: a route is reported only if no one-cell extension
is itself frequent. A and C have no such constraint, so they can report a short,
very common **prefix** of a longer corridor — exactly what B and D suppress as
redundant. Measured at ≥1 km on the full run: A's top route (4 cells, 1.10 km)
is contained in 79,952 trips and its best one-cell extension in 64,936, still far
above the band's floor; it is therefore not maximal, D omits it, and D's 14,330
describes a route that *cannot* be extended. Both counts are exact and they answer
different questions. **Read `top support` down a method's column, never across.**

`evaluation.py` states this in the generated `method_comparison_*.md`. This
section previously claimed the opposite and was not updated when the generator
was corrected — a report contradicting the pipeline that produced it, which is
the one thing this project treats as a defect in its own right.

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

Plus **73 unit tests**, including brute-force cross-checks of the LCP-interval
enumeration (8 cases) and the Aho-Corasick automaton (400 randomised trials), and
a regression test that a window may never span a GPS gap.

---

## 7. Approximate vs exact (measured)

Accuracy is `overlap@100` against the exact top-100, so measuring it requires
running the exact baseline the sketches exist to *avoid*. At 1.71M that means
shuffling 359,752,506 window rows, so the full-scale run used `--approx-only`
and reports cost without accuracy. **Accuracy below is quoted at 5k, cost at
1.71M, and the two are not interchangeable** — an unlabelled recall figure would
imply a full-scale measurement that was never taken.

Source: `results/statistics/m7_approx_mining_sample.md` (the one sample-scale
file in the results bundle, kept for exactly this reason) and
`m7_approx_mining_full.md`.

### Accuracy at 5k, re-measured 2026-08-04

| min_len | candidates | overlap@100 | precision | recall | rel. error |
|---|---|---|---|---|---|
| 1 km | 42,822 | 99 | 0.99 | **0.99** | 0.000 |
| 3 km | 45,755 | 94 | 0.94 | **0.94** | 0.000 |
| 5 km | 13,007 | 97 | 0.97 | **0.97** | 0.000 |
| ≥10 km | 5,845 | 97 | 0.97 | **0.97** | 0.000 |
| ≥20 km | 32,530 | 78 | 0.78 | **0.78** | 0.000 |
| ≥40 km | 564 | 0 | 0.00 | **0.00** | — |

**Relative error is 0.000 in every retained band, and that is a consequence of a
fix rather than a coincidence.** Candidates are filtered on Space-Saving's
guaranteed **lower** bound, not its estimate: a route is emitted only when the
sketch's one-sided error cannot have inflated it into the list. Filtering on the
upper bound asks *"could this be popular?"*; the deliverable asks *"is this
demonstrably shared?"* Before that fix, 100 rows shipped at ≥40 km with estimate
8 and lower bound 1 — a band the exact method reports as empty.

### Cost

| | 5k | 1.71M |
|---|---|---|
| sketch time | 4.6 s | 1,290.7 s |
| sketch memory | **82.2 MB** (fixed by capacity) | **172.8 MB** (fixed by capacity) |
| exact groupBy | 1.8 s | not run (`--approx-only`) |
| exact key table | 253.7 MB (578,880 keys) | — |
| window rows into the exact path | 1,162,961 | 359,752,506 |
| memory ratio | **3.1× smaller** | — |

**The case for the sketches is memory, not time.** The sketch bundle is bounded
by its capacity, so it grows 82 → 173 MB while the exact key table it replaces
grows with the number of distinct sub-routes. Runtime is not a win: M7 is the
most expensive stage in the pipeline, because the sketch saves the key *table*
and not the window enumeration that feeds it.

### Two claims this section used to make, both now retracted

**First retraction (kept for the record).** It once read: *"the ≥10 km rows are a
sample artefact, not a sketch failure… it resolves with scale."* A 200k run
falsified that — recall at ≥10 km measured 0.17 at 5k and 0.01 at 200k.

**Second retraction: that first retraction was measuring the encoder, not the
sketch.** Both of those runs predate gap densification (§9e). The encoder was
dropping cells a vehicle demonstrably drove, so long windows were being destroyed
before the sketch ever saw them — and a comparison against an exact baseline
computed on the *same* damaged corpus cannot separate the two effects. Post-fix,
recall at ≥10 km is **0.97**, not 0.17. The 200k figures are withdrawn rather
than restated: the raw dataset is not held locally and re-measuring that scale
needs the cloud, so this section reports one scale honestly instead of two
scales where one is stale.

**What survives is the structural argument, with its location corrected.**
Space-Saving retains **heavy hitters**, and a corridor is long *because* few
trips repeat it — so long corridors sit in the tail by construction and compete
for retained slots against far more numerous short ones. That is a property of
the sketch, not of the corpus, and more data does not repair it. The measurement
now places the fall-off at **≥20 km (recall 0.78)** rather than at ≥10 km. The
earlier number attributed an encoder defect to the sketch and so put the cliff
two bands too early.

**This is still why Method D carries the deliverable.** Its longest corridor is
**25.83 km, supported by 59 trips across 48 distinct taxis** — and the sketch
still misses roughly a fifth of that band's top 100. Note that the reason has
changed with the data: pre-densification this corridor had support 2 and was
invisible to a top-k sketch *in principle*. It is now comfortably frequent, and
the sketch misses it through slot competition instead. The conclusion held while
its justification did not, which is the case worth flagging rather than quietly
inheriting.

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

> The ≥10 km row here carries the same encoder artifact §9e diagnoses — at
> sample scale the band held 13 routes for the same reason it held 22 at full
> scale. After densification the full-scale band holds 100 routes at a median
> of 212 distinct taxis. The `support_taxis` column below was still the right
> instrument; it was reading starved data.

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

> ⚠️ **The ≥10 km and ≥20 km rows below are SUPERSEDED by §9e.** They were
> measured before gap densification, which is now known to have been starving
> exactly those bands. The reasoning in this subsection is sound and the
> distinct-taxi column did its job; the *data* it was reasoning about was
> incomplete. Read §9e for the corrected figures.

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

## 9e. The bands were being starved by the encoder (2026-08-04 re-run)

The paragraph above is the right instinct applied to incomplete data. The
≥20 km band was not hollow because Porto lacks long shared corridors. It was
hollow because the *encoder* was destroying the evidence.

GPS is sampled every 15 s, so above ~32 km/h a vehicle crosses an H3 cell
between two fixes and that cell is never recorded. Measured on the encoded
corpus: only **95.1% of consecutive cells were adjacent**. A sub-route matches
only when *every* cell matches, so the fraction of windows surviving intact
falls as 0.951^(L-1):

| band | 1 km | 3 km | 5 km | 10 km | 20 km | 40 km |
|---|---|---|---|---|---|---|
| P(window intact) | 86% | 64% | 47% | **23%** | **5.5%** | 0.3% |

And two taxis matched each other only where their holes coincided, so support
decayed faster still. The measured bucket coverage tracked that curve almost
exactly, which is what exposed it.

`cells.densify_points` resamples the GPS **polyline** — not the cell chain —
so no cell along a segment can be stepped over. Bounded by the same derived
`max_cell_hop_km` that `split_at_gaps` uses, in the opposite direction: below
it a retained vehicle demonstrably drove the distance, above it we do not know
the path and must not invent one. Cost: **+4.6% cells** for **100% adjacency**
(`worst_hop_km` 0.368 = one cell step, across all 1,614,508 encoded trips).

### The corrected deliverable (Method D, top-100 per band, 1.71M trips)

| min_len | routes | median support | median taxis | max taxis | longest |
|---|---|---|---|---|---|
| ≥1 km | 100 | 7,370 | **422** | 438 | 16.37 km |
| ≥3 km | 100 | 6,118 | **412** | 434 | 16.37 km |
| ≥5 km | 100 | 3,009 | **384** | 422 | 18.90 km |
| ≥10 km | 100 | 592 | **212** | 306 | 21.44 km |
| ≥20 km | 100 | 67 | **41** | 75 | 25.83 km |
| ≥40 km | **0** | — | — | — | — |

**The "hollow" reading is reversed.** At ≥20 km, **0 of 100 routes have ≤2
taxis** — the minimum is 9 and the median is 41 distinct vehicles out of a
442-taxi fleet. The length-versus-confidence trade-off is real and still
visible (median trips-per-taxi falls 18.0 → 15.2 → 8.2 → 3.2 → 1.6 as the
length requirement rises), but it is a gradient, not a cliff, and it no longer
bottoms out at a single vehicle.

### ≥40 km is still empty — and now we can show the candidates were artifacts

The re-run *did* produce 10 candidates in the ≥40 km band, the longest claiming
44.0 km. Every one was support 2 from a **single** taxi, and every one re-entered
some cell **three times**. `_compact` removes only *consecutive* duplicates, so a
vehicle circling encodes `A>B>A>B` and accumulates length it never travelled;
densification simply gave such trips enough cells to reach 40 km of it.

The separation is total, so the threshold is derived rather than tuned:

| | 1–20 km bands (500 routes) | ≥40 km candidates (10 routes) |
|---|---|---|
| max visits to any one cell | **≤ 2, all of them** | **3, all of them** |

`cells.revisits_ok` (limit 2) removes **10 of 10 artifacts and 0 of 500 real
corridors**. Two visits is deliberate slack — driving a street and returning
along it is ordinary taxi behaviour. Three visits to the same ~200 m hexagon
inside one sub-route is not a corridor. **Porto has no 40 km stretch that two
taxis repeat**, and that conclusion is unchanged from §9d.

## 9f. Strong scaling: what adding machines actually buys

Everything above varies the *data* on a fixed cluster. This varies the
*cluster* on fixed data — the same 1,710,670-trip workload on 2, 5, 10 and 16
`n2-standard-4` workers. It is the one measurement in this project that cannot
be made on a laptop.

No extra instrumentation was needed: every stage of every run already appended
its wall time to `timings.jsonl`, and `cli._cluster_tags` stamps each row with
the cluster that produced it, so the sweep leaves a complete dataset behind as
a side effect of runs already paid for.

| workers | wall clock | speedup | parallel efficiency |
|---|---|---|---|
| 2 | 68m04s | 1.00x | 1.00 |
| 5 | 65m46s | 1.03x | 0.41 |
| 10 | 53m13s | 1.28x | 0.26 |
| 16 | 50m16s | **1.35x** | **0.17** |

**1.35x from 8x the machines.** The interesting part is not the curve but where
it comes from — the per-stage breakdown locates the ceiling instead of merely
reporting it:

| stage | share of wall (2w) | speedup 2w → 16w |
|---|---|---|
| `m7_approx` | **34%** | **1.04x** |
| `m18_temporal` | 19% | 1.71x |
| `m12_suffix_array` | 15% | **2.27x** |
| `m9_clustering` | 9% | 1.29x (capped at 50k trips *by design*) |
| `holdout_validation`, `m1_summary` | small | ~1.00x (fixed cost) |

Method D — the method that carries the deliverable — scales best. But
`m7_approx` is a third of the runtime and does not distribute at all, and
Method A is capped at `CLUSTERING_MAX_TRIPS` by a deliberate design decision
(§9b). Those two plus the fixed-cost stages are the whole story: a Karp-Flatt
serial fraction around 79%.

**Honest caveats.** n=1 per configuration — no repeats. `m7_approx` and
`m3_encoding` each ran *slower* at 5 workers than at 2, so run-to-run variance
is real and not separated from the trend here. And
`spark.sql.shuffle.partitions=400` is held fixed across all four runs, which is
the correct control for strong scaling but means partition granularity per core
varies 50:1 between the smallest and largest cluster; some of the efficiency
drop at the top end is that rather than Amdahl.

The practical reading for a budgeted project: at this data size the cheapest
configuration is also nearly the fastest, and buying more machines buys very
little. That is a finding about *this* workload at *this* scale, not about
Spark.

### The other findings held at full scale

- **Held-out generalisation:** lift **3.5–5.6x** over the null on 318 unseen
  trips (A 5.6x, C 4.5x, D 3.5x). Corridors mined from all 1.71M trips still
  describe how taxis move on data the pipeline never saw.
- **Temporal:** mean overlap **0.78**, night lowest at **0.69**, midday highest
  at 0.87 — near-identical to the 200k result, so the finding is stable in
  scale, not an artefact of sample size.
- **Cross-method:** A↔D agree strongly (A→D 0.97, D→A 0.86); C is the outlier
  (0.31–0.47), as it has been at every scale.

### One claim the full run disproved

The HyperLogLog justification was **wrong, and the measurement says so**: HLL is
**7.26x slower** than exact `countDistinct` even at 1.71M trips. The argument had
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
2. **The ≥40 km configuration is empty** (§9d, §9e) — a finding rather than a
   gap, and it should be presented as such rather than as a top-100 list the
   reader will assume is meaningful. The companion claim that ≥20 km is *hollow*
   was **wrong, and §9e retracts it**: that band was being starved by a sampling
   artifact in the encoder, and now holds 100 routes at a median of 41 distinct
   taxis. The limitation worth keeping from it is narrower — length and
   confidence still trade against each other, just as a gradient rather than a
   cliff.
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
9. **Method A's published numbers predate a counting fix and have not been
   re-measured.** Inside a cluster, the "shared by ≥60% of members" test compared
   a *segment* count against a *trip* count: a trip split at GPS gaps entered the
   candidate list once per gap-free stretch, so a single trip could satisfy the
   threshold several times over. Fixed — one entry per trip, deduped across its
   own segments — but every Method A figure in §3, §9b and §9d was produced by
   the old code and is **stale until `route_mining_clustering` is re-run**. The
   effect is bounded and one-directional: only gap-split trips were ever
   over-counted, and the reported `support` was always recounted globally by
   containment over all trips, so `support` was never wrong — what could be wrong
   is *which* run a cluster selected, and its `members_with_run`. Anything else
   would be a guess; it is not stated here until it is measured.
10. **The M7 sketch output before this round carried no route length in
   `--approx-only` mode** — the mode that runs at mid and full scale. Length was
   read from the exact aggregate that `--approx-only` exists to skip, so every
   row shipped a blank `length_km`, the one column the deliverable is filtered
   on. It is now recomputed from the sub-route key itself (exact, same function
   that produced it), and both `verify_approx_mining` and `verify_cloud_run` now
   fail on a blank length rather than skipping it.

---

## 11. How to reproduce

```bash
.venv/bin/python -m src.validate_env
.venv/bin/python -m src.make_sample --sample
.venv/bin/python -m src.run_pipeline --sample --verify   # 16 stages + 9 verifiers
.venv/bin/python -m pytest tests/ -q                     # the unit suite

.venv/bin/python -m src.make_sample --mid                # 200k
.venv/bin/python -m src.run_pipeline --mid               # scale behaviour
```

Cloud: set `PROJECT` and `BUCKET`, then `bash scripts/dataproc_submit.sh`.
Do a `SCALE=--sample` cloud rehearsal on a 2-worker cluster first — it costs
cents and proves the GCS path end to end before the full run.
