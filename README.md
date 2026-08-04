# Porto Taxi Trajectory Analysis — Big Data / Spark Project

Local-first PySpark project designed to migrate cleanly to **GCP DataProc**.
Build and debug on a small sample, validate at mid scale on one machine, and only
then spend cloud budget.

**Deliverable:** the top-100 **popular long sub-routes** for minimum lengths
{1, 3, 5, 10, 20, 40} km, found by **four independent methods**, plus activity
zones, anomalous routes, an approximate-vs-exact comparison, and a map.

---

## Dataset

| File | Size | What it is |
|------|------|-----------|
| `train.csv` | ~1.9 GB | Full Porto dataset, **1,710,670** trips, 442 taxis, 2013–2014 |
| `Porto_taxi_data_test_partial_trajectories.csv` | 447 KB | Test set, partial trajectories |
| `solution_challengeII.csv` | — | `TRIP_ID, TRAVEL_TIME` ground truth |
| `solution_fixed.csv` | — | `TRIP_ID, LATITUDE, LONGITUDE` ground truth |

Ships inside `taxi+service+trajectory+prediction+challenge+ecml+pkdd+2015/`;
`config.py` also accepts `train.csv/train.csv` or a plain `train.csv`, or an
explicit `RAW_TRAIN=` override.

**Schema:** `TRIP_ID, CALL_TYPE, ORIGIN_CALL, ORIGIN_STAND, TAXI_ID, TIMESTAMP,
DAY_TYPE, MISSING_DATA, POLYLINE`.

> ⚠️ **POLYLINE is `[longitude, latitude]` (lon first).** GPS sampled every 15 s,
> so `duration ≈ (n_points − 1) × 15` seconds. Getting lon/lat backwards flips
> the entire map — the single most common bug in this dataset.

---

## Quick start

See [SETUP.md](SETUP.md) for the full environment (Java 17, Python 3.11).

```bash
.venv/bin/python -m src.validate_env                    # env gate
.venv/bin/python -m src.make_sample --sample            # 5,000 trips
.venv/bin/python -m src.run_pipeline --sample --verify  # 16 stages + 9 verifiers
.venv/bin/python -m pytest tests/ -q                    # the unit suite
```

Open `outputs/maps/porto_map_sample.html`.

---

## Pipeline

```
raw CSV
  → clean_data           parse POLYLINE, reject corrupt trips, dedupe TRIP_ID
  → feature_engineering  distance / speed / sinuosity / bbox + anomaly flags
  → summarize_features   distribution statistics
  → spatial_encoding     H3 res-9 cell sequences  (anomalous trips EXCLUDED here)
  → four mining methods + anomaly analysis
  → evaluation (A/B/C/D comparison) · visualization (Folium / Colab)
```

Central idea: after encoding, **a trip is a string over an H3-cell alphabet**, so
a sub-route is a contiguous substring, "popular" = distinct-trip support, "long"
= ground length ≥ L. One representation feeds every method.

### The four methods

| | file | approach | popularity |
|---|---|---|---|
| **A** | `route_mining_clustering.py` | MinHash-LSH over directed bigram shingles → greedy star clustering → the longest cell run ≥60% of members share | distinct-trip support |
| **B** | `route_mining_maximal.py` | contiguous n-gram support table → maximal among routes clearing X%, **X calibrated per length config** | distinct-trip support |
| **C** | `route_mining_graph.py` | directed cell-transition graph → PageRank activity zones + dominant-flow heavy paths, validated against real trips | distinct-trip support |
| **D** | `route_mining_suffix_array.py` | generalised **suffix array + LCP intervals** — exact, without enumerating windows | trips **and distinct taxis** |

All four report the same unit, so the cross-method comparison compares like with
like. Supporting stages: `route_mining_exact.py` (exhaustive baseline, ground
truth at small scale), `route_mining_closed.py` (closed sub-routes),
`route_mining_approx.py` (Space-Saving + Count-Min sketches vs exact).

### Approximate structures

MinHash-LSH (Method A), Space-Saving frequent-items and Count-Min (M7),
Greenwald-Khanna quantiles (anomaly fences), and **HyperLogLog** for distinct
taxis per cell in the activity zones — each compared against exact.

No Bloom filter: there is no place in this pipeline where it earns its keep
(containment is already one Aho-Corasick pass), and adding a structure in order
to name it is the opposite of what the brief rewards.

### What "popular" means here

Support counts distinct **trips** *and* distinct **taxis** — with only 442
vehicles over a year, a corridor driven many times by one driver is a habit, not
a route.

At full scale this is the difference between a real answer and a wrong one. The
top ≥1 km and ≥3 km corridors are driven by **435 of the 442 taxis** — the whole
fleet, unambiguously public. But every route in the ≥20 km band has ≤2 taxis,
and the longest (26.25 km) is **2 trips from a single vehicle**. Without the
taxi column that would have been reported as a 26 km popular corridor.

---

## Scales

| flag | trips | purpose |
|---|---|---|
| `--sample` | 5,000 | correctness; every stage and verifier, ~3 min |
| `--mid` | 200,000 | real shuffle, skew and spill on one machine |
| `--scale s400k` / `s800k` | 400k / 800k | points for the scaling study |
| `--full` | 1,710,670 | **runs locally in ~33 min**; also the DataProc target |

The full dataset has been run end to end on one 8-core / 16 GB laptop
(`SPARK_SHUFFLE_PARTS=200 SPARK_DRIVER_MEM=10g`). See `docs/FINAL_REPORT.md` §9d
for stage timings and results.

**Measured:** the exhaustive window miner emits ~34M window rows at 200k trips
and OOMs on 16 GB; the suffix array indexes the same data as 2.8M suffixes in
26 s. So the window-based miners run at `--sample` only (they are the ground truth the others are
checked against), and `route_mining_exact` refuses to start above
`EXACT_MAX_TRIPS` rather than failing an hour in. `run_pipeline` picks the right
stages per scale automatically.

---

## Correctness

Every stage ships a `verify_*.py` that recomputes its result by an **independent
method**:

- sub-route support by brute-force substring containment vs the mining's
  window/groupBy or Aho-Corasick path;
- the suffix array's supports cross-checked against the exhaustive baseline
  (**0 disagreements** across all shared routes on the sample);
- sketch bounds (Space-Saving `[lb,ub]` brackets truth, Count-Min ≥ truth) and
  determinism;
- clustering cohesion and route continuity;
- graph routes validated against real trips (anti-"Frankenstein");
- anomaly self-consistency.

Plus 73 unit tests, including brute-force cross-checks of the LCP-interval
enumeration and the Aho-Corasick automaton, and a regression test for the storage
layer that decides whether cloud results survive teardown.

**And one check that is not internal.** Every verifier above recounts against the
same table the mining used — that catches bugs, not self-deception. So
`validate_holdout.py` tests the mined corridors against the dataset's held-out
split, which never enters the pipeline: unseen trips traverse mined corridors
**3.0–6.0x** more often than random walks over the same city's own road
adjacency. That is the only evidence here that the corridors are real rather than
memorised.

### Corrupt data is removed, not just flagged

Phase 2 flags physically impossible trajectories (GPS teleport, >200 km/h
segment, parked, outside the metro box); `spatial_encoding` **excludes** them
before mining, and every miner additionally splits trajectories at any hop larger
than `config.max_cell_hop_km()` (~1.18 km at res 9 — the retained-speed limit
plus cell quantisation).

This matters because sub-route length is measured between cell centres. Measured
on the 5k sample without the guard: the worst window reported **53.6 km per cell
hop** (45× the physical bound) and **563 windows ≥10 km were built across GPS
gaps** — one claiming 59.8 km from 19 cells. Those land directly in the graded
≥10/20/40 km lists. With the guard: worst ratio 1.10 km, zero violations.

---

## GCP DataProc

Almost nothing in the code changes — by design:

| Concern | Local | DataProc |
|---------|-------|----------|
| Storage | `data/` | `DATA_BASE=gs://bucket/porto` |
| Results | `outputs/` | `OUTPUT_BASE=gs://bucket/porto/outputs` (see `src/storage.py`) |
| Spark master | `local[*]` | `SPARK_ENV=cloud` (YARN provides it) |
| Submit | `python -m src.<stage>` | `bash scripts/dataproc_submit.sh` |

`src/storage.py` writes every report and CSV through Hadoop FS when the path has
a URI scheme, so results land in GCS and survive cluster teardown.

> Budget rule: **never debug in the cloud.** Get correct results on `--sample`,
> exercise the shuffle on `--mid`, then do one cheap `--sample` cloud rehearsal
> before the full run.

---

## Layout

```
src/       config, storage, cli, cells, ahocorasick, spark_session, load_data,
           make_sample, clean_data, feature_engineering, summarize_features,
           spatial_encoding, route_mining_{exact,closed,suffix_array,approx,
           maximal,clustering,graph}, anomaly_analysis, evaluation,
           visualization, run_pipeline, validate_env, + verify_*.py per stage
tests/     pytest unit tests (pure functions + storage layer)
.github/   CI: tests, every-module-imports, cloud/local stage-list consistency
docs/      ARCHITECTURE, DESIGN_REVIEW, DATAPROC, FINAL_REPORT, …
scripts/   dataproc_submit.sh
notebooks/ porto_routes_colab.ipynb   (Colab Enterprise demo, all six configs)
```

`data/` and `outputs/` are git-ignored and regenerated by
`python -m src.run_pipeline --sample --build-sample`.
