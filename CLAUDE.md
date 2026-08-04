# CLAUDE.md

Orientation for working in this repo. Read this before changing anything.

## What this is

Porto Taxi trajectory analysis for a Big Data / Cloud Computing course project.
PySpark, local-first, designed to run unchanged on GCP DataProc.

**Deliverable:** top-100 **popular long sub-routes** at minimum lengths
{1, 3, 5, 10, 20, 40} km, found by four independent methods, plus activity
zones, anomalous routes, an approximate-vs-exact comparison, and a map.

Dataset: 1,710,670 trips, **442 taxis**, 2013–2014, GPS every 15 s.

## The one idea everything rests on

After spatial encoding, **a trip is a string over an H3-cell alphabet**:

- sub-route = contiguous **substring**
- popular = **support** (distinct trips *and* distinct taxis)
- long = ground length ≥ L km

One representation feeds all four methods, which is what makes cross-method
comparison meaningful.

## Environment (both of these break Spark silently)

| | required | symptom if wrong |
|---|---|---|
| Java | 8, 11 or **17** | opaque JVM/reflection errors that look like a code bug |
| Python | 3.8–**3.12** (use 3.11) | `PYTHON_VERSION_MISMATCH`, only once a `pandas_udf` runs |

`spark_session.py` locates a supported JDK and pins `PYSPARK_PYTHON` itself —
you should not need to export anything. `python -m src.validate_env` is the gate.

Always use `.venv/bin/python`.

## Commands

```bash
.venv/bin/python -m src.validate_env                    # env gate
.venv/bin/python -m src.make_sample --sample            # build a scale
.venv/bin/python -m src.run_pipeline --sample --verify  # 16 stages + 9 verifiers
bash scripts/run_tests.sh                               # 60 tests (env gate first)
bash scripts/run_tests.sh --fresh                       # rebuild .venv from the lock
bash scripts/cloud_rehearsal.sh                         # URI-path dress rehearsal
```

`requirements.txt` says what the project asks for and why; `requirements.lock.txt`
is the resolved transitive closure, frozen from a venv built from scratch that ran
the suite green. Neither can pin the JDK — `validate_env` is the gate that does.

Scales: `--sample` (5k) · `--mid` (200k) · `--scale s400k|s800k` · `--full` (1.71M,
~33 min locally with `SPARK_SHUFFLE_PARTS=200 SPARK_DRIVER_MEM=10g`).

## Conventions that matter

**Paths.** `config.dataset_paths(scale)` is the *only* place a parquet filename
is built. Six modules once reconstructed them independently and the mismatch
shipped (commit `adf2caf`). Do not reintroduce that.

**Output I/O.** Everything under `OUTPUT_BASE` goes through `src/storage.py`.
Never use `open()` / `os.makedirs` for results — on a `gs://` base those silently
create a local directory literally named `gs:` and the cloud run loses its
outputs. `storage` dispatches to Hadoop FS for any path with a URI scheme.

**Row access.** Access Spark rows **by column name**, never `r[0]`. Adding
`TAXI_ID` to a frame once turned a positional `r[0]` into an int and broke
Method C at full scale.

**Reports.** Derive conclusions from computed numbers. `evaluation.py` used to
end with a hard-coded paragraph asserting which methods agreed; re-running on
different data produced a report that contradicted its own tables.

**Verifiers must exit non-zero.** 8 of 9 once printed `VERIFICATION FAILED` and
exited 0, so `--verify` reported OK regardless. A check that cannot fail is
documentation, not verification.

## Two guards you must not weaken

**1. Corrupt trajectories are excluded, not just flagged.** Sub-route length is
measured between cell *centres*, so a window spanning a GPS gap reports the gap's
width as route length. Measured without the guard: worst window **53.6 km per
cell-hop** (45× the physical bound) and **563 windows ≥10 km built across gaps**,
one claiming 59.8 km from 19 cells — straight into the graded lists.

Two layers: `spatial_encoding` drops `is_anomalous` trips, and every miner splits
trajectories at any hop above `config.max_cell_hop_km()` (~1.18 km at res 9 =
retained-speed limit + cell quantisation, *derived*, not tuned).

**1b. …but a discontinuity BELOW that bound is undersampling, and gets filled.**
The same threshold answers both questions, in opposite directions. GPS ticks
every 15 s, so above ~32 km/h the vehicle crosses a res-9 cell between two fixes
and that cell is never recorded. Measured: only **95.1% of consecutive cells
were adjacent**. A window matches only if *every* cell matches, so clean windows
decayed as 0.951^(L-1) — 86% at 1 km but **23% at 10 km and 5.5% at 20 km**, and
two taxis matched only where their holes coincided. That is what hollowed out
the long bands; it was the encoder, not Porto.

`cells.densify_points` resamples the GPS polyline (not the cell chain) so no
cell can be stepped over, giving **100% adjacency for +4.6% cells**. Measured
effect at sample scale: method D's ≥10 km band went **18 → 100 routes**, ≥20 km
**0 → 13**, and median support roughly doubled at 3–5 km — support is
independent of length measurement, so that is genuine extra matching.
**≥40 km stayed empty**, which is the load-bearing check that this repaired
rather than inflated.

Repair in *geographic* space, never in grid space: patching with
`h3.h3_line(A, C)` picks the wrong intermediate cell 5 times in 16, restoring
contiguity while leaving the two taxis on different chains. Densification is
bounded by `max_cell_hop_km` and therefore never invents a path across a real
gap — it fills what a retained vehicle demonstrably drove, and `split_at_gaps`
still cuts everything beyond.

**2. Support floors are ABSOLUTE, not percentages.** A percentage floor is
scale-dependent the wrong way: 0.01% is 2 trips on the sample but 172 at 1.71M,
so the long length bands empty out *as data grows*. `SUPPORT_MIN_SUP_GRID` is
absolute; the equivalent X% is reported alongside.

## Layout

```
src/
  config.py          single source of truth: paths, scales, thresholds, floors
  storage.py         the ONLY I/O boundary for outputs (gs:// aware)
  cli.py             scale flags, logging, per-stage timing -> timings.jsonl
  cells.py           cell-path geometry + split_at_gaps (the corruption guard)
  ahocorasick.py     multi-pattern containment (replaced ~5e9 substring scans)
  clean_data · feature_engineering · summarize_features · spatial_encoding
  route_mining_{clustering,maximal,graph,suffix_array}   <- methods A, B, C, D
  route_mining_{exact,closed,approx}                     <- baselines + sketches
  anomaly_analysis · evaluation · visualization · validate_holdout
  temporal_analysis · experiment_{scaling,cluster_cap}
  verify_*.py        one independent verifier per stage
docs/    FINAL_REPORT (the graded write-up) · ARCHITECTURE · DATAPROC (canonical
         cloud doc; other run-books are marked superseded)
scripts/ run_tests.sh · cloud_rehearsal.sh (free) · dataproc_submit.sh (DRY_RUN=1)
tests/   test_pure_functions (no JVM) · test_storage + test_method_a_spark
         (each starts a real SparkSession — if they SKIP, the JDK is missing and
         the checks that matter most did not run)
```

## The four methods

| | file | approach |
|---|---|---|
| A | `route_mining_clustering` | MinHash-LSH → star clustering → longest run ≥60% of members share |
| B | `route_mining_maximal` | n-gram support table → maximal-frequent. **Sample scale only** (quadratic) |
| C | `route_mining_graph` | transition graph → PageRank zones + dominant-flow heavy paths |
| D | `route_mining_suffix_array` | generalised suffix array + LCP intervals. **Exact and scalable — carries the deliverable** |

**Why B is sample-only:** it shares the O(n²) window table with M5/M6. Measured
at 200k: M5 OOMs, M8 spilled 21 GB without finishing, D does the same job in
41 s. `run_pipeline` gates stages per scale and CI asserts the cloud script
matches.

## Things that are findings, not bugs

- **≥40 km is empty at every scale.** Porto has no 40 km stretch two taxis
  repeat. A power law fitted on 5k–755k predicted 26–28 km at 1.71M; measured
  **26.25 km**. Don't "fix" this.
- **≥20 km is thin — but re-measure before quoting a number.** The published
  figures predate gap densification (guard 1b), which was worth 18 → 100 routes
  at ≥10 km on the sample. Length and confidence still move in opposite
  directions — hence the `support_taxis` column — but "hollow" was partly the
  encoder starving the band, so the old counts understate it.
- **HyperLogLog is 4.6× slower than exact here.** Group cardinality is capped by
  the 442-taxi fleet, so the sketch never pays. Kept as a measured negative
  result.
- **No Bloom filter.** It is on the brief's list but nothing here would earn it;
  saying why is worth more than the checkbox.

## Done, and where the proof is

The DataProc run **has happened** (2026-07-26): 1 master + 5 workers, 1.71M trips,
every input and output on `gs://`, 60 min, $2.71. Method D reproduced 420/420
corridors bit-identically against the local baseline. Results in FINAL_REPORT §9d,
proof in `docs/cloud_evidence/`. The presentation deck (`scripts/build_deck.py`)
and the `.docx` developer guide are built. Do not re-plan any of this.

## Still outstanding

1. **Two re-runs that supersede stale numbers.** Both landed after the graded run,
   so the published figures predate them (FINAL_REPORT §10.9, §10.10):
   * **Method A** — the cluster-internal "≥60% of members" test compared a
     *segment* count to a *trip* count, so a gap-split trip could clear it alone.
     Fixed; A's extents are stale until re-measured. `support` was never affected.
   * **M7 `--approx-only`** — every row shipped a blank `length_km`. Fixed by
     recomputing from the sub-route key.

   `ONLY="route_mining_clustering.py route_mining_approx.py"` re-runs just these
   — but note `ONLY` is read by `scripts/dataproc_submit.sh` ONLY.
   `run_pipeline` ignores it silently, so the same command run locally is a
   full-pipeline run wearing a disguise.
2. **EVERY published number now predates gap densification** (guard 1b), not just
   the two items above. Re-measure before quoting anything from FINAL_REPORT.
3. **Oscillation windows are only hidden, not fixed.** `_compact` drops
   *consecutive* duplicates, so a parked taxi whose GPS jitters across one cell
   boundary still encodes `A>B>A>B>…`, and the window miner counts every bounce
   as travel. Measured: one such window claimed **42.2 km from 117 cells that
   were really two.** The support floor (item below) keeps them out of the graded
   lists because no two trips jitter identically — but that is luck, not a
   guard. A distinct-cells/total-cells ratio test on each window would be the
   real fix.
4. **A `results/` bundle.** `outputs/` is gitignored, so the archive contains no
   route tables at all — nothing in it is reproducible without bucket access.
5. **Moodle submission** — one student submits, links all members.

`gcloud`/`gsutil` are denied in `.claude/settings.json` on purpose: the cloud run
spends real budget and should be typed by a person who means it.
