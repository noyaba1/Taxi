# Results bundle — Porto Taxi, full scale (1,710,670 trips)

Produced by the DataProc runs of 2026-08-04 (`gs://taxi-project-noyabayazi/taxi`),
1 master + 5 `n2-standard-4` workers in `europe-west1` (cluster `porto-final`,
completed by `porto-final2`; live evidence in `docs/cloud_evidence/*_20260804.*`).
The 2/5/10/16-worker sweep behind the scaling study is a separate set of runs.

## Route tables (`routes/`)

Top-100 popular long sub-routes per minimum length {1,3,5,10,20,40} km, by four
independent methods over one shared H3 (res 9) representation.

| method | file |
|---|---|
| A  MinHash-LSH clustering | `clustering_top100_full.csv` |
| C  transition graph       | `graph_heavy_paths_top100_full.csv` |
| D  generalised suffix array (carries the deliverable) | `suffix_array_top100_full.csv` |
| M7 sketches (Space-Saving + Count-Min) | `approx_top100_full.csv` |

Method B (maximal-frequent) is sample-scale only: it shares an O(n^2) window
table that OOMs at 200k. Method D reproduces its output exactly in one pass.

## Coverage

| band | A | C | D |
|---|---|---|---|
| 1 km | 100 | 100 | 100 |
| 3 km | 100 | 100 | 100 |
| 5 km | 100 | 100 | 100 |
| 10 km | 100 | 44 | 100 |
| 20 km | 1 | 0 | 100 |
| 40 km | 0 | 0 | **0** |

All four methods, including the sketch-based M7, now report the ≥40 km band
empty.

**>=40 km is empty, and that is a finding.** Porto has no 40 km stretch that two
taxis repeat. The raw run produced 10 candidates in that band; every one was
support 2 from a SINGLE taxi, and every one re-entered some cell three times --
a vehicle circling, whose "length" is the sum of the circling. Across the 500
routes in the 1-20 km bands no cell is ever entered more than twice, so the
guard (`cells.revisits_ok`, limit 2) removes 10 of 10 artifacts and 0 of 500 real
corridors. **The miners now apply it at emission**, so this run produced clean
tables rather than tables that needed cleaning: Method D emitted 0 routes at
≥40 km, 0 violating the guard, a maximum length of 25.83 km (down from a
cap-adjacent 43.99) and a minimum support of 56 (up from 2).

**>=20 km is real.** 100 routes at a median of 41 distinct taxis -- not one
driver's commute. Earlier runs reported this band as hollow because the encoder
was starving it (see below), not because Porto lacks long shared corridors.

## What changed since the earlier published figures

GPS is sampled every 15 s, so above ~32 km/h a vehicle crosses an H3 cell
between two fixes and that cell is never recorded. Measured: only **95.1% of
consecutive cells were adjacent**. A sub-route matches only if every cell
matches, so intact windows decayed as 0.951^(L-1) -- 86% at 1 km but 23% at
10 km and 5.5% at 20 km, and two taxis matched only where their holes coincided.

`cells.densify_points` resamples the GPS polyline (not the cell chain) so no cell
can be stepped over: **100% adjacency for +4.6% cells**, `worst_hop_km` 0.368
across all 1,614,508 encoded trips. Effect on the deliverable: >=10 km went from
22 routes to 100, >=20 km from 0 to 100, and median support roughly doubled at
3-5 km -- support is independent of length measurement, so that is genuine extra
matching rather than inflation.

## Scaling study (`statistics/cluster_scaling_full.md`)

Same workload on 2/5/10/16 workers: **1.35x speedup from 8x the machines**,
efficiency 1.00 -> 0.17. The per-stage table locates the ceiling: `m7_approx` is
34% of wall time and scales 1.04x, and Method A is capped at 50k trips by
design. Method D scales best at 2.27x. Caveat: n=1 per configuration, and two
stages ran slower at 5 workers than at 2, so run-to-run variance is real.

## Where the sketch and the exact method disagree

`approx_top100_full.csv` had 100 rows at ≥40 km whose Space-Saving **estimate**
was 8 but whose guaranteed **lower bound** was 1 -- the sketch could not rule out
that every one was a single trip, while Method D (exact) reports the band empty.
They are excluded here, and the reason is worth stating rather than hiding: a
frequency sketch's one-sided error bites hardest exactly where support is
thinnest, which is precisely the long bands. Filtering on the upper bound asks
"could this be popular?"; the deliverable needs "is this demonstrably shared?",
which is the lower bound. `route_mining_approx` now applies that floor itself.

### How accurate the sketch is, and at which scale

`statistics/m7_approx_mining_sample.md` is the **only** file in this bundle at
sample scale, and it is here deliberately. Accuracy is `overlap@100` against the
exact top-100, so measuring it requires running the exact baseline the sketches
exist to avoid: at 1.71M that means shuffling 359,752,506 window rows, so the
full-scale run used `--approx-only` and reports cost without accuracy. Cost is
therefore quoted at full scale and accuracy at sample scale; conflating the two
would imply a 1.71M recall figure that was never measured.

Re-measured after the lower-bound fix above: recall 0.99 / 0.94 / 0.97 / 0.97 at
1–10 km, **0.78** at ≥20 km, 0.00 at ≥40 km. Mean relative error is **0.000**
across every retained band -- a consequence of the fix, not a coincidence, since
filtering on the guaranteed lower bound only admits routes whose count the sketch
cannot have overstated. The ≥20 km fall-off is structural: Space-Saving retains
heavy hitters, and a corridor is long *because* few trips repeat it, so long
corridors sit in the tail by construction. More data does not repair this.

## Known limitations

* Two post-hoc filters were applied when assembling this bundle, both now fixed
  in the miners for future runs: the 100 sketch rows above, and **1 route from
  Method A** at ≥10 km that violated the revisit guard. Method A builds its
  corridors from the longest run shared within a cluster rather than by window
  emission, so it did not inherit the guard the other methods apply at source.
* Method A reports 1 route at ≥20 km and Method C none: both cap or prune their
  candidate space by design (`CLUSTERING_MAX_TRIPS`, `GRAPH_MIN_EDGE_SUPPORT`),
  and Method D -- exact and scalable -- is the method that carries the
  deliverable at length.
