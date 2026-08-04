# Results bundle — Porto Taxi, full scale (1,710,670 trips)

Produced by the DataProc run of 2026-08-04 (`gs://taxi-project-noyabayazi/taxi`),
1 master + {2,5,10,16} `n2-standard-4` workers, `europe-west1`.

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

**>=40 km is empty, and that is a finding.** Porto has no 40 km stretch that two
taxis repeat. The raw run produced 10 candidates in that band; every one was
support 2 from a SINGLE taxi, and every one re-entered some cell three times --
a vehicle circling, whose "length" is the sum of the circling. Across the 500
routes in the 1-20 km bands no cell is ever entered more than twice, so the
guard (`cells.revisits_ok`, limit 2) removes 10 of 10 artifacts and 0 of 500 real
corridors. Rows removed by that guard are excluded here; see the note below.

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

## Known limitations

* `approx_top100_full.csv` at >=40 km: 17 rows survive the revisit guard, all
  with a Space-Saving lower bound of 1 -- the sketch cannot rule out that they
  are single-trip routes, and Method D (exact) finds none. Reported as a measured
  demonstration of one-sided sketch error where support is thinnest, not as
  corridors.
* The route tables here were filtered by the revisit guard AFTER the cloud run;
  the miners now apply it at window emission, so a re-run reproduces these files
  directly. That re-run has not been performed.
