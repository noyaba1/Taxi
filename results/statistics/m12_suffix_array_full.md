# Method D: Suffix Array Sub-route Mining (full)
_generated: 2026-08-04T03:34:02+00:00_

trips: 1,614,508 | suffixes indexed: 27,033,877 | mining floor: 2 trips | candidate routes: 5,447,275

Suffixes are bucketed by their first 3 cells, so all occurrences of any sub-route land in one partition and per-partition counting is globally exact -- no cross-partition merge, no window explosion.

## Top maximal-frequent routes per length configuration

The support floor is calibrated **per configuration**: the TIGHTEST floor that still fills the band, i.e. the strongest claim the data supports at that length. It is absolute (a trip count) because a percentage floor gets harder to clear as the dataset grows -- see config.SUPPORT_MIN_SUP_GRID. The X% it corresponds to at this scale is reported alongside.

`support` counts distinct TRIPS; `taxis` counts distinct VEHICLES. A corridor with high support but very few taxis is one driver's habit, not a popular route.

| min_len_km | min_sup | = X% | #maximal(>=L) | top_support | taxis | longest_km |
|---|---|---|---|---|---|---|
| 1 | 5,000 | 0.3097% | 619 | 11,205 | 436 | 16.37 |
| 3 | 5,000 | 0.3097% | 172 | 8,948 | 425 | 16.37 |
| 5 | 2,500 | 0.1548% | 194 | 4,408 | 206 | 18.90 |
| 10 | 500 | 0.0310% | 181 | 977 | 306 | 21.44 |
| 20 | 50 | 0.0031% | 153 | 156 | 73 | 25.83 |
| 40 | 2 | 0.0001% | 10 | 2 | 1 | 43.99 |

## Support-floor sweep (how length trades against strictness)
| min_sup | = X% | #maximal-frequent | longest_km |
|---|---|---|---|
| 5,000 | 0.3097% | 619 | 16.37 |
| 2,500 | 0.1548% | 1,286 | 18.90 |
| 1,000 | 0.0619% | 3,059 | 20.71 |
| 500 | 0.0310% | 5,834 | 21.44 |
| 250 | 0.0155% | 11,004 | 21.83 |
| 100 | 0.0062% | 24,249 | 25.10 |
| 50 | 0.0031% | 43,767 | 25.83 |
| 20 | 0.0012% | 93,087 | 26.94 |
| 10 | 0.0006% | 164,257 | 29.47 |
| 5 | 0.0003% | 286,253 | 35.25 |
| 3 | 0.0002% | 423,395 | 40.65 |
| 2 | 0.0001% | 557,678 | 43.99 |

## Holes analysis (why maximal routes terminate = traffic forks)

Reference X=0.5% -> min_sup=8073 trips. A route ends where traffic splits: the corridor continues, but no single next cell carries 8073 trips, so a HOLE opens between this sub-route and the next.

| support | cells | length_km | best right continuation | best left continuation |
|---|---|---|---|---|
| 19108 | 6 | 1.83 | 6532 | 5396 |
| 17303 | 5 | 1.45 | 5858 | 4564 |
| 16090 | 4 | 1.10 | 7866 | 3051 |
| 14690 | 5 | 1.45 | 7460 | 7332 |
| 14456 | 7 | 2.16 | 6960 | 4293 |
