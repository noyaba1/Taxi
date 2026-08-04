# Cross-Method Comparison (full)

- **A** = clustering
- **B** = maximal-frequent
- **C** = transition-graph
- **D** = suffix-array

All four report contiguous sub-routes and count support the same way (distinct trips containing the route), so a single support number means the same thing everywhere.

**`top_support` is NOT comparable across methods, and the difference is definitional, not a defect.** B and D report only *maximal* sub-routes: a route is emitted only if no one-cell extension is itself frequent. A and C have no such constraint, so they may report a short, very common PREFIX of a longer corridor -- which B and D deliberately suppress as redundant.

Measured on this dataset at `>=1 km`: A's top route (4 cells, 1.10 km) is contained in 79,952 trips, and its best one-cell extension is still contained in 64,936 -- far above the band's floor. It is therefore not maximal, D omits it, and D's top figure (14,330) describes a route that cannot be extended. Both counts are exact; they answer different questions. Read `top_support` DOWN a method's column, never ACROSS.

## Routes / popularity / longest, per min-length
| method | min_len | #routes | top_support | longest_km |
|---|---|---|---|---|
| A | 1 | 100 | 81039 | 10.18 |
| C | 1 | 100 | 68133 | 5.86 |
| D | 1 | 100 | 11205 | 16.37 |
| A | 3 | 100 | 58263 | 10.18 |
| C | 3 | 100 | 13762 | 15.66 |
| D | 3 | 100 | 8948 | 16.37 |
| A | 5 | 100 | 58263 | 11.63 |
| C | 5 | 100 | 9186 | 15.72 |
| D | 5 | 100 | 4408 | 18.90 |
| A | 10 | 100 | 19490 | 23.63 |
| C | 10 | 44 | 2831 | 15.72 |
| D | 10 | 100 | 977 | 21.44 |
| A | 20 | 1 | 16 | 23.63 |
| C | 20 | 0 | - | - |
| D | 20 | 100 | 156 | 25.83 |
| A | 40 | 0 | - | - |
| C | 40 | 0 | - | - |
| D | 40 | 0 | - | - |

## Cost per stage (measured this run)
| stage | wall_s | peak_rss_mb | rows_in | rows_out | shuffle_records |
|---|---|---|---|---|---|
| m7_approx | 1342.2 | 1419.7 | - | - | 387,000,554 |
| m18_temporal | 450.0 | 326.1 | 1,614,508 | - | - |
| m12_suffix_array | 373.1 | 354.6 | 1,614,508 | 5,107,086 | 27,033,877 |
| m9_clustering | 347.0 | 348.4 | 49,929 | 3,309 | 423,629 |
| m10_graph | 236.2 | 349.8 | 1,614,508 | - | 17,438 |
| m3_encoding | 179.3 | 337.2 | 1,663,886 | 1,614,508 | - |
| p1_clean | 102.2 | 332.7 | 1,710,670 | 1,663,886 | - |
| p2_features | 86.0 | 341.4 | 1,663,886 | 1,663,886 | - |
| m11_anomalies | 47.7 | 351.5 | 1,663,886 | - | - |
| m1_summary | 33.6 | 332.2 | 1,663,886 | 1,663,886 | - |
| holdout_validation | 10.9 | 328.9 | - | - | - |

## Cross-method overlap at >=3 km
Fraction of the ROW method's routes with a cell-set Jaccard>=0.5 match in the COLUMN method.

| row\col | A | C | D |
|---|---|---|---|
| A | 1.00 | 0.47 | 0.97 |
| C | 0.46 | 1.00 | 0.72 |
| D | 0.86 | 0.45 | 1.00 |

## Is it a popular route, or one driver's habit?

`support` counts distinct trips; `taxis` counts distinct vehicles. A corridor with many trips but few taxis is a commute, a depot run or a rank shuttle -- not a route the city uses. Ratio = trips per taxi.

| min_len_km | routes | median trips/taxi | worst ratio | routes with <=2 taxis |
|---|---|---|---|---|
| 1 | 100 | 17.99 | 30.61 | 0 |
| 3 | 100 | 15.26 | 30.61 | 0 |
| 5 | 100 | 8.19 | 21.40 | 0 |
| 10 | 100 | 3.17 | 7.41 | 0 |
| 20 | 100 | 1.62 | 10.22 | 0 |

Least-diverse corridor at each length:

- `>=1 km`: 8725 trips but only **285 taxi(s)** over 5.10 km — 30.6 trips per vehicle.
- `>=3 km`: 8725 trips but only **285 taxi(s)** over 5.10 km — 30.6 trips per vehicle.
- `>=5 km`: 4408 trips but only **206 taxi(s)** over 5.82 km — 21.4 trips per vehicle.
- `>=10 km`: 652 trips but only **88 taxi(s)** over 11.27 km — 7.4 trips per vehicle.
- `>=20 km`: 92 trips but only **9 taxi(s)** over 22.16 km — 10.2 trips per vehicle.

A ratio near 1.0 means almost every trip came from a different vehicle: genuinely public. A high ratio marks a route that the trip-support deliverable would rank as popular and a passenger would not recognise as one.

## Interpretation (derived from the tables above)
- Strongest agreement: **A->D** at 0.97 -- 97% of clustering's routes have a Jaccard>=0.5 partner among suffix-array's. Two methods with different failure modes converging on the same corridors is the strongest evidence available that those corridors are real.
- Weakest agreement: **D->C** at 0.45. Low overlap is not in itself a defect -- the methods optimise different things -- but it marks where the answers depend on the lens.
- A (clustering): 77 distinct routes, top support 58263, longest 10.18 km.
- C (transition-graph): 98 distinct routes, top support 13762, longest 15.66 km.
- D (suffix-array): 98 distinct routes, top support 8948, longest 16.37 km.
- Method(s) B are absent BY DESIGN at scale=full: they enumerate O(n^2) windows and are gated to sample scale. D reproduces B's maximal-frequent output exactly from a single pass, so nothing is lost by their absence here.
