# Phase 2 Feature Summary (full)
_generated: 2026-08-04T03:25:44+00:00_

rows: 1,663,886  |  source: gs://taxi-project-noyabayazi/taxi/processed/trips_features_full.parquet

## Distribution statistics
| feature | mean | stddev | min | p25 | p50 | p75 | p95 | max |
|---|---|---|---|---|---|---|---|---|
| total_distance_km | 5.381 | 6.149 | 0.000 | 2.431 | 3.987 | 6.502 | 14.049 | 1,229.271 |
| straight_line_km | 3.218 | 2.464 | 0.000 | 1.500 | 2.583 | 4.107 | 8.518 | 24.131 |
| duration_sec | 720.575 | 641.652 | 15.000 | 420.000 | 615.000 | 870.000 | 1,470.000 | 58,200.000 |
| avg_speed_kmh | 27.239 | 18.836 | 0.000 | 17.870 | 23.883 | 32.601 | 54.134 | 8,940.156 |
| max_seg_speed_kmh | 86.877 | 287.215 | 0.000 | 50.386 | 64.186 | 95.976 | 184.572 | 147,345.772 |
| sinuosity | 2.448 | 15.561 | 1.000 | 1.277 | 1.444 | 1.727 | 3.267 | 6,790.249 |

## Anomaly flags
(counted on the UNFILTERED feature table; spatial_encoding excludes `is_anomalous` trips before mining)
| flag | count | pct |
|---|---|---|
| is_teleport | 37,220 | 2.24% |
| is_idle | 2,742 | 0.16% |
| is_too_fast | 2,178 | 0.13% |
| is_too_short | 10,161 | 0.61% |
| is_offgrid | 946 | 0.06% |
| is_anomalous | 49,378 | 2.97% |
