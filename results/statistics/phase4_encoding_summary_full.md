# Phase 4 H3 Encoding Summary (res 9, full)
_generated: 2026-08-04T06:19:48+00:00_

trips in: 1,663,886 | encoded: 1,614,508 | dropped as anomalous: 49,378

## Why trips were dropped before encoding
| flag | trips | share of input |
|---|---|---|
| is_teleport | 37,220 | 2.24% |
| is_idle | 2,742 | 0.16% |
| is_too_fast | 2,178 | 0.13% |
| is_too_short | 10,161 | 0.61% |
| is_offgrid | 946 | 0.06% |
| is_anomalous | 49,378 | 2.97% |

A trajectory containing a GPS teleport yields sub-routes that no
vehicle drove: sub-route length is measured between consecutive cell
centres, so a single jump reads as a 10-45 km 'route'. Excluding these
trips is what keeps the >=10/20/40 km configurations meaningful.

## Encoding metrics
| metric | value |
|---|---|
| avg_raw | 48.9 |
| avg_compact | 18.7 |
| avg_compression | 3.15 |
| avg_encoded_km | 6.44 |
| avg_gps_km | 5.26 |
| worst_hop_km | 0.368 |

Residual check: 0 encoded trips still contain a hop larger
than the grid limit (1.18 km). Windows spanning such a hop are
rejected by the miners' hop guard, so they cannot become sub-routes.
