# Temporal analysis: does 'popular' depend on the hour? (full)
_generated: 2026-08-04T09:05:40+00:00_

trips: 1,614,508 | corridors compared at >=3 km | support floor 0.05% of each bucket | match = cell-set Jaccard >= 0.5

Every headline number in this project aggregates 2013-2014 whole. This asks whether that average describes any actual hour.

| bucket | trips | floor | corridors | overlap vs all-time | wall_s |
|---|---|---|---|---|---|
| night | 296,575 | 148 | 100 | 0.76 | 71.4 |
| morning_peak | 271,960 | 136 | 100 | 0.77 | 67.0 |
| midday | 506,599 | 253 | 100 | 0.86 | 89.9 |
| evening_peak | 314,049 | 157 | 100 | 0.89 | 67.2 |
| evening | 225,325 | 113 | 100 | 0.82 | 53.3 |

## Verdict

Mean overlap 0.82. The corridors are **structural**: the same stretches dominate at 03:00 and at 08:00, so it is the road network rather than time-varying demand that decides where taxis go. The all-time top-100 is therefore a fair summary and not an artefact of averaging — which is worth having measured rather than assumed.

Most distinctive hour: **night** (overlap 0.76) — the period whose corridors the all-time list represents least well. Most typical: **evening_peak** (0.89).
