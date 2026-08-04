# Strong scaling: fixed data, varying cluster size
_generated: 2026-08-04T05:14:53+00:00_

Workload held fixed at scale `full`. Baseline is the smallest cluster measured (2 workers), because a single-core time for this dataset was never measured and extrapolating one would invent the very number the study is about.

| workers | VMs | wall (s) | wall | speedup | efficiency | implied serial % | est. $ |
|---|---|---|---|---|---|---|---|
| 2 | 3 | 4,085 | 68m04s | 1.00x | 1.00 | — | $0.80 |
| 5 | 6 | 3,947 | 65m46s | 1.03x | 0.41 | 94.4% | $1.54 |
| 10 | 11 | 3,193 | 53m13s | 1.28x | 0.26 | 72.7% | $2.29 |
| 16 | 17 | 3,017 | 50m16s | 1.35x | 0.17 | 70.1% | $3.34 |

`efficiency` = speedup / (workers relative to baseline). 1.00 is perfect scaling; the cost column is an on-demand estimate (VM rate + Dataproc surcharge), not the billed amount.

## What the numbers say

- **Fastest measured:** 16 workers at **1.35x** the baseline.
- **Efficiency falls below 0.50 at 5 workers** (0.41) — past this point each added machine returns less than half a machine's worth of speed.
- **Implied serial fraction ≈ 79.1%** (mean over 3 comparison(s)). Amdahl caps this pipeline at **1.3x** however many machines it is given — the floor is cluster creation plus ~13 sequential job submissions, which no amount of hardware parallelises.
- **Cheapest was 2 workers ($0.80); fastest was 16 workers ($3.34).** The $2.54 difference is what the extra speed cost, which is the trade a budgeted project has to make explicitly.

## Per-stage wall time (s)

| stage | 2w | 5w | 10w | 16w | speedup |
|---|---|---|---|---|---|
| holdout_validation | 11 | 11 | 11 | 11 | 1.02x |
| m10_graph | 247 | 255 | 229 | 228 | 1.08x |
| m11_anomalies | 52 | 43 | 38 | 37 | 1.38x |
| m12_suffix_array | 618 | 348 | 285 | 272 | 2.27x |
| m18_temporal | 769 | 657 | 494 | 450 | 1.71x |
| m1_summary | 34 | 40 | 32 | 34 | 1.00x |
| m3_encoding | 316 | 420 | 179 | 184 | 1.71x |
| m7_approx | 1,394 | 1,677 | 1,350 | 1,342 | 1.04x |
| m9_clustering | 364 | 311 | 403 | 283 | 1.29x |
| p1_clean | 153 | 99 | 90 | 94 | 1.63x |
| p2_features | 126 | 86 | 83 | 80 | 1.57x |

A stage whose time barely moves is a fixed cost (driver-side work, job submission latency); one that falls with the cluster is genuinely distributed. The mix of the two is what the serial fraction above is made of.

