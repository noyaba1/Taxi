# M9 Method A: Clustering Route Discovery (full)
_generated: 2026-08-04T06:32:32+00:00_

trips clustered: 49,929 of 1,607,677 (cap CLUSTERING_MAX_TRIPS=50,000) | LSH tables=5 jdist<=0.3 | edges: 423,629 | clusters: 3,363

Each cluster reports the longest cell run shared by >=60% of its
members -- a SUB-route, not a whole trip. `support` is then measured
against ALL trips by containment, so it is directly comparable with
Methods B, C and D.

> **Caveat:** clustering ran on a 3.1% sample of trips (49,929/1,607,677); `cluster_size` is therefore a sample statistic. `support` is not -- it is measured on all 1,607,677 trips. Whether the cap loses corridors is measured in `experiment_cluster_cap_*.md`.

| min_len_km | #routes(>=L) | top_support | longest_km |
|---|---|---|---|
| 1 | 100 | 81,039 | 10.18 |
| 3 | 100 | 58,263 | 10.18 |
| 5 | 100 | 58,263 | 11.63 |
| 10 | 100 | 19,490 | 23.63 |
| 20 | 1 | 16 | 23.63 |
| 40 | 0 | 0 | 0.00 |
