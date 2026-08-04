# M7 Approximate vs Exact Sub-route Mining (full)
_generated: 2026-08-04T09:28:10+00:00_

config: frequent_strings lg=16 (~49,152 counters) x6 ; count_min 5x131,072 ; seed=lib-default(deterministic) ; top_k=100

## Runtime, memory & shuffle
- approx sketch time    : 1290.7s
- approx memory (real)  : 172.8 MB  (fixed by capacity)
- exact shuffled records: 359,752,506  (window rows into groupBy)
- approx merged bundles : 4  (1 sketch bundle per partition)

_Run with `--approx-only`: the exact baseline was skipped, so no accuracy comparison is reported here._
