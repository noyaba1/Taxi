# M7 Approximate vs Exact Sub-route Mining (sample)
_generated: 2026-08-04T11:04:40+00:00_

config: frequent_strings lg=16 (~49,152 counters) x6 ; count_min 5x131,072 ; seed=lib-default(deterministic) ; top_k=100

## Runtime, memory & shuffle
- approx sketch time    : 4.6s
- approx memory (real)  : 82.2 MB  (fixed by capacity)
- exact shuffled records: 1,162,961  (window rows into groupBy)
- approx merged bundles : 13  (1 sketch bundle per partition)
- exact groupBy time    : 1.8s
- exact memory (est)    : 253.7 MB  (578,880 keys x ~390B)
- memory ratio          : 3.1x smaller

## Accuracy vs exact top-100
| min_len | emitted_cand | overlap@100 | precision@100 | recall@100 | abs_err(MAE) | rel_err(MRE) | CMS_MAE |
|---|---|---|---|---|---|---|---|
| 1 | 42,822 | 99 | 0.99 | 0.99 | 0.0 | 0.000 | 2.7 |
| 3 | 45,755 | 94 | 0.94 | 0.94 | 0.0 | 0.000 | 2.3 |
| 5 | 13,007 | 97 | 0.97 | 0.97 | 0.0 | 0.000 | 2.5 |
| 10 | 5,845 | 97 | 0.97 | 0.97 | 0.0 | 0.000 | 2.7 |
| 20 | 32,530 | 78 | 0.78 | 0.78 | 0.0 | 0.000 | 2.7 |
| 40 | 564 | 0 | 0.00 | 0.00 | 0.0 | 0.000 | 0.0 |
