# M10 Method C: Transition Graph (full)
_generated: 2026-08-04T06:53:17+00:00_

trips: 1,614,508 | nodes: 4,982 | edges: 17,438 | frequent edges: 12,212 | heavy paths validated: 1,545

## Popular routes (heavy paths validated vs trips) per length config
| min_len_km | #validated(>=L) | top_support | longest_km |
|---|---|---|---|
| 1 | 100 | 68,133 | 5.86 |
| 3 | 100 | 13,762 | 15.66 |
| 5 | 100 | 9,186 | 15.72 |
| 10 | 44 | 2,831 | 15.72 |
| 20 | 0 | 0 | 0.00 |
| 40 | 0 | 0 | 0.00 |

## Top activity zones (PageRank, dangling mass redistributed)

`taxis` is the number of DISTINCT VEHICLES seen in the cell. `trips/taxi` separates a public hotspot from a depot: a cell with heavy traffic from few vehicles is a rank or a garage, not a place the city is busy.

| rank | cell | lat | lon | pagerank | in_traffic | taxis | trips/taxi |
|---|---|---|---|---|---|---|---|
| 1 | 89392200373ffff | 41.23851 | -8.66955 | 0.001700 | 72,046 | 438 | 164.5 |
| 2 | 8939220037bffff | 41.23534 | -8.67069 | 0.001177 | 25,972 | 437 | 59.4 |
| 3 | 8939220183bffff | 41.20335 | -8.65119 | 0.000978 | 76,668 | 438 | 175.0 |
| 4 | 8939220e9b7ffff | 41.20774 | -8.46354 | 0.000875 | 42 | 29 | 1.4 |
| 5 | 89392200363ffff | 41.23609 | -8.66654 | 0.000845 | 68,696 | 438 | 156.8 |
| 6 | 89392202217ffff | 41.29794 | -8.73354 | 0.000820 | 15 | 12 | 1.2 |
| 7 | 893922015a3ffff | 41.24060 | -8.72311 | 0.000797 | 236 | 135 | 1.7 |
| 8 | 8939220db2fffff | 41.06641 | -8.47606 | 0.000775 | 60 | 34 | 1.8 |
| 9 | 89392200e87ffff | 41.25812 | -8.64307 | 0.000767 | 755 | 272 | 2.8 |
| 10 | 89392202207ffff | 41.29552 | -8.73052 | 0.000764 | 12 | 11 | 1.1 |

## HyperLogLog vs exact: distinct taxis per cell

**Measured verdict: the sketch does NOT earn its place here, and the original justification for it was wrong.**

The argument for HLL was '~85M (cell, taxi) pairs at full scale'. That is the INPUT size, and it is the wrong quantity. What decides whether a distinct-count sketch pays is the CARDINALITY PER GROUP -- and this dataset has only 442 taxis, so no cell can ever exceed 442 distinct values. An exact set of at most 442 ids is trivial; the sketch's fixed register array is pure overhead at every scale, which is why HLL is slower here even on the full 1.71M dataset.

The table is kept because a negative result measured is worth more than a positive one assumed. HLL would pay if groups were unbounded -- distinct PASSENGERS per cell, or distinct trips per cell over years -- and that is the shape to look for before reaching for one.

| metric | value |
|---|---|
| cells measured | 4,982 |
| HLL time | 12.0s |
| exact `countDistinct` time | 1.6s |
| HLL time / exact time | 7.26x |
| mean relative error | 1.263% |
| worst absolute error | 20 taxis |

There is no crossover to find: group cardinality is capped by the fleet size (442), so the exact count stays cheap at every scale this dataset can reach. Reporting the sketch as a win would be exactly the kind of unearned claim this project has spent its time removing.
