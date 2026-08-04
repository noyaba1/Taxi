# Rider mix per length configuration (full)
_generated: 2026-08-04T09:31:56+00:00_

`CALL_TYPE` records how a trip BEGAN: **A** dispatched from a central
office, **B** hailed at a taxi stand, **C** flagged down in the street.
Trips are attributed to a band once, however many of its corridors they
traverse, so these are shares of distinct trips.

Corpus baseline over 1,614,508 trips: **A** 21.9% · **B** 48.5% · **C** 29.5%

| min_len | traversing trips | A dispatch | B taxi stand | C street hail |
|---|---|---|---|---|
| ≥1 km | 534,937 | 20.1% | 51.6% | 28.3% |
| ≥3 km | 338,462 | 19.5% | 48.6% | 31.9% |
| ≥5 km | 178,399 | 20.3% | 47.9% | 31.8% |
| ≥10 km | 46,592 | 20.4% | 47.1% | 32.5% |
| ≥20 km | 6,550 | 7.6% | 49.9% | 42.5% |

## What the numbers say

- **A (dispatch)** falls from 20.1% at ≥1 km to 7.6% at ≥20 km (-12.5 points).
- **B (taxi stand)** falls from 51.6% at ≥1 km to 49.9% at ≥20 km (-1.7 points).
- **C (street hail)** rises from 28.3% at ≥1 km to 42.5% at ≥20 km (+14.2 points).

Read against the corpus baseline above, not against each other: a
share only means something as a deviation from how trips begin in
general.
