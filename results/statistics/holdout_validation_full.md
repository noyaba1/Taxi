# Held-out validation (full)
_generated: 2026-08-04T03:45:37+00:00_

Corridors mined from the TRAINING data, tested against **318 trips the pipeline has never seen** (`test.csv` — the original challenge's held-out split), encoded with the same H3 grid and split at gaps the same way (300 gap-free segments).

Every other check in this project is internal — verifiers recount support against the same table the mining used. This is the only one that can tell the difference between *a real corridor* and *a memorised training path*.

Corridors considered: length >= 1 km.

| method | corridors | held-out coverage | null model | lift |
|---|---|---|---|---|
| A | 277 | 39.3% | 5.7% | **6.9x** |
| C | 239 | 30.7% | 6.7% | **4.6x** |
| D | 484 | 28.7% | 5.0% | **5.7x** |
- Method B: no corridors at >=1 km for scale=full (stage not run, or none found).

**Null model.** Coverage alone proves little: corridors sit on busy roads and so do most trips. The null is a set of random walks over the held-out city's OWN observed adjacency, matched to the real corridors' length distribution — i.e. plausible routes that simply were not mined as popular. Uniform-random cells would be disconnected, unmatchable, and would inflate the lift into meaninglessness.

**Reading it.** Lift > 1 means mined corridors are traversed by unseen trips more often than comparable un-mined paths — the corridors generalise. Lift near 1 would mean the miner found busy geography in general rather than specific popular routes.
