"""
cells.py
========
Geometry over sequences of H3 cells. Small, pure, and unit-tested -- three
modules used to carry their own copy of this arithmetic.

THE GAP RULE
------------
`h3_seq_compact` is a trip's trajectory with consecutive duplicate cells
collapsed. Consecutive cells are therefore normally NEIGHBOURS: at res 9 their
centres are ~0.3 km apart, and even 120 km/h over the 15 s sampling interval only
covers ~0.5 km. A larger jump is not travel -- it is a hole in the GPS trace
(dropped signal, tunnel, tracker reset).

That matters because sub-route length is measured between cell centres. A window
spanning a 40 km jump reports itself as a 40 km "route", which is exactly the
length band the assignment grades. So every miner splits trajectories at gaps
first and mines the resulting continuous segments.

TWO KINDS OF DISCONTINUITY, AND THEY NEED OPPOSITE TREATMENT
------------------------------------------------------------
Not every non-adjacent pair is a gap. GPS is sampled every 15 s, so above
~32 km/h a vehicle crosses a res-9 cell *between* two fixes and that cell is
never observed. The chain then reads A, C with B missing -- a hole punched by
the sampling clock, not by lost signal.

MEASURED on the 5k sample: only 95.1% of consecutive pairs are actually
adjacent. 4.8% are two cells apart and 0.04% three. That sounds minor until you
compound it over a window: a route is only matched when EVERY hop is intact, so
the fraction of clean windows falls as 0.951^(L-1) -- 86% at 1 km (L=4), 23% at
10 km (L=30), 5.5% at 20 km (L=59). And two taxis on the same road only match
each other when their holes coincide, so support decays faster still. That is
an ENCODING artifact masquerading as a data property, and it is what hollowed
out the long length bands.

The repair has to happen in GEOGRAPHIC space, not grid space. The tempting fix
is to patch the cell chain afterwards with `h3.h3_line(A, C)`, but between two
cells two steps apart there are often two equally valid grid paths and h3 breaks
the tie by its own rule: reconstructing each skipped cell of a straight res-9
line that way picks the WRONG cell 5 times in 16. It restores contiguity while
still leaving two out-of-phase taxis on different chains, which is the one thing
the repair exists to prevent.

`densify_points` works on the GPS polyline instead. Between two fixes 15 s apart
a taxi travels at most a few hundred metres, which on a road is effectively a
straight line, so the segment is resampled finely enough that no cell along it
can be skipped. The cells a segment crosses then depend only on where the road
goes -- not on where the sampling clock happened to tick -- so two taxis driving
the same street encode identically by construction.

The bound is the same one `split_at_gaps` uses. A gap SHORTER than
`config.max_cell_hop_km()` cannot be lost signal (no retained vehicle can move
that far between samples without being dropped as a teleport), so it is
undersampling and gets filled. A gap LONGER than it is a real hole and is left
alone -- there we do not know which way the vehicle went, and inventing a path
is the exact fabrication the gap rule exists to prevent.

Same threshold, opposite treatment. Between them they cover every
discontinuity, which leaves a clean post-encoding invariant: consecutive cells
are either adjacent, or separated by a genuine gap the miners split at. Nothing
in between, and no jump is ever admitted as road.
"""
from __future__ import annotations

import math

import h3

from src import config


def hops_km(cells) -> list[float]:
    """Centre-to-centre distance for each consecutive pair. len == len(cells)-1."""
    if not cells or len(cells) < 2:
        return []
    centres = [h3.h3_to_geo(c) for c in cells]
    return [h3.point_dist(a, b, unit="km") for a, b in zip(centres[:-1], centres[1:])]


def path_length_km(cells) -> float:
    """Total ground length of a cell path."""
    return float(sum(hops_km(cells)))


def cumulative_km(cells) -> list[float]:
    """Prefix sums of the hops, so any window length is an O(1) subtraction."""
    cum = [0.0]
    for h in hops_km(cells):
        cum.append(cum[-1] + h)
    return cum


def revisits_ok(cells, max_revisits: int | None = None) -> bool:
    """
    True if no cell is entered more than `max_revisits` times.

    The third guard, and the one the other two cannot cover. `split_at_gaps`
    rejects windows built across a hole; `densify_points` fills holes the clock
    punched. Neither says anything about a window that never leaves: length is
    summed hop by hop, so a vehicle oscillating across one cell boundary
    accumulates kilometres it never travelled, and `_compact` removes only
    CONSECUTIVE duplicates so A>B>A>B passes through intact.

    See `config.MAX_CELL_REVISITS` for the measurement that fixes the limit at 2.
    """
    if not cells:
        return True
    limit = config.MAX_CELL_REVISITS if max_revisits is None else max_revisits
    counts: dict[str, int] = {}
    for c in cells:
        n = counts[c] = counts.get(c, 0) + 1
        if n > limit:
            return False
    return True


def cell_step_km(resolution: int | None = None) -> float:
    """Centre-to-centre distance between adjacent hexagons: sqrt(3) * edge."""
    res = config.H3_RESOLUTION if resolution is None else resolution
    return (3.0 ** 0.5) * h3.edge_length(res, unit="km")


def densify_points(pts, resolution: int | None = None,
                   max_gap_km: float | None = None) -> list:
    """
    Resample a GPS polyline finely enough that no cell along it can be skipped.

    `pts` are `[lon, lat]` pairs (the POLYLINE order -- longitude first). For
    each consecutive pair:

      <= max_gap_km   the vehicle demonstrably drove it, and over a few hundred
                      metres a road is effectively straight, so intermediate
                      points are inserted by linear interpolation. Spacing is
                      capped at half the hexagon INRADIUS, which is the largest
                      step that cannot step over a cell.
      >  max_gap_km   a real hole in the trace: no points are inserted, the
                      discontinuity survives into the cell sequence, and
                      `split_at_gaps` cuts there.

    Interpolating in geographic space is what makes the result sampling-phase
    independent -- the cells come from where the road goes, not from where the
    clock ticked. See the module docstring for why patching the cell chain with
    `h3.h3_line` instead does not achieve this.

    Returns a new list; the input is not modified.
    """
    # `pts` arrives as a numpy array inside the encoder's pandas_udf, where
    # `not pts` raises. Test length, never truthiness.
    if pts is None or len(pts) < 2:
        return [] if pts is None else list(pts)
    res = config.H3_RESOLUTION if resolution is None else resolution
    limit = config.max_cell_hop_km(res) if max_gap_km is None else max_gap_km
    # An eighth of the centre-to-centre spacing. Any step below the inradius
    # already guarantees contiguity (measured: 100% of trips at every divisor
    # tried), so the divisor only buys FIDELITY -- catching cells the route
    # merely clips a corner of. Measured against a /64 reference on real trips:
    # /2 = 0.976, /4 = 0.986, /8 = 0.992, /16 = 0.997, at 0.05 / 0.08 / 0.13 /
    # 0.23 s per 600 trips. /8 is where the curve flattens against its cost.
    step_km = max(cell_step_km(res) / 8.0, 1e-4)

    out = [pts[0]]
    for a, b in zip(pts[:-1], pts[1:]):
        d = h3.point_dist((a[1], a[0]), (b[1], b[0]), unit="km")
        if d <= limit and d > step_km:
            # ceil, not int: a 1.9-step gap must be split into 2 -- truncating
            # to 1 inserts nothing and leaves the gap exactly as it was.
            n = int(math.ceil(d / step_km))
            for i in range(1, n):
                f = i / n
                out.append([a[0] + (b[0] - a[0]) * f, a[1] + (b[1] - a[1]) * f])
        out.append(b)
    return out


def split_at_gaps(cells, max_hop_km: float | None = None) -> list[list[str]]:
    """
    Break a cell sequence into maximal CONTINUOUS segments.

    Returns segments of length >= 2 only: a single stranded cell cannot form a
    route. An uncorrupted trajectory returns exactly one segment, so this is
    free in the common case.
    """
    if not cells or len(cells) < 2:
        return []
    limit = config.max_cell_hop_km() if max_hop_km is None else max_hop_km
    segments, current = [], [cells[0]]
    for prev, cur, hop in zip(cells[:-1], cells[1:], hops_km(cells)):
        if hop > limit:
            if len(current) >= 2:
                segments.append(current)
            current = [cur]
        else:
            current.append(cur)
    if len(current) >= 2:
        segments.append(current)
    return segments
