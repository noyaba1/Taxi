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
"""
from __future__ import annotations

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
