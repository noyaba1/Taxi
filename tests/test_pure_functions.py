"""
Unit tests for the PURE (non-Spark) functions -- the parts where bugs actually
hid during development (window enumeration, star clustering). Run:

    python -m pytest tests/ -q
"""
import h3
import pytest

from src.spatial_encoding import _compact
from src.feature_engineering import _haversine_km
from src.route_mining_exact import _subroutes, MIN_L, MAX_L_CAP, DELIM
from src.route_mining_clustering import star_cluster


# ---------------- _compact (consecutive-duplicate collapse) ----------------
def test_compact_collapses_consecutive():
    assert _compact(["a", "a", "b", "b", "b", "c"]) == ["a", "b", "c"]


def test_compact_keeps_revisits():
    # a cell revisited after leaving is NOT a consecutive dup -> kept
    assert _compact(["a", "b", "a"]) == ["a", "b", "a"]


def test_compact_edge_cases():
    assert _compact([]) == []
    assert _compact(["x"]) == ["x"]


# ---------------- _haversine_km ----------------
def test_haversine_zero():
    assert _haversine_km(-8.6, 41.15, -8.6, 41.15) == pytest.approx(0.0, abs=1e-9)


def test_haversine_one_degree_lat():
    # 1 degree of latitude is ~111.2 km anywhere
    d = _haversine_km(-8.6, 41.0, -8.6, 42.0)
    assert d == pytest.approx(111.2, abs=1.0)


def test_haversine_symmetric():
    a = _haversine_km(-8.6, 41.1, -8.62, 41.16)
    b = _haversine_km(-8.62, 41.16, -8.6, 41.1)
    assert a == pytest.approx(b, abs=1e-9)


# ---------------- _subroutes (window enumeration) ----------------
def _line_cells(km_apart_points):
    """Build a real H3 cell path along an H3 line between two points."""
    a = h3.geo_to_h3(41.15, -8.61, 9)
    b = h3.geo_to_h3(41.20, -8.61, 9)      # ~5.5 km north
    return h3.h3_line(a, b)


def test_subroutes_short_input_returns_empty():
    # two adjacent res-9 cells are ~0.17 km apart -> below MIN_L (1 km) -> no window
    a = h3.geo_to_h3(41.15, -8.61, 9)
    nb = list(h3.k_ring(a, 1) - {a})[0]
    assert _subroutes([a, nb]) == []


def test_subroutes_properties():
    cells = _line_cells(None)
    out = _subroutes(cells)
    keys = [k for k, _l, _n in out]
    # 1. no duplicate windows within a trip
    assert len(keys) == len(set(keys))
    # 2. every window respects the length band and n_cells matches its key
    for key, length, ncells in out:
        assert MIN_L <= length <= MAX_L_CAP
        assert ncells == len(key.split(DELIM))
        assert ncells >= 2


# ---------------- star_cluster (anti-chaining property) ----------------
def test_star_cluster_star():
    # node 1 is the hub of a star -> one cluster {1,2,3,4}
    clusters = star_cluster([(1, 2), (1, 3), (1, 4)], min_size=2)
    assert len(clusters) == 1
    seed, members = next(iter(clusters.values()))
    assert seed == 1 and set(members) == {1, 2, 3, 4}


def test_star_cluster_does_not_chain():
    # a chain 1-2-3-4: star clustering must NOT merge all four (that was the CC bug)
    clusters = star_cluster([(1, 2), (2, 3), (3, 4)], min_size=3)
    # the seed (highest degree, deg 2) forms a size-3 star; the 4th node is not
    # transitively chained in.
    assert all(len(members) <= 3 for _seed, members in clusters.values())
    all_members = [m for _s, mem in clusters.values() for m in mem]
    assert len(all_members) < 4    # not everyone ended up in one blob


def test_star_cluster_respects_min_size():
    # a single edge with min_size 3 yields no cluster
    assert star_cluster([(1, 2)], min_size=3) == {}
