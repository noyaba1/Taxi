"""
Unit tests for the PURE (non-Spark) functions -- the parts where bugs actually
hid during development: window enumeration, gap splitting, the suffix-array LCP
walk, multi-pattern matching, and star clustering.

    python -m pytest tests/ -q
"""
import h3
import pytest

from src import cells as cells_mod
from src import config
from src.ahocorasick import Automaton
from src.feature_engineering import _haversine_km
from src.route_mining_clustering import longest_shared_run, star_cluster
from src.route_mining_exact import DELIM, MAX_L_CAP, MIN_L, _subroutes
from src.route_mining_maximal import min_support_for
from src.route_mining_suffix_array import _lcp_len, lcp_intervals, mine_bucket
from src.spatial_encoding import _compact


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
    assert _haversine_km(-8.6, 41.0, -8.6, 42.0) == pytest.approx(111.2, abs=1.0)


def test_haversine_symmetric():
    a = _haversine_km(-8.6, 41.1, -8.62, 41.16)
    b = _haversine_km(-8.62, 41.16, -8.6, 41.1)
    assert a == pytest.approx(b, abs=1e-9)


# ---------------- gap splitting (the corrupt-trajectory guard) ----------------
def _line(lat1=41.15, lat2=41.20):
    a = h3.geo_to_h3(lat1, -8.61, config.H3_RESOLUTION)
    b = h3.geo_to_h3(lat2, -8.61, config.H3_RESOLUTION)
    return h3.h3_line(a, b)


def test_hop_limit_is_above_real_driving_and_below_a_teleport():
    limit = config.max_cell_hop_km()
    # a vehicle at the retention speed limit covers this much per GPS sample
    travel = config.MAX_SPEED_KMH * config.GPS_INTERVAL_SEC / 3600.0
    assert travel < limit, "limit must admit the fastest trip we choose to keep"
    assert limit < 2.0, "limit must still reject a real GPS jump"


def test_split_at_gaps_passes_clean_path_through():
    cells = _line()
    assert cells_mod.split_at_gaps(cells) == [cells]


def test_split_at_gaps_breaks_on_a_teleport():
    near = _line(41.15, 41.17)
    far = _line(41.28, 41.30)          # ~13 km away: unreachable in one hop
    segs = cells_mod.split_at_gaps(near + far)
    assert len(segs) == 2
    assert segs[0] == near and segs[1] == far


def test_split_at_gaps_drops_stranded_single_cells():
    lone = h3.geo_to_h3(41.05, -8.75, config.H3_RESOLUTION)
    segs = cells_mod.split_at_gaps(_line() + [lone])
    assert all(len(s) >= 2 for s in segs)


def test_path_length_matches_cumulative():
    cells = _line()
    assert cells_mod.path_length_km(cells) == pytest.approx(
        cells_mod.cumulative_km(cells)[-1], abs=1e-9)


# ---------------- _subroutes (window enumeration) ----------------
def test_subroutes_short_input_returns_empty():
    # two adjacent res-9 cells are ~0.3 km apart -> below MIN_L (1 km) -> no window
    a = h3.geo_to_h3(41.15, -8.61, 9)
    nb = list(h3.k_ring(a, 1) - {a})[0]
    assert _subroutes([a, nb]) == []


def test_subroutes_properties():
    out = _subroutes(_line())
    keys = [k for k, _l, _n, _t in out]
    assert len(keys) == len(set(keys))            # no duplicate windows per trip
    for key, length, ncells, truncated in out:
        assert MIN_L <= length <= MAX_L_CAP
        assert ncells == len(key.split(DELIM)) >= 2
        assert isinstance(truncated, bool)


def test_subroutes_never_spans_a_gap():
    """THE regression test for the audit's most damaging finding.

    Two distant fragments joined into one sequence must not produce a window
    that bridges them: such a window reports the ~13 km jump as route length and
    lands in the >=10 km top-100 with only a handful of cells.
    """
    near, far = _line(41.15, 41.17), _line(41.28, 41.30)
    limit = config.max_cell_hop_km()
    for key, _length, _ncells, _t in _subroutes(near + far):
        cells = key.split(DELIM)
        assert max(cells_mod.hops_km(cells)) <= limit
        # and the window lies wholly inside one fragment
        assert set(cells) <= set(near) or set(cells) <= set(far)


def test_subroutes_km_per_cell_is_physical():
    """Every emitted window must average a plausible distance per cell step."""
    limit = config.max_cell_hop_km()
    for _key, length, ncells, _t in _subroutes(_line(41.15, 41.35)):
        assert length / (ncells - 1) <= limit


# ---------------- suffix array: LCP intervals ----------------
def _intervals(strings):
    s = sorted(strings)
    lcp = [0] + [_lcp_len(s[i - 1], s[i], 99) for i in range(1, len(s))]
    return {s[l][:h]: r - l + 1 for h, l, r in lcp_intervals(lcp, len(s))}


def _brute(strings):
    """Right-maximal repeats under GENERALISED suffix semantics: the end of a
    string is its own distinct right context (a route ending at the trip's last
    cell genuinely cannot be extended)."""
    s = sorted(strings)
    res = {}
    for st in s:
        for h in range(1, len(st) + 1):
            p = st[:h]
            occ = [(i, x) for i, x in enumerate(s) if x[:h] == p]
            nxt = {(x[h] if len(x) > h else ("$", i)) for i, x in occ}
            if len(nxt) > 1:
                res[p] = len(occ)
    return res


@pytest.mark.parametrize("case", [
    ["ab", "abc", "abd", "b"],
    ["aa", "aa"],
    ["aab", "aac", "aba"],
    ["xyz"],
    ["ab", "ab", "abc", "abcd", "b", "bc"],
    list("abcdefg"),
    ["aaaa", "aaab", "aaba", "abaa", "baaa"],
    ["abcab", "bcab", "cab", "ab", "b"],
])
def test_lcp_intervals_match_brute_force(case):
    assert _intervals(case) == _brute(case)


def test_lcp_intervals_empty_and_single():
    assert list(lcp_intervals([], 0)) == []
    assert list(lcp_intervals([0], 1)) == []


def test_mine_bucket_counts_trips_not_occurrences():
    """A run repeated inside ONE trip must count once."""
    suffix = list(_line())
    rows = [("tripA", None, suffix), ("tripA", None, suffix), ("tripB", None, suffix)]
    got = mine_bucket(rows, min_sup=2, min_len_km=0.0, max_len_km=999.0)
    assert got, "expected at least one frequent run"
    assert all(sup <= 2 for _k, sup, _n, _l, _br, _bl in got), \
        "support must count distinct trips (2), not suffix occurrences (3)"


def test_mine_bucket_prunes_below_min_support():
    rows = [("t1", None, list(_line()))]
    assert mine_bucket(rows, min_sup=5, min_len_km=0.0, max_len_km=999.0) == []


# ---------------- Aho-Corasick ----------------
def test_automaton_matches_overlapping_patterns():
    a = Automaton([("a", "b"), ("b", "c"), ("a", "b", "c")])
    assert a.matches(("x", "a", "b", "c", "x")) == {0, 1, 2}


def test_automaton_empty_and_absent():
    a = Automaton([("a", "b")])
    assert a.matches(()) == set()
    assert a.matches(("z", "z")) == set()


def test_automaton_matches_brute_force_randomised():
    import random

    random.seed(11)
    alpha = [f"c{i}" for i in range(5)]
    for _ in range(400):
        pats = [tuple(random.choices(alpha, k=random.randint(1, 4)))
                for _ in range(random.randint(1, 5))]
        text = tuple(random.choices(alpha, k=random.randint(0, 15)))
        exp = {i for i, p in enumerate(pats)
               if any(text[j:j + len(p)] == p for j in range(len(text) - len(p) + 1))}
        assert Automaton(pats).matches(text) == exp


# ---------------- longest shared run (Method A sub-route extraction) ----------
def test_longest_shared_run_finds_the_common_corridor():
    common = list("cdefg")
    seqs = [list("ab") + common, common + list("hi"), list("z") + common + list("y")]
    run, n = longest_shared_run(seqs, min_members=3)
    assert list(run) == common and n == 3


def test_longest_shared_run_respects_the_member_threshold():
    seqs = [list("abcd"), list("abcd"), list("xyzw")]
    run, n = longest_shared_run(seqs, min_members=2)
    assert list(run) == list("abcd") and n == 2
    strict = longest_shared_run(seqs, min_members=3)
    assert strict is None or len(strict[0]) < 4


def test_longest_shared_run_handles_empty():
    assert longest_shared_run([], 2) is None
    assert longest_shared_run([[], []], 2) is None


# ---------------- min-support arithmetic ----------------
def test_min_support_floors_at_two():
    assert min_support_for(0.0001, 100) == 2
    assert min_support_for(50.0, 100) == 50
    assert min_support_for(0.5, 1_710_670) == 8554


# ---------------- star_cluster (anti-chaining property) ----------------
def test_star_cluster_star():
    clusters = star_cluster([(1, 2), (1, 3), (1, 4)], min_size=2)
    assert len(clusters) == 1
    seed, members = next(iter(clusters.values()))
    assert seed == 1 and set(members) == {1, 2, 3, 4}


def test_star_cluster_does_not_chain():
    # a chain 1-2-3-4: star clustering must NOT merge all four (that was the CC bug)
    clusters = star_cluster([(1, 2), (2, 3), (3, 4)], min_size=3)
    assert all(len(members) <= 3 for _seed, members in clusters.values())
    all_members = [m for _s, mem in clusters.values() for m in mem]
    assert len(all_members) < 4


def test_star_cluster_respects_min_size():
    assert star_cluster([(1, 2)], min_size=3) == {}


# ---------------- maximal-at-floor semantics (Method D calibration) ----------
def test_mine_bucket_reports_extension_supports():
    """
    `best_right`/`best_left` are what make the X grid a filter instead of a
    re-mine. Two trips sharing a long run, one continuing further, must show a
    right-extension support of 1 on the shared prefix.
    """
    seg = list(_line(41.15, 41.22))
    short, long_ = seg[:6], seg[:9]
    rows = [("t1", None, short), ("t2", None, long_)]
    got = mine_bucket(rows, min_sup=2, min_len_km=0.0, max_len_km=999.0)
    assert got, "the shared prefix should be reported"
    for _key, sup, _n, _l, best_right, best_left in got:
        # support counts distinct trips; extensions can never exceed it
        assert best_right <= sup and best_left <= sup
        # no trip precedes these runs (both start at position 0)
        assert best_left == 0


def test_maximal_filter_semantics_are_monotone():
    """
    A route is maximal at a floor iff it clears the floor and no extension does.
    Raising the floor can only ever turn a non-maximal route maximal (its
    extension drops out first) -- never the reverse for a route that stays
    frequent. This encodes the invariant the calibration relies on.
    """
    support, best_ext = 50, 30
    maximal = lambda ms: support >= ms and best_ext < ms
    assert not maximal(20)      # extension still frequent -> not maximal
    assert maximal(40)          # extension dropped out -> maximal
    assert not maximal(60)      # route itself no longer frequent
