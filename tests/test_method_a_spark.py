"""
Method A through its REAL Spark path, on synthetic encoded data.

WHY THIS EXISTS SEPARATELY FROM test_pure_functions
---------------------------------------------------
`longest_shared_run` is covered there, but the defect it was fixed for did not
live in that function -- it lived in how `_cluster_run` CALLED it, inside an
`applyInPandas`. Segments from `split_at_gaps` were flattened into one candidate
list while the member threshold was computed from the trip count, so a trip
broken at GPS gaps entered the list once per stretch and could clear a "60% of
members" bar by itself.

An edit inside a pandas UDF fails at RUNTIME, never at import, and the pure-file
suite cannot see it. So this drives `discover()` end to end and asserts the
invariant the bug violated:

    members_with_run <= cluster_size

which cannot hold if one trip is counted more than once.

Needs a JVM (like test_storage.py). Run just these:
    pytest tests/test_method_a_spark.py -q
"""
import shutil
import tempfile

import h3
import pytest

from src import config

RES = config.H3_RESOLUTION


def _corridor(lat, lon, n_cells):
    """A run of adjacent H3 cells heading east -- i.e. a gap-free trajectory."""
    cells, step = [], 0.0016
    for i in range(n_cells):
        c = h3.geo_to_h3(lat, lon + i * step, RES)
        if not cells or c != cells[-1]:
            cells.append(c)
    return cells


@pytest.fixture(scope="module")
def spark():
    from src.spark_session import get_spark
    s = get_spark("test-method-a")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def discovered(spark):
    """
    Run the real `discover()` against a synthetic encoded parquet.

    The population is deliberately shaped to EXPOSE the double-count, which is
    harder than it sounds: a trip with two stretches in different places counts
    once for each run, and nothing is wrong. The bug only shows when the SAME
    trip contains the SAME run in more than one stretch.

    So `T_GAP` drives the shared corridor, loses signal, and drives it again. The
    hop from the end of the first pass back to the start of the second is far
    above `max_cell_hop_km`, so `split_at_gaps` returns two segments that BOTH
    contain the corridor. Under the old flattening that one trip contributed 2.
    """
    from src import route_mining_clustering as rmc

    tmp = tempfile.mkdtemp()
    enc = f"{tmp}/encoded.parquet"
    shared = _corridor(41.15, -8.62, 40)
    other = _corridor(41.20, -8.58, 30)

    rows = [(f"T{i}", 100 + (i % 5), list(shared), 5.0) for i in range(20)]
    rows += [(f"T{i}", 200 + (i % 5), list(other), 4.0) for i in range(20, 32)]
    rows.append(("T_GAP", 999, list(shared) + list(shared), 9.0))

    spark.createDataFrame(
        rows, "TRIP_ID string, TAXI_ID int, h3_seq_compact array<string>, "
              "encoded_len_km double").write.mode("overwrite").parquet(enc)

    orig = config.dataset_paths
    config.dataset_paths = lambda scale: {**orig(scale), "encoded": enc}
    try:
        yield rmc.discover(spark, "sample")
    finally:
        config.dataset_paths = orig
        shutil.rmtree(tmp, ignore_errors=True)


def test_discover_runs_and_finds_the_planted_corridors(discovered):
    routes, meta = discovered
    assert meta["n_clusters"] >= 1
    assert routes, "no corridor found in data built entirely of shared corridors"


def test_no_trip_is_counted_more_than_once_in_its_cluster(discovered):
    """
    The invariant the segment/trip double-count violated. `members_with_run`
    counts members containing the run; it cannot exceed the number of members.
    """
    routes, _meta = discovered
    for support, cluster_size, members_with_run, _ln, _nc, _sub in routes:
        assert members_with_run <= cluster_size, (
            f"members_with_run={members_with_run} exceeds cluster_size="
            f"{cluster_size}: a trip was counted once per gap-free segment")
        assert support >= 1


def test_reported_corridors_are_gap_free(discovered):
    """
    No reported run may contain a hop above the physical bound -- otherwise the
    gap's width is being reported as route length.
    """
    from src import cells as cells_mod

    routes, _meta = discovered
    limit = config.max_cell_hop_km()
    for _sup, _size, _nwith, _ln, _nc, subroute in routes:
        hops = cells_mod.hops_km(subroute.split(">"))
        assert all(h <= limit for h in hops), (
            f"corridor spans a GPS gap: max hop {max(hops):.2f} km > {limit:.2f}")
