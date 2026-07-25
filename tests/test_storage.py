"""
Regression tests for src/storage.py -- the layer that decides whether results
survive a cloud run.

WHY THESE NEED SPARK
--------------------
The whole point of `storage` is that a path with a URI scheme goes through
Hadoop's FileSystem instead of `open()`. Exercising only the local branch would
test nothing that matters: the bug this module exists to prevent is results being
written to a node's local disk and destroyed with the cluster.

`file://` is a real Hadoop FileSystem with a URI scheme, so it takes exactly the
same code path as `gs://` -- the JVM calls, the overwrite flag, the stream
handling, the UTF-8 decode -- without needing cloud credentials.

These tests are slower than the pure-function suite (one local SparkSession).
Run just them with:  pytest tests/test_storage.py -q
"""
import tempfile

import pytest

from src import config, storage


@pytest.fixture(scope="module")
def remote_base():
    """A URI-scheme OUTPUT_BASE backed by a temp dir, plus a live Spark JVM."""
    from src.spark_session import get_spark

    spark = get_spark("test-storage")
    original = config.OUTPUT_BASE
    config.OUTPUT_BASE = "file://" + tempfile.mkdtemp()
    yield config.OUTPUT_BASE
    config.OUTPUT_BASE = original
    spark.stop()


def test_is_remote_discriminates():
    assert storage.is_remote("gs://bucket/x")
    assert storage.is_remote("file:///tmp/x")
    assert not storage.is_remote("/tmp/x")
    assert not storage.is_remote("outputs/routes/x.csv")


def test_out_path_preserves_uri_scheme(remote_base):
    p = storage.out_path("routes", "x.csv")
    assert p.startswith("file://"), "storage_join must not collapse the //"
    assert storage.is_remote(p)


@pytest.mark.parametrize("payload", [
    "plain ascii",
    "héllo — ✓ ✗ →",          # reports contain em-dashes and arrows
    "",                         # empty file must not raise
    "a" * 50_000,               # larger than one buffer
    "line1\nline2\n",           # trailing newline preserved exactly
])
def test_text_round_trip_over_uri(remote_base, payload):
    """
    The regression that matters: an earlier implementation read via
    `readFully()` into a py4j array proxy, which the JVM fills but py4j does not
    reflect back -- yielding a correctly-sized buffer of NUL bytes. Every report
    would have been silently written fine and read back as zeros.
    """
    p = storage.out_path("statistics", "probe.md")
    storage.write_text(p, payload)
    assert storage.read_text(p) == payload


def test_overwrite_replaces_content(remote_base):
    p = storage.out_path("routes", "over.csv")
    storage.write_csv(p, ["a"], [(1,), (2,)])
    storage.write_csv(p, ["a"], [(9,)])
    assert storage.read_csv_rows(p) == [{"a": "9"}]


def test_csv_round_trip_over_uri(remote_base):
    p = storage.out_path("routes", "c.csv")
    storage.write_csv(p, ["min_len_km", "support", "subroute"],
                      [(1, 42, "a>b>c"), (3, 7, "d>e")])
    assert storage.read_csv_rows(p) == [
        {"min_len_km": "1", "support": "42", "subroute": "a>b>c"},
        {"min_len_km": "3", "support": "7", "subroute": "d>e"},
    ]


def test_missing_file_reads_as_empty_not_error(remote_base):
    """A stage skipped at this scale must degrade, not crash the next stage."""
    assert storage.read_csv_rows(storage.out_path("routes", "absent.csv")) == []
    assert not storage.exists(storage.out_path("routes", "absent.csv"))


def test_append_jsonl_accumulates(remote_base):
    p = storage.out_path("statistics", "t.jsonl")
    storage.append_jsonl(p, {"stage": "a", "wall_s": 1})
    storage.append_jsonl(p, {"stage": "b", "wall_s": 2})
    lines = [ln for ln in storage.read_text(p).splitlines() if ln.strip()]
    assert len(lines) == 2
    assert '"stage": "b"' in lines[1]
