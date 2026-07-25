"""
storage.py
==========
The single I/O boundary for everything written under OUTPUT_BASE.

WHY THIS EXISTS
---------------
`config.storage_join` already kept `gs://` intact for the DATA paths, but every
report and result CSV was written with the Python builtins:

    os.makedirs(os.path.join(config.OUTPUT_BASE, "routes"))
    open(path, "w").write(...)

On a `gs://` base those do not fail loudly -- they create a LOCAL directory
literally named `gs:` on whichever machine the driver happens to be. The cloud
submit script worked around it by pointing OUTPUT_BASE at the master's /tmp, and
then deleted the cluster (and therefore every top-100 CSV) on teardown.

So: local paths behave exactly as before, and any path with a URI scheme goes
through Hadoop's FileSystem via the JVM the SparkSession already owns. No new
dependency, and it works unchanged on DataProc.

Everything here takes/returns text or row tuples -- these are SMALL artefacts
(<=600 rows). Bulk data still goes through Spark's own parquet writer.
"""
from __future__ import annotations

import csv
import io
import os

from src import config


def is_remote(path: str) -> bool:
    """True for gs://, s3a://, hdfs:// ... i.e. anything the builtins can't open."""
    return "://" in str(path)


def out_path(*parts: str) -> str:
    """Path under OUTPUT_BASE, scheme-safe."""
    return config.storage_join(config.OUTPUT_BASE, *parts)


# ----------------------------------------------------------------- JVM bridge
def _hadoop(path: str):
    """
    Return (FileSystem, Path) for a remote URI, using the active SparkSession's
    JVM. Raises a clear error if no session is alive -- writing to gs:// without
    Spark is a programming mistake, not a runtime condition to paper over.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError(
            f"cannot access remote path {path!r}: no active SparkSession. "
            "Call this while Spark is up, or set OUTPUT_BASE to a local dir."
        )
    jvm = spark.sparkContext._jvm
    jpath = jvm.org.apache.hadoop.fs.Path(path)
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    return jpath.getFileSystem(hconf), jpath


# ----------------------------------------------------------------- public API
def makedirs(path: str) -> None:
    if is_remote(path):
        fs, jpath = _hadoop(path)
        fs.mkdirs(jpath)
    else:
        os.makedirs(path, exist_ok=True)


def exists(path: str) -> bool:
    if is_remote(path):
        fs, jpath = _hadoop(path)
        return bool(fs.exists(jpath))
    return os.path.exists(path)


def write_text(path: str, text: str) -> str:
    """Write `text` to `path`, creating the parent directory. Returns `path`."""
    if is_remote(path):
        fs, jpath = _hadoop(path)
        fs.mkdirs(jpath.getParent())
        stream = fs.create(jpath, True)          # True = overwrite
        try:
            stream.write(bytearray(text.encode("utf-8")))
        finally:
            stream.close()
    else:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
    return path


def write_lines(path: str, lines) -> str:
    """Write report lines (the miners all build a list of markdown lines)."""
    return write_text(path, "\n".join(lines) + "\n")


def write_csv(path: str, header, rows) -> str:
    """Write a small CSV. Rows are tuples/lists; header is a list of names."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    return write_text(path, buf.getvalue())


def read_text(path: str) -> str:
    """
    Read a small text object.

    The remote branch decodes JVM-SIDE via commons-io (a Hadoop dependency, so
    always on a Spark classpath) and returns one finished string.

    Two approaches that look reasonable and are not:
      * `stream.read()` in a loop returns one int per byte -- a py4j round-trip
        per byte, so a 100 KB report becomes 100k JVM calls.
      * `readFully(gateway.new_array(...))` silently returns ZEROS. The JVM fills
        its own array, but the py4j proxy does not reflect that write back, so
        you get a correctly-sized buffer of nulls and a very confusing bug.
    """
    if is_remote(path):
        fs, jpath = _hadoop(path)
        if int(fs.getFileStatus(jpath).getLen()) == 0:
            return ""
        from pyspark.sql import SparkSession

        jvm = SparkSession.getActiveSession().sparkContext._jvm
        stream = fs.open(jpath)
        try:
            return jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
        finally:
            stream.close()
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def read_csv_rows(path: str) -> list[dict]:
    """
    Read a small CSV into a list of dicts (what every verify_* script wants).
    Returns [] for a missing file so a downstream stage can degrade gracefully
    instead of dying on an upstream stage that was skipped.
    """
    if not exists(path):
        return []
    return list(csv.DictReader(io.StringIO(read_text(path))))


def append_jsonl(path: str, record: dict) -> str:
    """
    Append one JSON record. Object stores cannot append, so remote writes do
    read-modify-write; these files hold one row per stage run, so that is fine.
    """
    import json

    line = json.dumps(record, sort_keys=True) + "\n"
    prior = read_text(path) if exists(path) else ""
    return write_text(path, prior + line)
