"""
spark_session.py
================
Factory for a SparkSession that behaves the SAME locally and on DataProc.

Why a factory?
  - Locally we want master="local[*]" and modest memory.
  - On DataProc we DO NOT set master at all (YARN provides it).
  The single env var SPARK_ENV="cloud" flips the behaviour, so the same
  scripts run in both places untouched.
"""
import os
import sys
from pyspark.sql import SparkSession
from src import config


def _short_path(path: str) -> str:
    """Return the Windows 8.3 short path (ASCII) for `path`, else unchanged."""
    if os.name != "nt" or not path:
        return path
    import ctypes
    buf = ctypes.create_unicode_buffer(1024)
    n = ctypes.windll.kernel32.GetShortPathNameW(str(path), buf, 1024)
    return buf.value if n else path


def _ensure_ascii_spark_paths() -> None:
    """
    Spark's Windows launcher (spark-class2.cmd) cannot handle non-ASCII paths
    in the classpath. If this project lives under a non-ASCII path (e.g. a
    Hebrew 'Desktop' folder under OneDrive), the JVM gets a corrupted classpath
    and fails with ClassNotFoundException: SparkSubmit.

    Fix: point SPARK_HOME and the worker/driver Python at the ASCII 8.3 short
    paths. This is a no-op on Linux/DataProc (os.name != 'nt') and on already
    ASCII paths, so it is safe to always call.
    """
    if os.name != "nt":
        return

    def is_ascii(s: str) -> bool:
        return all(ord(c) < 128 for c in s)

    try:
        import pyspark
        spark_home = os.environ.get("SPARK_HOME") or os.path.dirname(pyspark.__file__)
    except Exception:  # noqa: BLE001
        spark_home = os.environ.get("SPARK_HOME", "")

    if is_ascii(sys.executable) and is_ascii(spark_home):
        return  # clean path, nothing to do

    short_py = _short_path(sys.executable)
    os.environ["SPARK_HOME"] = _short_path(spark_home)
    os.environ["PYSPARK_PYTHON"] = short_py
    os.environ["PYSPARK_DRIVER_PYTHON"] = short_py


def _ensure_hadoop_home() -> None:
    """
    On Windows, BOTH the local-filesystem read and write paths go through
    Hadoop's native layer:
      * writes use winutils.exe (Shell.checkHadoopHome),
      * reads call NativeIO$Windows.access0 (FileUtil.canRead during listStatus),
        which is a NATIVE method requiring hadoop.dll to be LOADED.

    `hadoop.dll` is only loaded if its directory is on the JVM's library path,
    which on Windows is derived from PATH. So we must ALWAYS put HADOOP_HOME\\bin
    on PATH (even when HADOOP_HOME is already set), or the JVM can't find
    hadoop.dll and access0 throws UnsatisfiedLinkError. No-op on Linux/DataProc.
    """
    if os.name != "nt":
        return
    # Pick HADOOP_HOME: respect an existing valid one, else probe C:\hadoop.
    candidates = [os.environ.get("HADOOP_HOME", ""), r"C:\hadoop"]
    for candidate in candidates:
        if candidate and os.path.exists(os.path.join(candidate, "bin", "winutils.exe")):
            os.environ["HADOOP_HOME"] = candidate
            bin_dir = os.path.join(candidate, "bin")
            # ALWAYS ensure bin is on PATH so the JVM can load hadoop.dll.
            path_parts = os.environ.get("PATH", "").split(os.pathsep)
            if bin_dir not in path_parts:
                os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
            return


# Spark 3.5.x runs on Java 8, 11 or 17 ONLY. A newer JDK (this machine shipped
# with 26) fails deep inside the JVM with opaque reflection/module errors rather
# than a readable "unsupported version", so we resolve a supported one ourselves
# instead of relying on whatever JAVA_HOME happens to be.
SUPPORTED_JAVA = (8, 11, 17)


def _java_major(java_home: str) -> int | None:
    """Read the major version from a JDK's release file (no subprocess)."""
    release = os.path.join(java_home, "release")
    try:
        with open(release, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if line.startswith("JAVA_VERSION="):
                    ver = line.split("=", 1)[1].strip().strip('"')
                    head = ver.split(".")
                    # "1.8.0_xxx" -> 8 ; "17.0.19" -> 17
                    return int(head[1]) if head[0] == "1" else int(head[0])
    except OSError:
        return None
    return None


def _ensure_supported_java() -> None:
    """
    Point JAVA_HOME at a Spark-supported JDK if it is unset or too new.

    Probes the usual install locations for Homebrew, the macOS java_home
    registry and Linux/DataProc. A cluster image already exports a correct
    JAVA_HOME, so this is a no-op there.
    """
    current = os.environ.get("JAVA_HOME")
    if current and _java_major(current) in SUPPORTED_JAVA:
        return

    candidates = []
    for major in (17, 11, 8):
        candidates += [
            f"/opt/homebrew/opt/openjdk@{major}/libexec/openjdk.jdk/Contents/Home",
            f"/usr/local/opt/openjdk@{major}/libexec/openjdk.jdk/Contents/Home",
            f"/Library/Java/JavaVirtualMachines/temurin-{major}.jdk/Contents/Home",
            f"/usr/lib/jvm/java-{major}-openjdk-amd64",
            f"/usr/lib/jvm/temurin-{major}-jdk-amd64",
        ]
    for cand in candidates:
        if _java_major(cand) in SUPPORTED_JAVA:
            os.environ["JAVA_HOME"] = cand
            return
    # Nothing found: leave JAVA_HOME alone and let Spark produce its own error.
    # validate_env reports this properly before anyone reaches a real job.


def get_spark(app_name: str = "porto-taxi", shuffle_parts: int | None = None) -> SparkSession:
    _ensure_supported_java()     # pick a JDK Spark 3.5 actually supports
    _ensure_ascii_spark_paths()  # Windows non-ASCII path guard (no-op elsewhere)
    _ensure_hadoop_home()        # Windows winutils for local writes (no-op elsewhere)

    env = os.environ.get("SPARK_ENV", "local")
    builder = SparkSession.builder.appName(app_name)

    if env == "local":
        # Spark launches Python WORKERS via `python3` from PATH, which is the
        # system interpreter (3.13 here) -- not the venv the driver is running
        # in. The mismatch only surfaces once a pandas_udf actually executes,
        # as PYTHON_VERSION_MISMATCH. Pin both sides to THIS interpreter.
        # Cloud mode is left alone: YARN executors live on other machines where
        # this path does not exist, and the image sets its own.
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        # Binding to the LAN address breaks when the machine roams between
        # networks mid-run; loopback is stable and correct for local[*].
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.makedirs(config.LOCAL_SPARK_TMP, exist_ok=True)
        # local[*] = use all CPU cores as workers (simulates a tiny cluster).
        builder = (
            builder.master("local[*]")
            # Give the local JVM room; lower if your laptop has < 8GB free.
            .config("spark.driver.memory", config.LOCAL_DRIVER_MEM)
            # Fewer shuffle partitions -> less overhead on a single machine.
            .config(
                "spark.sql.shuffle.partitions",
                str(shuffle_parts or config.LOCAL_SHUFFLE_PARTITIONS),
            )
            # Keep shuffle spill on the project disk, not the (smaller) /tmp
            # volume: the mid-scale run spills tens of GB.
            .config("spark.local.dir", config.LOCAL_SPARK_TMP)
        )
    # In "cloud" mode we deliberately set nothing: DataProc/YARN injects
    # master, executors, memory and partition counts from the cluster.

    # Adaptive Query Execution: lets Spark re-plan joins/partitions at runtime.
    # Helps on BOTH local and cloud, so we always enable it. Skew handling is
    # stated explicitly because downtown H3 cells ARE hot keys -- this is the
    # single most important setting for the sub-route groupBy.
    builder = (builder
               .config("spark.sql.adaptive.enabled", "true")
               .config("spark.sql.adaptive.skewJoin.enabled", "true")
               .config("spark.sql.adaptive.coalescePartitions.enabled", "true"))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # silence the INFO flood
    return spark
