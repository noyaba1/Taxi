"""
validate_env.py
===============
One-shot smoke test: confirms Java, Python, PySpark, Arrow and every third-party
library the pipeline imports all work together BEFORE you run a real stage.

    python -m src.validate_env

This is the gate for a local run, so it must be honest about the two things that
actually stop Spark from starting:

  * Java. Spark 3.5.x supports 8, 11 and 17 ONLY. A newer JDK does not say
    "unsupported" -- it dies deep in the JVM with reflection/module errors that
    look like a code bug. This machine shipped with Java 26, so we resolve a
    supported JDK ourselves (see spark_session._ensure_supported_java) and report
    which one we picked.
  * Python. PySpark 3.5.1 supports 3.8-3.12; 3.13 fails with cryptic Py4J errors.
"""
import importlib
import os
import sys

from src.spark_session import SUPPORTED_JAVA, _ensure_supported_java, _java_major

# Everything the pipeline imports at runtime; a missing one should be reported
# here, not three stages into a cloud run.
REQUIRED_MODULES = [
    ("pyspark", "Spark engine"),
    ("h3", "spatial encoding"),
    ("datasketches", "Count-Min / Space-Saving sketches"),
    ("pandas", "pandas_udf bridge"),
    ("numpy", "vectorised haversine"),
    ("pyarrow", "Arrow transport for pandas_udf"),
    ("folium", "map rendering"),
    ("geohash", "grid comparison baseline"),
]


def check(label, ok, detail=""):
    print(f"[{'OK' if ok else 'FAIL'}] {label}" + (f" -> {detail}" if detail else ""))
    return bool(ok)


def main() -> int:
    all_ok = True

    # 1. Python version (PySpark 3.5 supports 3.8-3.12).
    v = sys.version_info
    all_ok &= check(
        f"Python {v.major}.{v.minor}.{v.micro}",
        v.major == 3 and 8 <= v.minor <= 12,
        "PySpark 3.5 needs Python 3.8-3.12; use 3.11",
    )

    # 2. A Java version Spark can actually use.
    _ensure_supported_java()
    java_home = os.environ.get("JAVA_HOME", "")
    major = _java_major(java_home) if java_home else None
    all_ok &= check(
        "Java for Spark",
        major in SUPPORTED_JAVA,
        f"JAVA_HOME={java_home or 'unset'} (major={major}); "
        f"Spark 3.5 supports {SUPPORTED_JAVA}",
    )

    # 3. Every third-party import the pipeline needs.
    for mod, why in REQUIRED_MODULES:
        try:
            m = importlib.import_module(mod)
            ver = getattr(m, "__version__", "?")
            all_ok &= check(f"import {mod} ({why})", True, ver)
        except Exception as exc:  # noqa: BLE001
            all_ok &= check(f"import {mod} ({why})", False, repr(exc)[:120])

    # 4. Input data is reachable.
    from src import config

    all_ok &= check("raw train.csv resolved", os.path.exists(config.RAW_TRAIN),
                    config.RAW_TRAIN)

    # 5. PySpark runs a real job, including an Arrow pandas_udf round-trip --
    #    the feature and encoding stages are useless if Arrow is broken.
    try:
        import pandas as pd
        from pyspark.sql import functions as F
        from pyspark.sql.pandas.functions import pandas_udf
        from pyspark.sql.types import LongType

        from src.spark_session import get_spark

        spark = get_spark("validate-env")
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

        @pandas_udf(LongType())
        def _double(s: pd.Series) -> pd.Series:
            return s * 2

        n = spark.range(1000).filter("id % 2 = 0").count()
        arrow_sum = (spark.range(10).select(_double(F.col("id")).alias("d"))
                     .agg(F.sum("d")).collect()[0][0])
        spark.stop()
        all_ok &= check("PySpark job (expect 500)", n == 500, f"got {n}")
        all_ok &= check("Arrow pandas_udf (expect 90)", arrow_sum == 90,
                        f"got {arrow_sum}")
    except Exception as exc:  # noqa: BLE001
        all_ok &= check("PySpark / Arrow", False, repr(exc)[:300])

    print("\n" + ("ALL GOOD - ready to run the pipeline."
                  if all_ok else "Fix the FAIL lines above."))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
