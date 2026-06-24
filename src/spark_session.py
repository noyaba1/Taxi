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
from pyspark.sql import SparkSession
from src import config


def get_spark(app_name: str = "porto-taxi", shuffle_parts: int | None = None) -> SparkSession:
    env = os.environ.get("SPARK_ENV", "local")
    builder = SparkSession.builder.appName(app_name)

    if env == "local":
        # local[*] = use all CPU cores as workers (simulates a tiny cluster).
        builder = (
            builder.master("local[*]")
            # Give the local JVM room; lower if your laptop has < 8GB free.
            .config("spark.driver.memory", "4g")
            # Fewer shuffle partitions -> less overhead on a single machine.
            .config(
                "spark.sql.shuffle.partitions",
                str(shuffle_parts or config.LOCAL_SHUFFLE_PARTITIONS),
            )
        )
    # In "cloud" mode we deliberately set nothing: DataProc/YARN injects
    # master, executors, memory and partition counts from the cluster.

    # Adaptive Query Execution: lets Spark re-plan joins/partitions at runtime.
    # Helps on BOTH local and cloud, so we always enable it.
    builder = builder.config("spark.sql.adaptive.enabled", "true")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # silence the INFO flood
    return spark
