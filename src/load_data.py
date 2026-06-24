"""
load_data.py
============
Loads the raw Porto Taxi CSV into a Spark DataFrame with an EXPLICIT schema.

Why an explicit schema (and not inferSchema=True)?
  - inferSchema makes Spark read the whole 1.9 GB file an extra time just to
    guess types. On a cluster that is wasted money. We KNOW the schema, so we
    declare it once.
  - It also protects us from Spark guessing TIMESTAMP as a date, ORIGIN_CALL
    as int when it contains "NA", etc.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, BooleanType,
)

# Raw schema EXACTLY as the lecturer's CSV header:
# TRIP_ID, CALL_TYPE, ORIGIN_CALL, ORIGIN_STAND, TAXI_ID,
# TIMESTAMP, DAY_TYPE, MISSING_DATA, POLYLINE
RAW_SCHEMA = StructType([
    StructField("TRIP_ID", StringType(), True),
    StructField("CALL_TYPE", StringType(), True),
    # ORIGIN_CALL / ORIGIN_STAND contain the literal "NA" -> read as string,
    # cast later only if we need them. Avoids parse failures.
    StructField("ORIGIN_CALL", StringType(), True),
    StructField("ORIGIN_STAND", StringType(), True),
    StructField("TAXI_ID", LongType(), True),
    StructField("TIMESTAMP", LongType(), True),   # unix seconds
    StructField("DAY_TYPE", StringType(), True),
    StructField("MISSING_DATA", StringType(), True),  # "True"/"False" text
    StructField("POLYLINE", StringType(), True),   # JSON text, parsed later
])


def load_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read the raw CSV with the fixed schema. Handles quoted JSON in POLYLINE."""
    return (
        spark.read
        .option("header", True)
        .option("quote", '"')          # POLYLINE is wrapped in double quotes
        .option("escape", '"')         # standard CSV escaping
        .option("multiLine", False)    # each trip is on one line
        .schema(RAW_SCHEMA)
        .csv(path)
    )
