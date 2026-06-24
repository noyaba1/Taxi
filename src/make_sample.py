"""
make_sample.py
==============
Creates a SMALL sample CSV from the 1.9 GB train.csv so we can iterate fast.

WHY THIS IS THE FIRST THING WE RUN:
  - 1.9 GB on a laptop = every test takes minutes and may swap to disk.
  - We develop & debug ALL logic on a few thousand trips (seconds), then run
    the SAME code once on the full file, then on DataProc.
  - This is the #1 way to protect the professor's $50 GCP budget: never debug
    in the cloud. Debug locally on a sample.

Run:
    python -m src.make_sample            # default 5000 trips
    python -m src.make_sample 20000      # bigger local subset
"""
import sys
from src.spark_session import get_spark
from src import config
from src.load_data import load_raw


def main(n: int = 5000) -> None:
    spark = get_spark("make-sample")

    df = load_raw(spark, config.RAW_TRAIN)
    total = df.count()
    print(f"[make_sample] full file rows = {total:,}")

    # Sample by fraction (cheap, no full sort) then cap to exactly n rows.
    frac = min(1.0, (n * 1.5) / total)  # over-sample slightly, then limit
    sample = df.sample(withReplacement=False, fraction=frac, seed=42).limit(n)

    # Write a single CSV file so it is easy to inspect by hand.
    out = config.SAMPLE_CSV
    (
        sample.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(out + "_dir")  # Spark writes a folder; we point the loader there
    )
    print(f"[make_sample] wrote ~{n:,} trips to {out}_dir")
    spark.stop()


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    main(n)
