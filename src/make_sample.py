"""
make_sample.py
==============
Creates a SMALL sample CSV from the 1.9 GB train.csv so we can iterate fast.

WHY THIS IS THE FIRST THING WE RUN:
  - 1.9 GB on a laptop = every test takes minutes and may swap to disk.
  - We develop & debug ALL logic on a few thousand trips (seconds), then run
    the SAME code at mid scale, then on DataProc.
  - This is the #1 way to protect the GCP budget: never debug in the cloud.

Two scales, so the local story is complete before any spend:
    python -m src.make_sample --sample          # 5,000 trips: correctness
    python -m src.make_sample --mid             # 200,000 trips: real shuffle/skew
    python -m src.make_sample --sample --n 20000

The mid-scale run is the one that catches what the tiny sample cannot: hot-key
skew in the sub-route groupBy, spill, and memory pressure.
"""
from src import cli, config
from src.load_data import load_raw
from src.spark_session import get_spark

log = cli.setup_logging("sample")


def main(scale: str, n: int | None) -> None:
    if scale == "full":
        raise SystemExit("--full has nothing to sample; it IS the whole file")
    n = n or config.DEFAULT_SAMPLE_N[scale]

    spark = get_spark("make-sample")
    with cli.stage("m0_make_sample", scale, log) as st:
        log.info("reading raw file: %s", config.RAW_TRAIN)
        df = load_raw(spark, config.RAW_TRAIN)
        total = df.count()
        if total == 0:
            raise RuntimeError(
                f"read 0 rows from {config.RAW_TRAIN!r}. Set RAW_TRAIN or check "
                f"that the dataset folder is present.")
        log.info("full file rows = %s", f"{total:,}")

        # Sample by fraction (cheap, no full sort) then cap to exactly n rows.
        frac = min(1.0, (n * 1.5) / total)  # over-sample slightly, then limit
        sample = df.sample(withReplacement=False, fraction=frac, seed=42).limit(n)

        out = config.sample_csv_dir(scale)
        (sample.coalesce(1)
         .write.mode("overwrite")
         .option("header", True)
         .option("quote", '"')
         .option("escape", '"')
         .csv(out))
        log.info("wrote ~%s trips to %s", f"{n:,}", out)
        st.update(rows_in=total, rows_out=n)

    spark.stop()


if __name__ == "__main__":
    ap = cli.scale_parser(__doc__)
    ap.add_argument("--n", type=int, default=None,
                    help="override the row count for this scale")
    args = ap.parse_args()
    main(cli.scale_of(args), args.n)
