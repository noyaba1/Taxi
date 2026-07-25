# Setup

Two hard requirements, both of which fail in confusing ways if you get them wrong:

| | required | why |
|---|---|---|
| **Java** | 8, 11 or **17** | Spark 3.5.1 supports no others. A newer JDK (macOS ships 21/26 now) does not say "unsupported" — it dies deep in the JVM with reflection/module errors that look like a bug in this code. |
| **Python** | 3.8–**3.12** (use 3.11) | PySpark 3.5.1 does not support 3.13; you get cryptic Py4J failures, and only once a `pandas_udf` actually executes. |

`python -m src.validate_env` checks both, plus every library import, the data
file, and a real Arrow round-trip. Run it first, and after any change to your
environment.

---

## macOS (Apple Silicon or Intel)

```bash
brew install openjdk@17          # keg-only formula: no sudo, no system JDK change
brew install python@3.11

cd /path/to/Taxi
/opt/homebrew/bin/python3.11 -m venv .venv     # /usr/local/... on Intel
.venv/bin/pip install -r requirements.txt

.venv/bin/python -m src.validate_env           # expect all [OK]
```

You do **not** need to set `JAVA_HOME` or uninstall your existing Java.
`src/spark_session.py` locates a supported JDK itself (Homebrew, the macOS
`java_home` registry, or the usual Linux paths) and pins `PYSPARK_PYTHON` to the
venv interpreter — Spark otherwise launches its Python workers from `PATH`, which
is the *system* interpreter, and the mismatch only surfaces later as
`PYTHON_VERSION_MISMATCH`.

Use `.venv/bin/python` (or activate the venv) for every command below.

> `brew install --cask temurin@17` also works but needs a `sudo` password.
> The `openjdk@17` formula installs under `/opt/homebrew` without one.
> `validate_env` accepts either.

---

## Linux

```bash
sudo apt install openjdk-17-jdk python3.11 python3.11-venv
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m src.validate_env
```

---

## Windows

1. **Java 17** — install Temurin 17 and set `JAVA_HOME` to its folder.
2. **Python 3.11** — `py -3.11 -m venv .venv; .\.venv\Scripts\Activate.ps1`
3. `pip install -r requirements.txt`
4. **winutils** — Hadoop's Windows native layer is needed for *both* local reads
   and writes. Put `winutils.exe` and `hadoop.dll` in `C:\hadoop\bin`.
   `src/spark_session.py` finds `C:\hadoop` automatically and puts `bin` on
   `PATH` so the JVM can load `hadoop.dll` (without it, reads fail with
   `UnsatisfiedLinkError: NativeIO$Windows.access0`).
5. **Non-ASCII paths** — if the project lives under a Hebrew/accented folder,
   Spark's Windows launcher corrupts the classpath. `spark_session.py` switches
   `SPARK_HOME` and the Python paths to 8.3 short paths automatically.

Both Windows workarounds are no-ops on macOS/Linux and on DataProc.

---

## Data

Put the lecturer's files anywhere among these locations — `config.py` resolves
whichever exists, so no edit is needed:

```
taxi+service+trajectory+prediction+challenge+ecml+pkdd+2015/train.csv   <- as shipped
train.csv/train.csv
train.csv
```

Override explicitly with `RAW_TRAIN=/path/to/train.csv` if yours is elsewhere.
`validate_env` prints the file it resolved.

---

## First run

```bash
.venv/bin/python -m src.make_sample --sample            # 5,000 trips (~6 s)
.venv/bin/python -m src.run_pipeline --sample --verify  # 14 stages + 9 verifiers
.venv/bin/python -m pytest tests/ -q                    # 36 unit tests
```

Then open `outputs/maps/porto_map_sample.html`.

### Scales

| flag | trips | what it is for |
|---|---|---|
| `--sample` | 5,000 | correctness. Every stage, every verifier, ~3 minutes. |
| `--mid` | 200,000 | a real shuffle: hot-key skew, spill, memory pressure. This is where cloud surprises get caught for free. |
| `--full` | 1,710,670 | DataProc (`scripts/dataproc_submit.sh`). |

Build the mid sample with `python -m src.make_sample --mid` first.

**The exhaustive window miners (M5 exact, M6 closed) run at `--sample` only.**
They are quadratic in cells-per-trip and OOM at ~200k trips on a 16 GB machine;
they exist as ground truth for the sketches and the suffix array. `run_pipeline`
selects the right stages per scale, and `route_mining_exact` refuses to start
above `EXACT_MAX_TRIPS` (50,000) rather than dying an hour in.

---

## Tuning for your machine

All env-overridable, no code change:

| var | default | notes |
|---|---|---|
| `SPARK_DRIVER_MEM` | `8g` | lower to `4g` on an 8 GB machine |
| `SPARK_SHUFFLE_PARTS` | `64` | roughly 2–4x your core count locally |
| `SPARK_LOCAL_DIR` | `./.spark-tmp` | shuffle spill; keep it on a disk with room |
| `EXACT_MAX_TRIPS` | `50000` | the quadratic-baseline guard |
| `EXCLUDE_ANOMALOUS` | `1` | set `0` to encode corrupt trajectories too (don't) |
| `LOG_LEVEL` | `INFO` | |

---

## Troubleshooting

| symptom | cause | fix |
|---|---|---|
| `PYTHON_VERSION_MISMATCH` | Spark workers using system Python | should be automatic; check you ran `.venv/bin/python` |
| `UnsupportedClassVersionError`, odd JVM crashes | JDK 21+ | `brew install openjdk@17`; rerun `validate_env` |
| `OutOfMemoryError: Java heap space` in M5/M6 | quadratic baseline above its scale | use `--sample`, or the suffix array at scale |
| `read 0 rows from ...` | raw file not found | set `RAW_TRAIN`; `validate_env` shows what it resolved |
| `no method outputs found` from `evaluation` | mining stages not run yet | run `run_pipeline` rather than the stage alone |
