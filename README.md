# Porto Taxi Trajectory Analysis — Big Data / Spark Project

Local-first PySpark project (VS Code on Windows) designed to migrate cleanly to
**GCP DataProc** (5+ machines). We build & debug everything on a small local
sample, run once on the full 1.9 GB file, and only *then* go to the cloud.

---

## Dataset (lecturer's files)

| File | Size | What it is |
|------|------|-----------|
| `train.csv/train.csv` | ~1.9 GB | Full Porto dataset, ~1.71M trips, 442 taxis |
| `Porto_taxi_data_test_partial_trajectories.csv` | 447 KB | Test set, partial trajectories |
| `solution_challengeII.csv` | — | `TRIP_ID, TRAVEL_TIME` (ground-truth travel time) |
| `solution_fixed.csv` | — | `TRIP_ID, LATITUDE, LONGITUDE` (ground-truth destination) |

**Schema:** `TRIP_ID, CALL_TYPE, ORIGIN_CALL, ORIGIN_STAND, TAXI_ID, TIMESTAMP,
DAY_TYPE, MISSING_DATA, POLYLINE`.

> ⚠️ **POLYLINE is `[longitude, latitude]` (lon first).** GPS sampled every 15 s,
> so `duration ≈ (n_points − 1) × 15` seconds. Getting lon/lat backwards flips
> the entire map — this is the single most common bug in this dataset.

---

## Roadmap (phases)

| Phase | File(s) | Goal |
|-------|---------|------|
| 0 | `requirements.txt`, this README | Environment + Java + Spark working locally |
| 1 ✅ | `make_sample.py`, `load_data.py`, `clean_data.py` | Parse POLYLINE, clean, write Parquet |
| 2 | `feature_engineering.py` | Distance (haversine), avg speed, bbox, anomaly flags |
| 3 | `eda.py` / notebook | Statistics + exploratory analysis (small outputs only) |
| 4 | `spatial_encoding.py` | H3 vs Geohash vs S2 → encode trajectories to cell sequences |
| 5 | `route_mining_suffix.py` | Frequent sub-routes via suffix-array / n-gram counting |
| 6 | `route_mining_clustering.py` | Clustering-based popular-route discovery |
| 7 | `route_mining_approx.py` | Original 3rd method + Count-Min / HLL / LSH / Bloom |
| 8 | `evaluation.py`, `visualization.py` | Runtime/memory/accuracy comparison, maps, final summary |

**Target deliverable:** top-100 popular long sub-routes for min lengths
{1, 3, 5, 10, 20, 40} km, with runtime/memory/accuracy comparison across methods.

---

## Phase 0 — Local setup (Windows + VS Code)

### ⚠️ Two environment problems detected on your machine

1. **Java is not installed.** Spark runs on the JVM — it cannot start without it.
2. **You have Python 3.13.1.** PySpark officially supports **3.8–3.12 only**;
   3.13 can crash with cryptic Py4J errors. Use a 3.11 virtual env.

### Recommended: also move the project off OneDrive + Hebrew path
The current path `OneDrive\שולחן העבודה\...` has **spaces, non-ASCII, and live
OneDrive sync**. All three cause real problems for Spark on Windows (and OneDrive
will try to upload the 1.9 GB file). Strongly recommended working copy:
`C:\dev\taxi`. Keep the data there too.

### Steps

```powershell
# 1. Install Java 11 (Temurin/Adoptium) and Python 3.11.
#    Then verify:
java -version           # should print 11.x
py -3.11 --version      # should print 3.11.x

# 2. Create + activate a clean virtual env (from project root)
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1

# 3. Install dependencies
pip install -r requirements.txt

# 4. Tell Spark where Java + Python are (PowerShell session vars)
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-11..."  # adjust
$env:PYSPARK_PYTHON = (Resolve-Path .\.venv\Scripts\python.exe)

# 5. (Windows only) winutils.exe for Hadoop — see note below.
```

> **winutils:** Spark on Windows needs `winutils.exe` + `hadoop.dll` matching the
> bundled Hadoop version. Put them in `C:\hadoop\bin` and set
> `$env:HADOOP_HOME = "C:\hadoop"`. Without this, Parquet writes throw
> `NullPointerException` / `UnsatisfiedLinkError`.

### Run Phase 1

```powershell
# Always develop on the sample first (seconds, not minutes):
python -m src.make_sample 5000        # build a 5k-trip sample from train.csv
python -m src.clean_data --sample     # parse + clean + write Parquet

# Only after it looks right, run the full file once:
python -m src.clean_data --full
```

**Expected output (sample):** a quality report (total / valid / dropped trips)
and `data/processed/trips_clean_sample.parquet/`.

---

## Migration to GCP DataProc (what changes)

Almost nothing in the code — by design:

| Concern | Local | DataProc |
|---------|-------|----------|
| Storage | `data/` folder | `export DATA_BASE=gs://bucket/porto` |
| Spark master | `local[*]` (set in `spark_session.py`) | `export SPARK_ENV=cloud` (YARN sets it) |
| Memory / partitions | small, in `config.py` | from cluster config |
| Submit | `python -m src.clean_data` | `gcloud dataproc jobs submit pyspark` |

> Budget rule: **never debug in the cloud.** Get correct results on the local
> sample → full local run → then one clean DataProc run for the final timings.
