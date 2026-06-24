"""
config.py
=========
Single source of truth for paths, schema and tunable constants.

DESIGN PRINCIPLE (local-first, cloud-ready):
    Every path goes through this file. When we migrate to GCP DataProc we
    only change the *_BASE constants here (e.g. to "gs://my-bucket/...").
    No other source file hard-codes a path. This is what makes the migration
    a 5-minute change instead of a refactor.
"""
from pathlib import Path
import os

# ------------------------------------------------------------------
# 1. STORAGE LAYER  (local now, GCS later)
# ------------------------------------------------------------------
# Local development uses the project folder. On DataProc we will set
# the env var DATA_BASE="gs://<bucket>/porto" and nothing else changes.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Allow overriding via environment variable -> this is the cloud switch.
DATA_BASE = os.environ.get("DATA_BASE", str(PROJECT_ROOT / "data"))
OUTPUT_BASE = os.environ.get("OUTPUT_BASE", str(PROJECT_ROOT / "outputs"))

# Raw inputs (the lecturer's files).
# NOTE: the full dataset physically lives at  train.csv/train.csv
RAW_TRAIN = os.environ.get(
    "RAW_TRAIN", str(PROJECT_ROOT / "train.csv" / "train.csv")
)
RAW_TEST = str(PROJECT_ROOT / "Porto_taxi_data_test_partial_trajectories.csv")

# Working datasets
SAMPLE_CSV = str(Path(DATA_BASE) / "sample" / "train_sample.csv")
CLEAN_PARQUET = str(Path(DATA_BASE) / "processed" / "trips_clean.parquet")

# ------------------------------------------------------------------
# 2. DATASET CONSTANTS
# ------------------------------------------------------------------
# GPS is sampled every 15 seconds (given by the dataset spec).
GPS_INTERVAL_SEC = 15

# !!! CRITICAL !!! POLYLINE points are [LONGITUDE, LATITUDE] (lon first).
# Porto is around lon=-8.6, lat=41.15. Mixing this up flips the whole map.
PORTO_LON_RANGE = (-8.75, -8.45)   # plausible longitude box for Porto metro
PORTO_LAT_RANGE = (41.05, 41.30)   # plausible latitude  box for Porto metro

# Cleaning thresholds (justifiable, tweak in evaluation phase)
MIN_POINTS = 2          # a trip needs at least 2 points to have a direction
MAX_POINTS = 4000       # >4000 pts (~16.6h) is almost certainly corrupted
MAX_SPEED_KMH = 200.0   # taxi physically cannot exceed this between samples

# ------------------------------------------------------------------
# 3. SPATIAL ENCODING (used from Phase 4 onward)
# ------------------------------------------------------------------
H3_RESOLUTION = 9       # ~174 m edge hexagons - justified in README Phase 4

# ------------------------------------------------------------------
# 4. SPARK TUNING (local). DataProc overrides these via cluster config.
# ------------------------------------------------------------------
LOCAL_SHUFFLE_PARTITIONS = 16   # small for a laptop; DataProc uses ~200+
