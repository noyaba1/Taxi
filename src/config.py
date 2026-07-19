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


def storage_join(base, *parts):
    """
    Join storage paths with '/', PRESERVING a URI scheme like gs://.
    pathlib.Path collapses the double slash ('gs://b' -> 'gs:/b'), which would
    break every GCS path when DATA_BASE='gs://bucket/...'. Local backslash bases
    still work because Windows/Spark accept forward slashes.
    """
    return "/".join([str(base).rstrip("/"), *parts])

# Raw inputs (the lecturer's files).
# NOTE: the full dataset physically lives at  train.csv/train.csv
RAW_TRAIN = os.environ.get(
    "RAW_TRAIN", str(PROJECT_ROOT / "train.csv" / "train.csv")
)
RAW_TEST = str(PROJECT_ROOT / "Porto_taxi_data_test_partial_trajectories.csv")

# Working datasets (gs://-safe joins so the cloud switch actually works)
SAMPLE_CSV = storage_join(DATA_BASE, "sample", "train_sample.csv")
CLEAN_PARQUET = storage_join(DATA_BASE, "processed", "trips_clean.parquet")

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
# Env-overridable so a bigger local (dry-)run can use more memory/partitions
# without code changes; DataProc ignores these (cluster mode sets its own).
LOCAL_SHUFFLE_PARTITIONS = int(os.environ.get("SPARK_SHUFFLE_PARTS", "16"))
LOCAL_DRIVER_MEM = os.environ.get("SPARK_DRIVER_MEM", "4g")

# ------------------------------------------------------------------
# 5. ROUTE MINING (Phase 5 / M5-M7) - single source of truth
# ------------------------------------------------------------------
ROUTE_LENGTH_THRESHOLDS_KM = [1, 3, 5, 10, 20, 40]  # min sub-route lengths
TOP_K = 100                     # top-N routes reported per threshold
MAX_SUBROUTE_KM = 45.0          # safety cap on window enumeration (> max threshold)

# --- M8 min-support X% + maximal ("popular long sub-route" per the PDF) ---
# A sub-route is "popular" if >= X% of trips traversed it; we then keep the
# MAXIMAL such routes (extend-and-still-frequent is impossible) => this maximises
# length subject to support >= X%, and produces the "holes" where traffic forks.
# X must be small on the 5k sample (sparse); it grows meaningful on the full data.
SUPPORT_X_PCT = 0.5                       # default min-support, percent of trips
SUPPORT_X_PCT_SWEEP = [0.2, 0.5, 1.0, 2.0]  # experiment values (PDF asks us to)

# --- M9 clustering (Method A): MinHash-LSH on directed bigram shingles ---
LSH_NUM_FEATURES = 1 << 18   # HashingTF dimensionality for shingles
LSH_NUM_HASH_TABLES = 5      # MinHashLSH hash tables (more -> better recall)
LSH_JACCARD_DIST_MAX = 0.3   # approxSimilarityJoin max Jaccard DISTANCE (=1-sim);
                             # 0.3 => similarity >= 0.7 (trips must share most of
                             # their transitions -> same corridor; looser explodes)
# The LSH self-join grows ~quadratically AND suffers bucket skew as #trips rises
# (dry run: 50k = 243k edges, stable in ~90s; 100k = LSH bucket skew, worker
# timeout). So we cluster a REPRESENTATIVE sample bounded to a size proven stable.
# Popular corridors are frequent, hence well-represented in any large sample, so
# capping does not lose them. DataProc can raise this via the env var.
CLUSTERING_MAX_TRIPS = int(os.environ.get("CLUSTERING_MAX_TRIPS", "50000"))
CC_MAX_ITER = 15             # label-propagation sweeps for connected components
CLUSTER_MIN_SIZE = 3         # ignore clusters smaller than this (noise)

# --- M10 transition graph (Method C): PageRank zones + heavy-path routes ---
GRAPH_MIN_EDGE_SUPPORT = 5   # keep a cell->cell transition only if >= this trips
PAGERANK_ITERS = 15          # power-iteration sweeps
PAGERANK_DAMPING = 0.85      # standard damping factor
GRAPH_MAX_SEEDS = 3000       # top frequent edges used to seed heavy-path walks
GRAPH_MIN_FLOW_PROB = 0.4    # extend a corridor only while >=40% of a cell's flow
                             # continues that way (dominant flow); below => fork/hole
ACTIVITY_ZONES_TOP = 50      # number of activity-zone cells to report

# --- M11 anomalous-route analysis ---
ANOMALY_PCT = 0.99           # percentile fence for statistical outliers
ANOMALY_METRO_MARGIN = 0.05  # deg beyond metro bbox before a route counts as drift

# --- M7 approximate sketch sizing (all tunable) ---
SKETCH_LG_MAX_K = 16            # frequent-items map size = 2^LG (~49k counters)
CM_HASHES = 5                   # Count-Min depth  (failure prob ~ 2^-5)
CM_LG_BUCKETS = 17             # Count-Min width per row = 2^17
# datasketches sketches use a FIXED internal seed (Count-Min default 9001) so
# they are deterministic AND mergeable across partitions. deserialize() rebuilds
# with that default, so we must not override it. This constant documents that the
# sketches are seeded/deterministic; it is not passed to the CMS constructor.
SKETCH_SEED = 9001             # = datasketches Count-Min default seed
