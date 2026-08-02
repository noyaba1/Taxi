"""
config.py
=========
Single source of truth for paths, schema and tunable constants.

DESIGN PRINCIPLE (local-first, cloud-ready):
    Every path goes through this file. When we migrate to GCP DataProc we
    only change the *_BASE constants here (e.g. to "gs://my-bucket/...").
    No other source file hard-codes a path. This is what makes the migration
    a 5-minute change instead of a refactor.

That principle is now ENFORCED rather than merely stated:
  * `dataset_paths(scale)` is the ONLY place a parquet filename is built. Six
    modules used to reconstruct `_encoded_r{res}_{scale}.parquet` independently,
    which caused a real cloud outage (commit adf2caf, "cloud --full blocker").
  * `RAW_*` inputs are RESOLVED against the filesystem instead of assumed, so a
    checkout whose data sits in a differently-named folder still runs.
  * Mining constants live here only; `route_mining_exact` used to redefine the
    length thresholds locally, so editing this file silently did nothing.
"""
from __future__ import annotations  # PEP-604 `X | None` in signatures
# must not be evaluated at import time: the DataProc submit script reads
# config with the SYSTEM python3, which on Debian/Cloud Shell can be 3.9.
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


def _first_existing(env_var, *candidates):
    """
    Resolve an input file: an explicit env var always wins (that is the cloud
    override, and it may point at a gs:// object we cannot stat), otherwise take
    the first candidate that actually exists on disk.

    Returns the first candidate unchanged when none exist, so the caller still
    gets a usable path in the error message rather than None.
    """
    override = os.environ.get(env_var)
    if override:
        return override
    for c in candidates:
        if Path(c).exists():
            return str(c)
    return str(candidates[0])


# The lecturer ships the data inside a long UCI-style folder name; older
# instructions assumed a `train.csv/train.csv` layout. Accept both (and a plain
# file at the project root) instead of hard-coding one and failing on the other.
_UCI_DIR = PROJECT_ROOT / "taxi+service+trajectory+prediction+challenge+ecml+pkdd+2015"

RAW_TRAIN = _first_existing(
    "RAW_TRAIN",
    _UCI_DIR / "train.csv",
    PROJECT_ROOT / "train.csv" / "train.csv",
    PROJECT_ROOT / "train.csv",
)
RAW_TEST = _first_existing(
    "RAW_TEST",
    _UCI_DIR / "Porto_taxi_data_test_partial_trajectories.csv",
    PROJECT_ROOT / "Porto_taxi_data_test_partial_trajectories.csv",
)
# Ground truth for the original Kaggle challenge. Not used by the route-mining
# pipeline; kept resolvable so a future accuracy study does not re-invent this.
SOLUTION_TRAVEL_TIME = _first_existing(
    "SOLUTION_TRAVEL_TIME", _UCI_DIR / "solution_challengeII.csv")
SOLUTION_DESTINATION = _first_existing(
    "SOLUTION_DESTINATION", _UCI_DIR / "solution_fixed.csv")

# ------------------------------------------------------------------
# 2. DATASET SCALES AND DERIVED PATHS
# ------------------------------------------------------------------
# "sample" = a few thousand trips (seconds, for correctness);
# "mid"    = a few hundred thousand (minutes, exercises a real shuffle/skew);
# "full"   = all 1.71M (DataProc).
# Extra scales exist so full-scale behaviour can be EXTRAPOLATED from a measured
# curve rather than guessed. Two points is a line; four is a trend.
SCALES = ("sample", "mid", "s400k", "s800k", "full")
DEFAULT_SAMPLE_N = {"sample": 5_000, "mid": 200_000,
                    "s400k": 400_000, "s800k": 800_000}

_PROCESSED = storage_join(DATA_BASE, "processed")
_SAMPLE_DIR = storage_join(DATA_BASE, "sample")


def sample_csv_dir(scale: str) -> str:
    """Spark writes a DIRECTORY of part files; the loader reads the directory."""
    return storage_join(_SAMPLE_DIR, f"train_{scale}.csv_dir")


def dataset_paths(scale: str, resolution: int | None = None) -> dict:
    """
    The single place a dataset filename is constructed.

    Naming is uniform across scales (`_sample` / `_mid` / `_full`) on purpose:
    the previous convention left the full-scale clean/feature tables unsuffixed
    while the encoded table was suffixed `_full`, and the mismatch shipped to
    production once already.
    """
    if scale not in SCALES:
        raise ValueError(f"unknown scale {scale!r}; expected one of {SCALES}")
    res = H3_RESOLUTION if resolution is None else resolution
    return {
        "scale": scale,
        "raw_csv": RAW_TRAIN if scale == "full" else sample_csv_dir(scale),
        "clean": storage_join(_PROCESSED, f"trips_clean_{scale}.parquet"),
        "features": storage_join(_PROCESSED, f"trips_features_{scale}.parquet"),
        "encoded": storage_join(_PROCESSED, f"trips_encoded_r{res}_{scale}.parquet"),
    }


# ------------------------------------------------------------------
# 3. DATASET CONSTANTS
# ------------------------------------------------------------------
# GPS is sampled every 15 seconds (given by the dataset spec).
GPS_INTERVAL_SEC = 15

# The Porto fleet: 442 taxis over 2013-2014, fixed by the dataset. This is a
# CEILING, not a tuning knob -- no group of trips can contain more distinct
# vehicles than exist. It is what makes a distinct-count sketch pointless here
# (see the HyperLogLog result in route_mining_graph), and it is the bound the
# cloud verifier holds `distinct_taxis` against.
FLEET_SIZE = 442

# The clock the data was generated on. TIMESTAMP is unix epoch, so turning it
# into an HOUR OF DAY requires a timezone -- and Spark's `from_unixtime` uses
# `spark.sql.session.timeZone`, which defaults to whatever the JVM's machine is
# set to. Left unset, the temporal analysis silently answers a different
# question on every machine:
#
#   MEASURED, same 1,614,508 trips, same code, same commit --
#     laptop (Asia/Jerusalem, UTC+3)   morning_peak = 209,618 trips
#     DataProc (UTC)                   morning_peak = 296,371 trips
#
#   Totals matched exactly; only the bucket ASSIGNMENT moved. The laptop's
#   "morning peak 6-10" was really Porto's ~3-7 a.m.
#
# Porto is Europe/Lisbon. Pinning it here makes the buckets mean what the report
# says they mean, and makes the result identical everywhere. spark_session
# applies this in BOTH local and cloud mode.
DATASET_TIMEZONE = os.environ.get("DATASET_TZ", "Europe/Lisbon")

# !!! CRITICAL !!! POLYLINE points are [LONGITUDE, LATITUDE] (lon first).
# Porto is around lon=-8.6, lat=41.15. Mixing this up flips the whole map.
PORTO_LON_RANGE = (-8.75, -8.45)   # plausible longitude box for Porto metro
PORTO_LAT_RANGE = (41.05, 41.30)   # plausible latitude  box for Porto metro

# Cleaning thresholds (justifiable, tweak in evaluation phase)
MIN_POINTS = 2          # a trip needs at least 2 points to have a direction
MAX_POINTS = 4000       # >4000 pts (~16.6h) is almost certainly corrupted
MAX_SPEED_KMH = 200.0   # taxi physically cannot exceed this between samples

# ------------------------------------------------------------------
# 4. SPATIAL ENCODING
# ------------------------------------------------------------------
H3_RESOLUTION = 9       # ~174 m edge hexagons - justified in README Phase 4


def max_cell_hop_km(resolution: int | None = None) -> float:
    """
    Largest plausible distance between two CONSECUTIVE compact cells.

    Derived, not guessed, from two facts we already commit to elsewhere:

      travel       MAX_SPEED_KMH is the speed above which we call a segment a
                   teleport and drop the trip. So the furthest a *retained*
                   vehicle can move between two GPS samples is
                   MAX_SPEED_KMH * GPS_INTERVAL_SEC.  = 0.83 km at 200 km/h / 15 s
      quantisation a cell sequence reports each position at its cell CENTRE,
                   which can sit up to one circumradius (== edge length, for a
                   hexagon) from the true point -- at BOTH ends of the hop.
                   = 2 * 0.175 km at res 9

    Sum: ~1.18 km at res 9. Anything larger cannot be produced by a vehicle we
    chose to keep, so it is a GAP in the trace -- lost signal, a tunnel, a
    tracker reset -- and the miners split trajectories there.

    Getting this bound wrong is expensive in both directions. Too tight and a
    quarter of ordinary fast trips get chopped into fragments (measured: a
    0.52 km limit cut 26% of clean sample trips). Too loose and a GPS jump is
    admitted as road, which is what let a 2-cell window report itself as a 40 km
    "sub-route" in the top-100 lists.
    """
    import h3  # local import: config must stay importable before deps install
    res = H3_RESOLUTION if resolution is None else resolution
    travel = MAX_SPEED_KMH * (GPS_INTERVAL_SEC / 3600.0)
    quantisation = 2.0 * h3.edge_length(res, unit="km")
    return travel + quantisation


# Drop trips flagged anomalous (teleport / impossible speed / idle / degenerate)
# before encoding. They are not "interesting outliers" for route mining -- their
# trajectories are physically impossible and pollute every downstream support
# count. The anomaly STUDY (M11) still runs on the unfiltered feature table.
EXCLUDE_ANOMALOUS = os.environ.get("EXCLUDE_ANOMALOUS", "1") not in ("0", "false", "False")

# ------------------------------------------------------------------
# 5. SPARK TUNING (local). DataProc overrides these via cluster config.
# ------------------------------------------------------------------
# Env-overridable so a bigger local (dry-)run can use more memory/partitions
# without code changes; DataProc ignores these (cluster mode sets its own).
LOCAL_SHUFFLE_PARTITIONS = int(os.environ.get("SPARK_SHUFFLE_PARTS", "64"))
LOCAL_DRIVER_MEM = os.environ.get("SPARK_DRIVER_MEM", "8g")
# Keep shuffle spill inside the project (plenty of disk) instead of /tmp.
LOCAL_SPARK_TMP = os.environ.get("SPARK_LOCAL_DIR", str(PROJECT_ROOT / ".spark-tmp"))

# ------------------------------------------------------------------
# 6. ROUTE MINING - single source of truth (do NOT redeclare downstream)
# ------------------------------------------------------------------
ROUTE_LENGTH_THRESHOLDS_KM = [1, 3, 5, 10, 20, 40]  # min sub-route lengths
TOP_K = 100                     # top-N routes reported per threshold
# Safety cap on window enumeration. Must exceed the largest threshold with room
# to spare: a window sitting exactly AT the cap has no recorded extension, so a
# maximality test would wrongly promote it. Windows that hit the cap are marked
# `truncated` and excluded from maximal output instead.
MAX_SUBROUTE_KM = float(max(ROUTE_LENGTH_THRESHOLDS_KM)) * 1.5   # 60.0

# The exhaustive window miner is quadratic in cells-per-trip. Measured: it OOMs
# at ~200k trips with an 8 GB driver, and would shuffle >100 GB at 1.71M. It is
# kept as GROUND TRUTH for the sketches and the suffix array at small scale, and
# refuses to start above this many trips rather than dying an hour in.
EXACT_MAX_TRIPS = int(os.environ.get("EXACT_MAX_TRIPS", "50000"))

# --- min-support X% + maximal ("popular long sub-route" per the PDF) ---
# A sub-route is "popular" if >= X% of trips traversed it; we then keep the
# MAXIMAL such routes (extend-and-still-frequent is impossible) => this maximises
# length subject to support >= X%, and produces the "holes" where traffic forks.
SUPPORT_X_PCT = 0.5                       # reference X for the headline report
# X is CALIBRATED PER LENGTH THRESHOLD from this descending grid: for each L we
# take the largest X that still yields TOP_K routes of length >= L. A single
# global X cannot serve both the 1 km and the 40 km config -- at X=0.5% on 1.71M
# trips a 40 km corridor would need ~8.5k distinct trips, so that config comes
# back empty. The PDF asks us to maximise length subject to >= X%, which only
# has an answer if X is allowed to move with L.
SUPPORT_X_PCT_GRID = [5.0, 2.0, 1.0, 0.5, 0.2, 0.1, 0.05, 0.02, 0.01]

# ...but calibration walks ABSOLUTE floors, not percentages, and here is why.
#
# A percentage floor is scale-dependent in the wrong direction. 0.01% is 2 trips
# on the 5k sample but 171 trips at 1.71M -- so the more data you have, the
# HARDER it becomes to clear the bottom of the grid, and the long length bands
# get emptier as the dataset grows. Measured: the sample reached min_sup=2 and
# found an 11.99 km corridor; the mid scale bottomed out at min_sup=19 and found
# 11.06 km. That is backwards.
#
# The brief asks us to tune X "when you are interested in maximising the
# sub-route length", so the honest instrument is an absolute floor, reported
# alongside the X% it happens to correspond to at that scale.
SUPPORT_MIN_SUP_GRID = [2, 3, 5, 10, 20, 50, 100, 250, 500, 1000, 2500, 5000]


def pct_of(min_sup: int, n_trips: int) -> float:
    """The X% an absolute support floor corresponds to at this scale."""
    return 100.0 * min_sup / n_trips if n_trips else float("nan")


# --- temporal analysis: is "popular" the same at 08:00 and 03:00? ---
# Hour-of-day buckets (local Porto time == UTC+0/+1; the dataset's TIMESTAMP is
# unix seconds). Boundaries are the conventional commute peaks, not tuned.
TIME_BUCKETS = [
    ("night", 0, 6),          # 00:00-05:59
    ("morning_peak", 6, 10),  # 06:00-09:59
    ("midday", 10, 16),       # 10:00-15:59
    ("evening_peak", 16, 20), # 16:00-19:59
    ("evening", 20, 24),      # 20:00-23:59
]

# --- Method A clustering: MinHash-LSH on directed bigram shingles ---
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
CLUSTER_MIN_SIZE = 3         # ignore clusters smaller than this (noise)
# Fraction of a cluster's members that must contain a cell run for it to be the
# cluster's reported SUB-ROUTE. Reporting the seed's whole trajectory instead
# would answer "which whole trips are similar", which the PDF explicitly excludes.
CLUSTER_SUBROUTE_PCT = 0.6

# --- Method C transition graph: PageRank zones + heavy-path routes ---
GRAPH_MIN_EDGE_SUPPORT = 5   # keep a cell->cell transition only if >= this trips
PAGERANK_ITERS = 15          # power-iteration sweeps
PAGERANK_DAMPING = 0.85      # standard damping factor
GRAPH_MAX_SEEDS = 3000       # top frequent edges used to seed heavy-path walks
GRAPH_MIN_FLOW_PROB = 0.4    # extend a corridor only while >=40% of a cell's flow
                             # continues that way (dominant flow); below => fork/hole
ACTIVITY_ZONES_TOP = 50      # number of activity-zone cells to report

# --- Method D suffix array (exact, scalable) ---
# Suffixes are bucketed by their first SA_PREFIX_CELLS cells. Every occurrence of
# a substring of >= SA_PREFIX_CELLS cells starts at exactly one suffix, and all
# suffixes sharing that prefix land in one partition -> per-partition counting is
# GLOBALLY exact with no cross-partition merge. Substrings shorter than that are
# below the 1 km floor anyway (3 cells ~= 0.6 km at res 9).
SA_PREFIX_CELLS = 3
SA_MAX_CELLS = 200           # truncate each suffix (bounds per-suffix memory)

# --- anomalous-route analysis ---
ANOMALY_PCT = 0.99           # percentile fence for statistical outliers
ANOMALY_METRO_MARGIN = 0.05  # deg beyond metro bbox before a route counts as drift

# --- approximate sketch sizing (all tunable) ---
SKETCH_LG_MAX_K = 16            # frequent-items map size = 2^LG (~49k counters)
CM_HASHES = 5                   # Count-Min depth  (failure prob ~ 2^-5)
CM_LG_BUCKETS = 17             # Count-Min width per row = 2^17
# datasketches sketches use a FIXED internal seed (Count-Min default 9001) so
# they are deterministic AND mergeable across partitions. deserialize() rebuilds
# with that default, so we must not override it. This constant documents that the
# sketches are seeded/deterministic; it is not passed to the CMS constructor.
SKETCH_SEED = 9001             # = datasketches Count-Min default seed
