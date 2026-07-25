# Running on GCP DataProc

**This is the canonical cloud document.** The other run-books in `docs/`
(`CLOUD_RUN_PLAYBOOK`, `PREFLIGHT`, `CLOUD_CHECKLIST`, `RELEASE_AUDIT`,
`LIVE_MONITORING_GUIDE`) predate the storage-layer fix and are kept for their
narrative and monitoring detail. Where they disagree with this file, this file is
right.

---

## What changes between local and cloud

Nothing in the code. Four environment variables:

| var | local | cloud |
|---|---|---|
| `SPARK_ENV` | unset (`local[*]`) | `cloud` — YARN provides the master |
| `DATA_BASE` | `./data` | `gs://<bucket>/porto` |
| `OUTPUT_BASE` | `./outputs` | `gs://<bucket>/porto/outputs` |
| `RAW_TRAIN` | auto-resolved | `gs://<bucket>/porto/raw/train.csv` |

`config.storage_join` keeps the `gs://` scheme intact for data paths, and
`src/storage.py` routes every report and result CSV through Hadoop's FileSystem
when the path has a URI scheme.

> **Why `OUTPUT_BASE` must be a `gs://` path.** An earlier version pointed it at
> `/tmp/porto_out` on the master, because the writers used plain `open()`. The
> submit script deletes the cluster on exit — so every top-100 CSV it had just
> spent an hour computing was destroyed with the node. Results now go straight to
> GCS and survive teardown.

---

## Step 0a — local dress rehearsal (free, offline, ~4 min)

```bash
bash scripts/cloud_rehearsal.sh
```

Runs the whole pipeline with `DATA_BASE`/`OUTPUT_BASE` behind a `file://` URI.
That is a real Hadoop FileSystem with a URI scheme, so it exercises the *same*
code path as `gs://` — without credentials, a bucket, or a cent.

Not hypothetical: the first run of this found three blockers that local testing
could never surface — `evaluation` and `visualization` had no SparkSession to
reach a remote path with (both produce deliverables), `verify_anomaly` read its
CSV with a bare `open()`, and `validate_holdout`'s input was never uploaded.

## Step 0b — dry run (free, do this first)

```bash
PROJECT=my-project BUCKET=gs://my-bucket DRY_RUN=1 bash scripts/dataproc_submit.sh
```

Checks every precondition that can be checked without spending anything:
gcloud/gsutil present, credentials active, project reachable, Dataproc and
Storage APIs enabled, bucket exists, both local input files resolve, and the
`src` package imports. It creates nothing. Almost every way a cloud run fails
before it starts is caught here.

## One command

```bash
PROJECT=my-project BUCKET=gs://my-bucket bash scripts/dataproc_submit.sh
```

It uploads code and data, creates 1 master + **5 workers**, submits every stage
against `gs://` paths, and deletes the cluster on any exit (success, failure or
Ctrl-C).

### Do a cheap rehearsal first

```bash
PROJECT=… BUCKET=… SCALE=--sample WORKERS=2 bash scripts/dataproc_submit.sh
```

This runs the whole pipeline on 5,000 trips on a 2-worker cluster. It costs
cents, takes minutes, and proves the things most likely to be wrong:
credentials, the GCS paths, the `pip` init action, and that results actually land
in `gs://…/outputs/routes/`. Only then run `--full`.

---

## Cost control

- `--max-idle 30m` plus a `trap … EXIT` that deletes the cluster unconditionally.
- 6 × `n2-standard-4` ≈ $1.20–1.50/hour in `europe-west1`. A full run fits
  comfortably inside a $50 budget **provided** you do not run the quadratic
  baseline (below).
- **Never debug in the cloud.** `--sample` for correctness, `--mid` for shuffle
  behaviour — both free on a laptop.

---

## Which stages run at `--full`, and why

`scripts/dataproc_submit.sh` submits:

```
clean_data · feature_engineering · summarize_features · spatial_encoding
route_mining_suffix_array · route_mining_clustering · route_mining_graph
anomaly_analysis · evaluation · visualization
route_mining_approx --approx-only          <- last: expensive, not a deliverable
```

Two deliberate omissions at `--full`:

**The window-enumeration family** — `route_mining_exact`, `route_mining_closed`,
`route_mining_maximal` — all build the same O(n²) support table:

| | 4,745 trips | 188,761 trips |
|---|---|---|
| windows emitted | 916,815 | 33,597,872 |
| exact miner | 7.5 s | **OOM (Java heap)** |
| maximal-frequent | 32.7 s | **21 GB spilled, unfinished** |
| suffix array | 9.0 s | **41 s** |

**`--compare-grids`** — the grid/conflation sweep re-encodes the whole dataset
once per candidate (H3 8/9/10 + geohash 6/7). Five extra full passes to justify a
design choice whose answer is the same on the sample, where it already ran.

At 1.71M trips that is order 10⁸–10⁹ rows and >100 GB of shuffle. The **suffix
array is the exact path at scale** and gives identical supports (verified: 0
disagreements over every shared route). `route_mining_exact` also refuses to
start above `EXACT_MAX_TRIPS` rather than failing an hour in.

Both baselines still run under `SCALE=--sample`, which is where the accuracy
comparison against the sketches belongs.

---

## Cluster dependencies

`h3`, `datasketches` and `python-geohash` are not on a stock DataProc image; the
script installs them on every node via the `pip-install.sh` initialization
action. `numpy`/`pandas`/`pyarrow` are preinstalled on image 2.1, so `pandas_udf`
works out of the box.

---

## Retrieving results

They are already in GCS:

```bash
gsutil ls -r "gs://<bucket>/porto/outputs/routes/"
gsutil -m cp -r "gs://<bucket>/porto/outputs" ./cloud_outputs/
```

Parquet tables live under `gs://<bucket>/porto/processed/`
(`trips_clean_full.parquet`, `trips_features_full.parquet`,
`trips_encoded_r9_full.parquet`). Every stage also prints its headline numbers,
captured in the Dataproc driver output on GCS.

For the Colab notebook, point `BASE` at `gs://<bucket>/porto/outputs/routes` and
set `SCALE = 'full'`.

---

## Checks along the way

| after | check |
|---|---|
| upload | `gsutil du -h "$BUCKET/porto/raw/train.csv"` ≈ 1.9 GiB |
| clean | `gsutil ls "$D/processed/trips_clean_full.parquet/_SUCCESS"`; driver prints valid-trip % |
| encoding | driver prints `dropped N anomalous` and `trips still containing a hop > … : 0` |
| suffix array | driver prints suffixes indexed and maximal routes found |
| end | `gsutil ls "$D/outputs/routes/"` lists a `*_top100_full.csv` per method |

If a stage fails the cluster is deleted by the trap, so read the driver output
from GCS rather than expecting to SSH in.

---

## Troubleshooting

| symptom | cause | fix |
|---|---|---|
| `FileNotFoundException` in `clean_data` | `RAW_TRAIN` unset or upload incomplete | `gsutil ls "$D/raw/train.csv"` |
| `ModuleNotFoundError: h3` on executors | init action did not run | check cluster-creation logs and the `--metadata PIP_PACKAGES` line |
| results not in GCS | `OUTPUT_BASE` not a `gs://` path | the script sets it; check the job's `--properties` |
| OOM in a mining stage | quadratic baseline slipped into a large run | it is not submitted at `--full`; check the stage list |
| job succeeds but tables are empty | stale parquet from an interrupted run | delete `$D/processed/` and rerun from `clean_data` |
