# Running on GCP DataProc (5+ machines) over GCS

The final run must execute on **DataProc with ≥5 machines** reading from **GCS**.
The code is already cloud-ready: the only switch is two environment variables.
**Execution and the $50 budget are the group's to spend — this doc gives the exact
commands and a cost-safe procedure.**

---

## 0. The cloud switch (no code changes)

| Concern | Local | DataProc |
|---|---|---|
| Spark master | `local[*]` (set in `spark_session.py`) | `SPARK_ENV=cloud` → YARN provides it |
| Data location | `data/` folder | `DATA_BASE=gs://<bucket>/porto` |
| Path safety | — | `config.storage_join` keeps `gs://` intact (pathlib would break it) |
| Windows shims | active | `os.name != 'nt'` → **no-op** |

So on the cluster: `export SPARK_ENV=cloud` and `export DATA_BASE=gs://<bucket>/porto`.

---

## 1. Prerequisites

```bash
gcloud config set project <PROJECT_ID>
export BUCKET=gs://<your-bucket>
export REGION=europe-west1          # near Portugal; cheap
gsutil mb -l $REGION $BUCKET        # once
```

## 2. Upload the data + code

```bash
# raw data (once): the ~1.9 GB file
gsutil -m cp "train.csv/train.csv" $BUCKET/porto/train.csv/train.csv
# code: zip the src package so jobs can import it
cd <repo> && zip -r src.zip src -x "*/__pycache__/*"
gsutil cp src.zip $BUCKET/code/src.zip
```

## 3. Create a 5-machine cluster (cost-safe)

```bash
gcloud dataproc clusters create porto \
  --region $REGION \
  --master-machine-type n2-standard-4 --num-masters 1 \
  --worker-machine-type n2-standard-4 --num-workers 4 \
  --image-version 2.1-debian12 \
  --max-idle 30m \                # AUTO-DELETE if idle -> protects the budget
  --properties spark:spark.sql.adaptive.enabled=true
```

1 master + 4 workers = **5 machines** in one Spark cluster. `--max-idle` auto-
deletes it so a forgotten cluster can't drain the $50.

## 4. Submit the pipeline (each stage = one PySpark job)

Each stage reads Parquet from the previous one. Submit in order (see
`scripts/dataproc_submit.sh` for a loop):

```bash
submit () {  # $1 = module file under src/
  gcloud dataproc jobs submit pyspark src/$1 \
    --cluster porto --region $REGION \
    --py-files $BUCKET/code/src.zip \
    --properties spark.yarn.appMasterEnv.SPARK_ENV=cloud,\
spark.yarn.appMasterEnv.DATA_BASE=$BUCKET/porto,\
spark.executorEnv.SPARK_ENV=cloud,spark.executorEnv.DATA_BASE=$BUCKET/porto \
    -- --full
}
submit clean_data.py
submit feature_engineering.py
submit spatial_encoding.py
submit route_mining_maximal.py      # Method B (PDF definition)
submit route_mining_clustering.py   # Method A
submit route_mining_graph.py        # Method C + zones
submit anomaly_analysis.py
submit route_mining_exact.py        # exact + approx comparison (M5/M7)
submit route_mining_approx.py
```

Parquet outputs land in `$BUCKET/porto/processed/`. The small `.md`/`.csv`
reports are written to the **driver** local disk; copy them up:
`gsutil -m cp -r /tmp/outputs $BUCKET/porto/outputs` (or set `OUTPUT_BASE=$BUCKET/...`
for the CSV route files, which Spark-free `open()` cannot write to gs:// — keep
those local and `gsutil cp`).

## 5. Delete the cluster (do this the moment you finish)

```bash
gcloud dataproc clusters delete porto --region $REGION -q
```

## 6. Budget guidance ($50)

- n2-standard-4 ×5 ≈ **$1/hour** total; a full pipeline pass is well under an hour.
- Debug on the **local 5k sample** (free) — never debug on the cluster.
- Use `--max-idle`, delete promptly, run the full job **once** for the final
  numbers. Realistic spend: a few dollars.

## 7. Validate cloud results by PARITY

The full-run top routes/zones must match the local sample's *shape* (same downtown
corridors, same top activity zones), with larger support counts. Run the local
verifiers' logic on the cloud outputs, or eyeball the map notebook against gs://.

---

## 8. Scale-hardening checklist (apply right before the full/cloud run)

These were **deliberately NOT applied to the sample pipeline** — on 5k trips they
give no measurable benefit and would destabilise validated code (see the M8.1
decision in the final report). Apply them when the full-scale cost is real and
**measure the delta**:

1. **Persist `cum_km` in encoding.** `route_mining_exact._subroutes` recomputes
   `h3.point_dist` per trip. Store a per-trip `cum_km: array<double>` in
   `spatial_encoding` and pass it in, removing ~29M H3 calls/mining-run at full
   scale. *(Also fixes the Phase-4 doc note.)* Low risk, re-run the 4 mining
   verifiers after.

2. **Hash the group key.** The exact miner shuffles ~260M window rows keyed by
   `>`-joined hex strings (~640 B). Group by `F.xxhash64(subroute)` and carry the
   string only for the surviving ≤600 top routes (join it back), cutting shuffle
   bytes ~40–80×. 64-bit collision prob over 810k keys is negligible.

3. **Skew.** Downtown keys are hot. Rely on `spark.sql.adaptive.skewJoin` (AQE is
   on) and, if needed, salt the hottest keys in a two-stage aggregation — or use
   the M7 sketches (no per-key reducer) as the scalable path.

4. **Partitions.** Raise `spark.sql.shuffle.partitions` (200–400 on the cluster;
   `config.LOCAL_SHUFFLE_PARTITIONS`=16 is a laptop value). Repartition the
   encoded table before the big groupBy.

5. **Clustering / graph at scale.** M9 collects the pruned edge list to the driver
   (capped by `EDGE_COLLECT_CAP`); M10 collects frequent edges for the walks. At
   1.71M, replace with distributed GraphFrames LPA/Louvain (A) and Pregel/beam (C).
   PageRank already runs distributed.

Expected impact is documented; **numbers to be filled in from the cloud run.**
