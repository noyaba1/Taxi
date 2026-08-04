# DataProc run evidence

Captured 2026-07-26 **while the cluster was alive**. This folder is tracked in git
(unlike `outputs/`, which is generated and ignored) because none of it can be
regenerated: `scripts/dataproc_submit.sh` deletes the cluster on exit — that is
what stops the billing — so the machine list exists only during the run.

| file | what it evidences |
|---|---|
| `cluster_size.txt` | 6 machines (1 master + 5 workers), each VM named and RUNNING |
| `cluster_describe.yaml` | full cluster spec: image `2.2.84-debian12`, `n2-standard-4`, disks, properties |

## The 2026-08-04 run (`porto-final`) — the one the results come from

The July run above predates gap densification, so its route tables have been
superseded (FINAL_REPORT §9e). The `*_20260804.*` files evidence the run that
produced the submitted `results/` bundle: same shape, 1 master + 5 workers of
`n2-standard-4` in `europe-west1-c`.

| file | what it evidences |
|---|---|
| `cluster_size_20260804.txt` | 6 machines, each VM named and RUNNING |
| `cluster_describe_20260804.yaml` | the spec, **including the lifecycle guards** |

The lifecycle fields are worth reading, because they are GCP attesting to the
budget controls rather than us asserting them:

```yaml
idleDeleteTtl:  1800s                       # --max-idle 30m
autoDeleteTime: '2026-08-04T10:08:31.151457Z'   # --max-age 4h, from 06:08:31Z
```

`--max-age` exists because the other two guards each have a hole: the EXIT trap
covers a clean exit but not a killed shell or a slept laptop, and `--max-idle`
only fires when the cluster is IDLE — a job that HANGS is neither, and six VMs
would bill until someone noticed.

Reproduce during a future run, from a second terminal:

```bash
bash scripts/cloud_status.sh evidence
```

## The run these came from

* project `finalproj-noyabayazi`, region `europe-west1`, bucket
  `gs://taxi-project-noyabayazi`, prefix `taxi`
* scale `--full`: 1,710,670 trips read from GCS
* 60 min wall, 50.5 min of job time across 13 stages
* every input read from and every output written to `gs://` — no local disk

## Correctness

`python -m src.verify_cloud_run --cloud-dir ./cloud_outputs --scale full` →
`CLOUD RUN VERIFICATION PASSED`.

The load-bearing check is Method D: a suffix array with no sampling, no seeds and
no hash-order dependence, so its output is a function of the input alone.

```
Method D corridor set matches   -> cloud=420 baseline=420 shared=420
Method D supports identical     -> 420 identical
trip count matches the baseline -> cloud=1,614,508 baseline=1,614,508
```

420 corridors reproduced exactly across a different machine count, a different
partitioning and a different filesystem. Activity zones matched 50/50 including
PageRank floats; Phase-1 cleaning matched on all five rejection counts.

Methods A and the M7 sketches are expected to differ slightly and are reported,
not failed: A samples trips and uses unseeded MinHash-LSH, and sketch merge order
is partition-dependent.

## Two reports were regenerated after the run

`temporal_analysis` and `method_comparison` were first produced before two fixes
landed, and were re-run on a fresh cluster against the same parquet afterwards:

* **session timezone.** `F.hour(F.from_unixtime(...))` renders in
  `spark.sql.session.timeZone`, which defaulted to the JVM's machine timezone.
  The same 1,614,508 trips bucketed differently on a laptop (UTC+3) and on
  DataProc (UTC) — totals identical, assignment shifted. Neither was Porto.
  Pinned to `Europe/Lisbon` in `config.DATASET_TIMEZONE`. This changed the
  report's *conclusion*, not just its numbers: mean overlap 0.78 -> 0.81 moved
  the derived verdict from "an average with a real caveat" to "the corridors are
  structural".
* **cross-method support.** `top_support` was presented as comparable across
  methods. B and D emit only maximal sub-routes; A and C do not, so A can report
  a short frequent PREFIX that D suppresses as redundant. The report now says so
  and shows the measurement.

Re-run with:

```bash
ONLY="temporal_analysis.py evaluation.py" bash scripts/dataproc_submit.sh
```

`ONLY` restricts the submit list, so a corrected report costs one short cluster
rather than the whole pipeline. No other stage reads hour-of-day, and no mining
output changed.
