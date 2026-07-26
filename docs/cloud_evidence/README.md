# DataProc run evidence

Captured 2026-07-26 **while the cluster was alive**. This folder is tracked in git
(unlike `outputs/`, which is generated and ignored) because none of it can be
regenerated: `scripts/dataproc_submit.sh` deletes the cluster on exit — that is
what stops the billing — so the machine list exists only during the run.

| file | what it evidences |
|---|---|
| `cluster_size.txt` | 6 machines (1 master + 5 workers), each VM named and RUNNING |
| `cluster_describe.yaml` | full cluster spec: image `2.2.84-debian12`, `n2-standard-4`, disks, properties |

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

## Caveat on two reports

`temporal_analysis` and `method_comparison` in GCS were produced BEFORE two fixes
landed (session timezone pinned to `Europe/Lisbon`; the cross-method support
caveat). The corrected versions are the local ones under `outputs/statistics/`.
Nothing else is affected — no other stage reads hour-of-day.
