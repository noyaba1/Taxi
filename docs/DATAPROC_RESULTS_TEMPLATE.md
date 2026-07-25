# DataProc Results Workbook (fill in during the cloud run)

> ⚠️ **Superseded by [DATAPROC.md](DATAPROC.md).** This run-book predates the
> storage-layer fix: it points `OUTPUT_BASE` at the master's `/tmp`, which the
> cluster teardown then destroys, uses `--num-workers 4`, and reads the raw file
> from `train.csv/train.csv`. Kept for its narrative and monitoring detail only —
> follow DATAPROC.md for the commands.


Record real numbers here as each stage completes. Sources: `gcloud dataproc jobs
wait <ID>` (driver output), Spark UI **Stages**/**Executors** tabs, Billing. Leave
a cell `-` if not measurable. After the run, these numbers replace the estimates in
`FINAL_REPORT.md`.

Run date: __________  ·  Operator: __________  ·  Commit run: __________

---

## 1. Cluster & worker configuration

| Field | Value |
|---|---|
| Region | __________ |
| Image version | 2.1-debian12 |
| Master type / count | n2-standard-4 / 1 |
| Worker type / count | n2-standard-4 / ____ |
| vCPUs total | ____ |
| Memory total | ____ GB |
| Init action (h3, datasketches) succeeded? | ____ |
| Cluster create time | ____ min |
| Cluster uptime (create → delete) | ____ min |

## 2. Per-stage execution metrics

| Stage | Input rows | Output rows | Runtime | Shuffle read | Shuffle write | Exec. failures | Retries | Peak mem / spill | CPU util | Partitions | Validation | Notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| clean_data | 1,710,670 | | | | | | | | | | | |
| feature_engineering | | | | | | | | | | | | |
| spatial_encoding | | | | | | | | | | | | |
| route_mining_maximal | | | | | | | | | | | | |
| route_mining_exact | | | | | | | | | | | | |
| route_mining_approx | | | | | | | | | | | | |
| route_mining_clustering | | | | | | | | | | | | |
| route_mining_graph | | | | | | | | | | | | |
| anomaly_analysis | | | | | | | | | | | | |

## 3. Data volumes

| Item | Value |
|---|---|
| Raw trips | 1,710,670 |
| Valid trips (after clean) | ____ ( ___ %) |
| Encoded trips | ____ |
| Window emissions (exact) | ____ |
| Distinct sub-routes | ____ |
| Maximal-frequent routes (X=0.5%) | ____ |

## 4. Method B — frequent sub-routes (per length threshold)

| min_len_km | #routes | top support | median support | longest_km | notes |
|---|---|---|---|---|---|
| 1 | | | | | |
| 3 | | | | | |
| 5 | | | | | |
| 10 | | | | | |
| 20 | | | | | |
| 40 | | | | | |

## 5. Method A — clustering

| Field | Value |
|---|---|
| Trips clustered (sample cap) | ____ of ____ |
| Similarity edges | ____ |
| Clusters (size ≥ 3) | ____ |
| Top cluster size | ____ |
| Longest representative route (km) | ____ |
| Coherence (verifier) | ____ |

## 6. Method C — transition graph + zones

| Field | Value |
|---|---|
| Graph nodes / edges | ____ / ____ |
| Frequent edges (≥5) | ____ |
| Validated corridors | ____ |
| Top corridor support | ____ |
| Activity zones reported | 50 |
| Top zone (cell / lat,lon / PageRank) | ____ |

## 7. Approximate vs exact (Method B)

| min_len_km | precision@100 | recall@100 | overlap@100 | SS support MAE | CMS MAE |
|---|---|---|---|---|---|
| 1 | | | | | |
| 3 | | | | | |
| 5 | | | | | |
| 10 | | | | | |

| Field | Value |
|---|---|
| Exact memory (est.) | ____ MB |
| Approx memory (real) | ____ MB |
| **Memory ratio** | ____ × |
| Exact shuffled records | ____ |
| Approx merged bundles | ____ |
| Deterministic rerun identical? | ____ |

## 8. Anomalies

| Detector | Count | % |
|---|---|---|
| a_speed | | |
| a_idle | | |
| a_distance | | |
| a_shape | | |
| a_drift | | |
| any | | |

## 9. Validation status (verify_* --full)

| Verifier | PASSED / FAILED | Notes |
|---|---|---|
| verify_phase1 | | |
| verify_encoding | | |
| verify_route_mining | | |
| verify_maximal | | |
| verify_approx_mining | | |
| verify_clustering | | |
| verify_graph | | |
| verify_anomaly | | |

## 10. Estimated cloud cost

| Item | Value |
|---|---|
| Cluster uptime | ____ min |
| Machine-hours (5 × uptime) | ____ |
| Compute cost | $ ____ |
| Storage (GCS) | $ ____ |
| **Total spent** | $ ____ / $50 |

## 11. Presentation screenshots collected

- [ ] Cluster config (5 machines)
- [ ] Heavy-mining Stages page (shuffle GB)
- [ ] Executors page (count/memory/GC)
- [ ] Exact-vs-approx driver output (memory ratio)
- [ ] Top-100 route table(s)
- [ ] Routes + activity-zones map
- [ ] A-vs-B-vs-C comparison table
- [ ] Billing spend

## 12. Unexpected observations

| # | Stage | Observation | Impact | Action taken |
|---|---|---|---|---|
| 1 | | | | |
| 2 | | | | |

---

## Lessons learned
-

## Potential improvements
-

## Questions raised during execution
-
