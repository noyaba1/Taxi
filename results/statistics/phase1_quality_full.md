# Phase 1 Data Quality (full)
_generated: 2026-08-04T06:13:28+00:00_

rows read: 1,710,670 | rows written: 1,663,886 (97.3%)

## Rejection reasons (a trip can fail several rules)
| rule | trips | share |
|---|---|---|
| MISSING_DATA flagged by vendor | 10 | 0.00% |
| fewer than 2 GPS points | 36,510 | 2.13% |
| more than 4000 GPS points | 0 | 0.00% |
| start or end outside Porto metro box | 10,463 | 0.61% |
| duplicate TRIP_ID | 11 | 0.00% |

Trajectory-level corruption (mid-route GPS teleports, impossible speeds,
parked vehicles) is not detectable from endpoints alone; it is flagged in
Phase 2 and excluded before spatial encoding. See the Phase 2 report.
