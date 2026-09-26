# Platform Evaluation

The evaluation script reads the monitoring log and measures update runtime, product-refresh runtime, storage, validation time, and one monitoring-table append.

## Final results

| Measurement | Result |
|---|---:|
| Taxi update | 104.352 s |
| Taxi validation | 20.042 s |
| Weather update | 6.581 s |
| Weather validation | 0.751 s |
| Air-quality update | 10.701 s |
| Air-quality validation | 0.881 s |
| Daily mobility refresh | 9.119 s |
| Other product refreshes | 4.649–5.982 s |
| Monitoring write probe | 22.662 s |

## Storage

| Location | Size |
|---|---:|
| Bronze | 1.63 GB |
| Silver | 2.83 GB |
| Gold | 2.47 GB |
| Update files | 23.73 MB |
| Rejected rows | 24.78 MB |
| Monitoring log | 93.91 KB |

Taxi dominates the cost because its release has over half a million rows. The weather and air-quality releases are small hourly batches, so their validation and insertion costs remain low. The monitoring table is very small relative to data layers.

## Run

```powershell
scripts\operations\evaluate.py
```

The result is saved to `data/benchmark/operations_evaluation.json`.
