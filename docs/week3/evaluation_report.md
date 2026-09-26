# Platform Evaluation

## Update Results

| Dataset | Processed | Inserted | Rejected | Time |
|---|---:|---:|---:|---:|
| Taxi trips | 517,165 | 508,675 | 8,490 | 104.352 s |
| Weather | 168 | 168 | 0 | 6.581 s |
| Air quality | 168 | 168 | 0 | 10.701 s |

The taxi rejection count is expected: all 8,490 rejected records were intentionally copied duplicates. Weather and air quality accepted every record.

---

## Validation Cost

| Dataset | Validation time |
|---|---:|
| Taxi trips | 20.042 s |
| Weather | 0.751 s |
| Air quality | 0.881 s |

Duplicate detection is the main extra cost because taxi has no stable source identifier. The pipeline uses a hash of shared taxi fields to prevent repeated release records from being inserted.

---

## Product Refresh Time

| Product | Time |
|---|---:|
| daily_mobility | 9.119 s |
| taxi_zone_statistics | 5.903 s |
| weather_impact | 5.982 s |
| air_quality_impact | 4.649 s |

Daily mobility takes longest because it groups trips by both day and pickup zone. All products refreshed because the release changed taxi, weather, and air-quality data.

---

## Storage

| Location | Size |
|---|---:|
| Bronze | 1.63 GB |
| Silver | 2.83 GB |
| Gold | 2.47 GB |
| Update files | 23.73 MB |
| Rejected rows | 24.78 MB |
| Monitoring log | 93.91 KB |

The update files and monitoring log are small compared with the analytical layers. The rejected-row area is retained as audit evidence and can be reviewed without scanning the main tables.

---

## Conclusion

The platform accepted the second release without rebuilding unchanged source data. It inserted only new records, isolated duplicate rows, accepted the two documented schema additions, refreshed the four dependent products, and kept a queryable record of every operation. For a larger deployment, stable taxi-trip IDs and month-level product refreshes would reduce duplicate-check and full-refresh work further.
