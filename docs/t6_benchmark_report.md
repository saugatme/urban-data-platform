# Benchmark Report

## Method

The benchmark writes the silver taxi-trip table twice: once partitioned by `year` and `month`, and once without partitions. It measures write time, storage size, Parquet-file count, and the required query latencies. Each query is run once to warm the environment and once for the reported measurement.

## Results

| Metric | Partitioned by year and month | Flat |
|---|---:|---:|
| Write time | 30.5 s | 8.9 s |
| Storage size | 347.3 MB | 347.4 MB |
| Parquet files | 30 | 18 |
| Trips per borough | 0.94 s | 0.89 s |
| Average duration per day | 0.97 s | 1.02 s |
| Average fare per borough | 0.84 s | 0.95 s |

## Discussion

The flat table writes substantially faster and creates fewer files. Storage size is effectively the same for both designs.

The three required queries are full scans because none filters by `year` or `month`. As a result, partition pruning is not used. The flat table is slightly faster for trips per borough, while the partitioned table is slightly faster for average duration per day and average fare per borough. The differences are small.

The partitioned table remains useful for future time-filtered queries and larger data volumes. At the current scale, its main cost is slower writing and more files rather than a clear advantage for the required full-scan queries.
