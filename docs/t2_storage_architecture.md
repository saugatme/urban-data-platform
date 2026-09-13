# Storage Architecture

```text
data/
  raw/       original source files
  bronze/    validated Delta tables
  silver/    common-model Delta tables
  gold/      integrated_taxi_trips Delta table
  rejected/  rejected rows with a reason
  metadata/  ingestion_log Delta table
  benchmark/ storage-strategy outputs
```

All table and column names use `snake_case`. Taxi trips, air quality, and the integrated table are partitioned by `year` and `month`. Weather and taxi zones are unpartitioned because they are small. Taxi zones are the lookup table.

Partitioning is useful for large, time-filtered tables. It is harmful for small tables, high-cardinality keys, and queries that read every partition. At higher volume, compact files first and introduce daily partitions only when measurements show that monthly partitions are too large.