# Ingestion Framework

The framework uses one pipeline for every dataset.

```text
load -> rename -> schema check -> timestamp parsing -> validation -> Delta write -> metadata
```

The configuration for each dataset supplies the path, format, required columns, primary key, timestamp columns, partitioning, schema version, and validity rule. The shared pipeline supports CSV and Parquet input, required-schema checks, configured timestamp parsing, duplicate-key checks, Delta output, rejected-row output, and ingestion metadata.

Generic checks reject missing primary keys, invalid timestamps, and duplicate primary keys. Dataset rules reject invalid trip distances, fares, passenger counts, weather time values, missing air-quality measurements, and zone IDs. A rejected row has a reason and does not stop the rest of the pipeline. Each rejected-row table represents the latest run; the metadata log retains the run history. Taxi trips have no reliable key, so duplicate-key validation is not claimed for that source.

The metadata table records processed, accepted, and rejected records, execution time, ingestion time, layer, and schema version. Adding a dataset normally requires configuration and a small validity rule, not a new ingestion script.
