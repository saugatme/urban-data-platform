# Week 1 — Urban Data Integration Platform

Week 1 creates the foundation used by the rest of the project: validated Bronze tables, standardized Silver tables, and the integrated Gold taxi-trip table.

## Documents

| Document | Description |
|---|---|
| [Architecture](architecture.md) | Platform layers and data flow |
| [Data catalog](data_catalog.md) | Source entities, keys, joins, and growth characteristics |
| [Storage architecture](storage_architecture.md) | Storage-layer design and partitioning choices |
| [Common data model](common_data_model.md) | Standardized model and field conventions |
| [Integration pipeline](integration_pipeline.md) | Gold-table joins and enrichment logic |
| [Ingestion framework](ingestion_framework.md) | Shared ingestion, validation, rejection, and metadata flow |
| [Benchmark report](benchmark_report.md) | Partitioned versus flat storage comparison |

Run the Week 1 pipeline from the project root:

```bash
python run_ingestion.py
python run_integration.py
python run_benchmark.py
```
