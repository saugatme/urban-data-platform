# Task 4 — Data Engineering for Machine Learning

The comparison script measures two implementations of trip-duration model preparation:

```powershell
python scripts/ml/compare_workflows.py
```

| Workflow | Preparation required |
|---|---|
| Approach A — raw datasets | Load Parquet/CSV sources; rename columns; validate; construct timestamps; aggregate PM2.5; join weather, air quality, and zones; then train. |
| Approach B — integrated platform | Load validated Gold Delta data; derive ML features; then train. |

Recorded measurements were 50.09 seconds for the raw workflow and 35.19 seconds for the integrated-platform workflow: a 29.8% reduction. The platform also removes duplicated and error-prone implementation work. For example, the raw workflow initially produced all-null PM2.5 values because its air-quality timestamp did not match the source format; the established Silver transformation already handles this correctly.

The comparison is directional rather than a strict identical-row benchmark because Week 3 adds incremental records to the Gold table while Approach A begins with the original raw release. It nevertheless demonstrates the reuse, reproducibility, and lower preprocessing burden provided by the data platform.
