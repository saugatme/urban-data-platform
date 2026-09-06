# Urban Data Integration Platform

A reusable Spark + Delta Lake data engineering platform that ingests heterogeneous
urban datasets (taxi trips, weather, air quality, taxi zones), validates and
standardizes them, and integrates them into a single analytical dataset.

Built for the *Data-intensive Computing* course, Week 1: Build a Generic Urban
Data Integration Platform.

## Project Structure

```javascript
urban-data-platform/
├── config/                  # dataset configs + Spark settings (config over code)
├── data/
│   ├── raw/                 # original files, NEVER modified (gitignored)
│   └── lake/                # all Delta tables (gitignored)
├── src/
│   ├── common/              # reusable platform engine
│   │   └── spark_session.py # one portable Spark+Delta session builder
│   ├── ingestion/           # dataset-specific transforms
│   ├── integration/         # trip enrichment joins
│   └── catalog/
├── scripts/                 # thin entry points (run_ingestion.py, ...)
├── notebooks/               # exploration + scratch work
├── tests/
├── reports/                 # design + benchmark reports (deliverables)
└── docs/
    ├── data_catalog.md      # Task 1.1 deliverable
    ├── common_data_model.md # Task 1.4 deliverable
    └── setup_log.md         # full environment setup history (read this first if stuck)
```

## Setup (new teammate — ~15 min)

1. **Clone the repo**

```bash
   git clone https://github.com/saugatme/urban-data-platform.git
   cd urban-data-platform
```

2. **Python environment** (Python 3.11 or 3.12 — NOT 3.13, PySpark 3.5 is incompatible)

```bash
   uv venv --python 3.12
   .venv\Scripts\activate        # Windows
   uv pip install -r requirements.txt
```

3. **Java 17** — verify with `java -version`. If missing, install
[Eclipse Temurin 17](https://adoptium.net).
4. **Hadoop Windows helpers** (Windows only — Linux/Mac/WSL skip this):

- Download `winutils.exe` and `hadoop.dll` from
https://github.com/steveloughran/winutils (hadoop-3.0.0/bin)
- Place both in `C:\hadoop\bin\`
- Create a file `.env` in the project root (copy from `.env.example`):

```javascript
     HADOOP_HOME=C:\hadoop
```

- Details and the full troubleshooting history: see `docs/setup_log.md`

5. **Verify it works** — run `notebooks/test_spark.ipynb` top to bottom.
Expected output: `read back: 3` / `OK`.
6. **Download the datasets** from the course page into `data/raw/`
(paths in `config/datasets.yaml` assume they live there).

## How to Run

> Entry points land here as Week 1 tasks are implemented (`scripts/run_ingestion.py`, etc.)

## Team Workflow

- Never commit to `main`. One branch per task: `git checkout -b task-N-name`
- Commit early, push daily, merge via Pull Requests
- `data/` and `.env` are gitignored — every teammate downloads datasets locally
- Do not modify anything in `data/raw/` — the pipeline reads from it