# Data Catalog — Week 1

Exploration evidence gathered 2026-09-08 (pandas peek + Spark schema/rowcount in
`notebooks/exploration/`). Row counts below are measured, not assumed.

## 1. taxi_trips (3 Parquet files, Jan–Mar 2024)

- **Rows:** 9,554,778 (≈3.2M/month) · **Files:** `yellow_tripdata_2024-01/02/03.parquet`
- **Entity:** one row = one completed yellow taxi trip (NYC TLC data).
- **Primary key:** **none** — no trip ID column. Uniqueness cannot be enforced; dedup must use the full row or a composite (pickup, dropoff, PULocationID, DOLocationID, fare). This is a known TLC data property.
- **Join attributes:** `PULocationID`, `DOLocationID` → `taxi_zones.LocationID`; `tpep_pickup_datetime` (truncated to hour) → weather / air_quality hour.
- **Temporal:** `tpep_pickup_datetime`, `tpep_dropoff_datetime` (Spark read them as `timestamp_ntz`). Range to be confirmed in Week 1 DQ checks; files are calendar months → natural partition key is pickup month.
- **Categorical:** `VendorID` (1,2), `RatecodeID`, `payment_type`, `store_and_fwd_flag` (Y/N).
- **Growing?** Yes — monthly files append ~3M rows/month; source is published monthly.

## 2. weather (CSV, hourly)

- **Rows:** 8,784 = 366 days (2024 is a leap year) × 24 hours · **Dupe check on (year,month,day,hour): 0**
- **Entity:** one row = one hourly weather observation at a **single implicit NYC station** (no station/lat/lon columns — location is implicit, confirmed by peek).
- **Primary key:** composite `(year, month, day, hour)` — verified duplicate-free.
- **Join attributes:** hour — must first assemble a single timestamp from the 4 columns (planned in common data model: `timestamp_utc`/`timestamp_local` convention). Joins to trips on pickup hour.
- **Temporal:** year/month/day/hour columns, full year 2024, **24 observations per day for every day (no gaps)** — cleanest as-of join target.
- **Categorical:** none meaningful; `*_source` columns are provenance tags (isd_lite, dwd_mosmix); `coco` is a weather-condition code.
- **Growing?** No — static extract for 2024; would grow hourly only if refreshed from source.
- **Missing values:** `snwd`, `wpgt` often NaN in peek → common data model must define a missing-value convention.

## 3. air_quality (CSV, hourly)

- **Rows:** 8,139,551 · **Sites:** 925 unique (State Code, County Code, Site Num) — **nationwide, not just NYC** · **Date range:** 2024-01-01 → 2024-12-31
- **Entity:** one row = one hourly PM2.5 reading (`Parameter Code 88101`) from one monitor.
- **Primary key:** composite `(State Code, County Code, Site Num, POC, Date Local, Time Local)` — verified 0 duplicates.
- **Join attributes:** time (`Date Local` + `Time Local` → hour) **and space** (Latitude/Longitude / county). Joining to NYC trips requires first selecting NYC-area site(s); a plain time-only join would be ambiguous across 925 sites. Candidate approach: pick the site(s) in New York County / closest to Manhattan — decision deferred to Week 1 task 5.
- **Temporal:** `Date Local`/`Time Local` (local) and `Date GMT`/`Time GMT` — must standardize to one convention (common data model) to join with NYC-local taxi timestamps and weather.
- **Categorical:** `State Name`, `County Name`, `Method Type`, `Parameter Name`, `Qualifier` (mixed dtype — DtypeWarning on load, treat as string).
- **Growing?** Yes — hourly, appends continuously at source (EPA AQS).
- **Nulls:** 0 nulls in `Sample Measurement`, `Time Local` in measured sample (full scan in Week 3 DQ).

## 4. taxi_zones (CSV, lookup)

- **Rows:** 265 · **Dupes on LocationID: 0** · **Null LocationID: 0**
- **Entity:** one row = one TLC taxi zone (geographic area).
- **Primary key:** `LocationID` (integer, 1–265) — verified unique, no nulls.
- **Join attributes:** `LocationID` ← `taxi_trips.PULocationID` / `DOLocationID`.
- **Temporal:** none — slowly changing reference data (TLC occasionally adds zones).
- **Categorical:** `Borough` (Bronx, Brooklyn, EWR, Manhattan, Queens, Staten Island, **Unknown**), `Zone` (free text), `service_zone` (EWR, Boro Zone, Yellow Zone, …).
- **Growing?** Effectively no — static for this project; treat as dimension/lookup table.
- **Known DQ issue:** exactly **1 row has null Borough and null Zone** (the "Unknown" zone, LocationID 265) — trips referencing it must survive integration (left join), not be dropped.