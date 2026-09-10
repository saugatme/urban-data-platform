# Task 4: Common Data Model

## Overview

The four datasets arrive with different timestamp formats, column naming conventions, data types, and missing value patterns. The Common Data Model (CDM) defines a single standard that all datasets must conform to before integration. It is enforced in the silver layer, which reads from bronze and applies standardization transformations.

---

## Standards Defined

### 1. Timestamp Format

All datetime values are stored as Spark `timestamp` type, representing `yyyy-MM-dd HH:mm:ss` in UTC or local time as appropriate.

| Dataset | Raw Format | Standardized |
|---|---|---|
| Taxi Trips | `tpep_pickup_datetime` as string | `pickup_datetime` as `timestamp` |
| Air Quality | `Date Local` as date string, `Time Local` as `HH:mm` string | `date_local` as `timestamp`; broken `time_local` dropped |
| Weather | Separate `year`, `month`, `day`, `hour` integer columns | Kept as integers; combined at join time |
| Taxi Zones | No temporal attributes | No change |

**Fix applied:** In the air quality dataset, `time_local` and `time_gmt` were incorrectly cast to `timestamp` because they contain time-only strings (`HH:mm`). These columns were dropped in silver. The date columns (`date_local`) are sufficient for hourly joins.

---

### 2. Naming Conventions

All column names follow snake_case. Transformations are applied in the bronze layer via explicit rename maps.

Examples:

| Original | Standardized |
|---|---|
| `VendorID` | `vendor_id` |
| `tpep_pickup_datetime` | `pickup_datetime` |
| `PULocationID` | `pickup_location_id` |
| `Sample Measurement` | `sample_measurement` |
| `LocationID` | `location_id` |

---

### 3. Missing Value Rules

| Column | Dataset | Rule | Reason |
|---|---|---|---|
| `snwd` (snow depth) | Weather | Dropped | 100% null — no data recorded |
| `snwd_source` | Weather | Dropped | 100% null |
| `wpgt` (wind gust) | Weather | Dropped | 100% null — no data recorded |
| `wpgt_source` | Weather | Dropped | 100% null |
| `uncertainty` | Air Quality | Dropped | 100% null |
| `precipitation` | Weather | Filled with `0.0` | No precipitation recorded = 0, not unknown |
| `condition_code` | Weather | Filled with `-1` | Sentinel value for unknown condition |
| `qualifier` | Air Quality | Kept as null | Present in 8% of rows; meaningful when populated |
| `time_local`, `time_gmt` | Air Quality | Dropped | Incorrectly typed; date column is sufficient |

**Rule:** Columns that are 100% null carry no information and are dropped. Columns with partial nulls are filled with domain-appropriate defaults (0 for measurements, -1 for unknown codes) or left as null if the absence itself is meaningful.

---

### 4. Common Data Types

| Type | Standard | Applied To |
|---|---|---|
| Datetime | `timestamp` | All pickup/dropoff/observation times |
| Categorical IDs | `integer` | `vendor_id`, `payment_type`, `rate_code_id`, `passenger_count`, `location_id` |
| Measurements | `double` | `fare_amount`, `trip_distance`, `temp`, `sample_measurement` |
| Codes | `integer` | `condition_code`, `state_code`, `county_code` |
| Labels | `string` | `borough`, `zone`, `state_name`, `county_name` |

**Note:** Numeric columns that represent categories (e.g. `vendor_id = 2` means Vendor 2, not the number two) are cast to `integer`, not `double`. They will never be averaged or summed.

---

## Transformations by Dataset

### Taxi Trips
- `rate_code_id`, `payment_type`, `passenger_count` cast from `long` to `integer`
- All other types already correct from Parquet schema

### Weather
- Dropped: `snwd`, `snwd_source`, `wpgt`, `wpgt_source`
- `precipitation` nulls filled with `0.0`
- `condition_code` nulls filled with `-1`

### Air Quality
- Dropped: `uncertainty`, `time_local`, `time_gmt`, `date_gmt`
- `sample_measurement` nulls filled with `0.0`
- `time_local` and `time_gmt` were dropped. This means hourly AQ joins are not possible — addressed in Task 5 by aggregating to daily average PM2.5.

### Taxi Zones
- No changes required — already clean and correctly typed

---

## Silver Layer Results

| Dataset | Bronze Rows | Silver Rows | Cols Removed |
|---|---|---|---|
| Taxi Trips | 8,480,870 | 8,480,870 | 0 |
| Weather | 8,784 | 8,784 | 4 |
| Air Quality | 8,139,551 | 8,139,551 | 3 |
| Taxi Zones | 265 | 265 | 0 |

No rows are lost in the silver transformation — only columns are dropped or filled.

---

## Implications for Integration

The CDM ensures that when Task 5 joins these datasets:
- All timestamps are the same type and can be compared directly
- All column names are unambiguous and consistent
- Null values have defined semantics — no silent errors during joins
- Categorical columns will not be accidentally treated as numeric features