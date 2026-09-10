# Task 1: Data Catalog

## Definitions

| Term | Definition |
|---|---|
| Primary Entity | The real-world object or event represented by one row |
| Primary Key | Column(s) that uniquely identify one row |
| Join Attributes | Columns used to connect this dataset to another |
| Temporal Attributes | Columns representing when something occurred or was recorded |
| Categorical Attributes | Columns representing discrete groups or labels — even if numeric |
| Growth | Whether new records are expected over time |

---

## Dataset Summaries

### Taxi Trips

| Attribute | Value |
|---|---|
| Primary Entity | One completed yellow taxi trip |
| Primary Key | None — no unique identifier exists |
| Join Attributes | `pickup_location_id`, `dropoff_location_id`, `pickup_datetime` |
| Temporal Attributes | `pickup_datetime`, `dropoff_datetime` |
| Categorical Attributes | `vendor_id`, `rate_code_id`, `payment_type`, `store_and_fwd_flag`, `pickup_location_id`, `dropoff_location_id` |
| Likely to Grow? | Yes — continuously |

**Key observations:**
- 9,554,778 records covering January–March 2024
- Composite key test (vendor, timestamps, locations) was not unique — no reliable PK exists
- Location IDs join to Taxi Zone Lookup; pickup time joins to Weather and Air Quality

---

### Weather

| Attribute | Value |
|---|---|
| Primary Entity | One hourly weather observation |
| Primary Key | `(year, month, day, hour)` |
| Join Attributes | Assembled hourly timestamp |
| Temporal Attributes | `year`, `month`, `day`, `hour` |
| Categorical Attributes | `*_source` fields, `condition_code` |
| Likely to Grow? | No — static 2024 extract |

**Key observations:**
- 8,784 records = 366 days × 24 hours (leap year 2024, complete)
- `snwd` and `wpgt` are 100% null — dropped in silver
- One implicit weather location; no station coordinate column

---

### Air Quality

| Attribute | Value |
|---|---|
| Primary Entity | One hourly PM2.5 measurement from one monitoring site |
| Primary Key | `(state_code, county_code, site_num, parameter_code, poc, date_local, time_local)` |
| Join Attributes | Measurement timestamp + site selection |
| Temporal Attributes | `date_local`, `time_local`, `date_gmt`, `time_gmt` |
| Categorical Attributes | State/county/site codes, parameter code, POC, method fields, qualifier, units, datum |
| Likely to Grow? | Yes — hourly, from 925 sites |

**Key observations:**
- 8,139,551 records from 925 monitoring sites across 2024
- `parameter_code` is always `88101` (PM2.5 only)
- A time-only join with taxi trips is ambiguous — site selection is required

---

### Taxi Zone Lookup

| Attribute | Value |
|---|---|
| Primary Entity | One TLC taxi zone |
| Primary Key | `location_id` |
| Join Attributes | `location_id` |
| Temporal Attributes | None |
| Categorical Attributes | `borough`, `zone`, `service_zone` |
| Likely to Grow? | Rarely |

**Key observations:**
- 265 rows; `location_id` is unique with no nulls
- `location_id = 265` has unknown borough and zone — preserved via left join, not dropped

---

## Summary Table

| Dataset | Primary Entity | Primary Key | Grows? |
|---|---|---|---|
| Taxi Trips | One taxi trip | None | Yes, highly |
| Weather | One hourly observation | `(year, month, day, hour)` | No |
| Air Quality | One hourly PM2.5 reading | 7-column composite | Yes, hourly |
| Taxi Zone Lookup | One taxi zone | `location_id` | Rarely |

---

## Key Findings

**Primary keys:** Taxi Trips has no primary key — composite key test failed. All others have valid, tested keys. No artificial key was invented.

**Joins:** Taxi Trips → Taxi Zones via location ID (two joins: pickup and dropoff). Taxi Trips → Weather via hourly timestamp. Taxi Trips → Air Quality requires both timestamp and site selection — time alone is insufficient across 925 sites.

**Categorical vs numeric:** `vendor_id = 2` means Vendor 2, not the quantity two. Location IDs, state codes, and payment types are all categorical despite being stored as integers.

**Nulls:** `snwd`, `wpgt`, and `uncertainty` are structurally null (100%) and carry no information. Addressed in Task 4.