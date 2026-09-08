# Data Catalog

## Definitions

1. Primary Entity: The main real-world object, event, or observation represented by each record in a dataset.
2. Primary Key: An attribute or combination of attributes that uniquely identifies one record.
3. Join Attributes: Columns used to connect records from one dataset to records in another dataset.
4. Temporal Attributes: Columns that represent when an event occurred, was measured, or was recorded.
5. Categorical Attributes: Attributes whose values represent discrete groups, classes, labels, or codes rather than continuous measurements. Numeric IDs can still be categorical.
6. Growth: The expected increase in records or values over time as new data is generated or collected.


# Dataset Catalog

## Taxi Trips

| Attribute | Description |
|---|---|
| Primary Entity | One completed yellow taxi trip |
| Primary Key | None explicitly provided |
| Join Attributes | `PULocationID`, `DOLocationID`, `tpep_pickup_datetime` |
| Temporal Attributes | `tpep_pickup_datetime`, `tpep_dropoff_datetime` |
| Categorical Attributes | `VendorID`, `RatecodeID`, `payment_type`, `store_and_fwd_flag`, `PULocationID`, `DOLocationID` |
| Likely to Grow? | Yes, highly |

### Observations

- Dataset contains approximately 9.55 million records for January–March 2024.
- No explicit unique trip identifier is provided.
- A candidate composite key using `VendorID`, pickup/dropoff timestamps, and pickup/dropoff locations was not unique.
- Therefore, an artificial primary key should not be assumed at this stage.
- `PULocationID` and `DOLocationID` connect to the Taxi Zone Lookup dataset.
- Pickup time can be used to connect trips to hourly Weather and Air Quality data.
- The dataset is a large fact/event table and is expected to grow continuously.

## Weather

| Attribute | Description |
|---|---|
| Primary Entity | One hourly weather observation |
| Primary Key | `(year, month, day, hour)` |
| Join Attributes | Assembled hourly timestamp |
| Temporal Attributes | `year`, `month`, `day`, `hour` |
| Categorical Attributes | `*_source` fields and `coco` weather-condition code |
| Likely to Grow? | No, static 2024 extract |

### Observations

- Dataset contains 8,784 records.
- 8,784 = 366 days × 24 hours, matching the leap year 2024.
- `(year, month, day, hour)` was tested and found to be unique.
- No null values were found in these key fields.
- Weather source columns such as `temp_source` and `rhum_source` describe data provenance.
- `coco` represents a weather-condition code.
- Numerical measurements include temperature, humidity, precipitation, wind speed, pressure, and cloud cover.
- `snwd` and `wpgt` contain missing values, and their source columns are entirely missing.
- The supplied dataset has an implicit location/station rather than an explicit station or coordinate column.

---

## Air Quality

| Attribute | Description |
|---|---|
| Primary Entity | One hourly PM2.5 measurement from one monitor |
| Primary Key | `(State Code, County Code, Site Num, Parameter Code, POC, Date Local, Time Local)` |
| Join Attributes | Measurement timestamp + site/spatial information |
| Temporal Attributes | `Date Local`, `Time Local`, `Date GMT`, `Time GMT` |
| Categorical Attributes | State/county/site codes, parameter codes, POC, method fields, qualifier, units, state/county names, datum |
| Likely to Grow? | Yes, hourly |

### Observations

- Dataset contains approximately 8.14 million records.
- It contains measurements from 925 monitoring sites.
- The date range is 2024-01-01 to 2024-12-31.
- The candidate composite key was tested and found to be unique.
- No null values were found in the tested primary-key fields.
- `Parameter Code` is always `88101`, because this file contains PM2.5 measurements.
- State, county, site, parameter, and POC codes are categorical identifiers even though some are numeric.
- `Qualifier`, method fields, units, and `Datum` are categorical/descriptive attributes.
- A time-only join with taxi trips would be ambiguous because there are many monitoring sites.
- Integration therefore requires both temporal and spatial/site selection.


## Taxi Zone Lookup

| Attribute | Description |
|---|---|
| Primary Entity | One TLC taxi zone |
| Primary Key | `LocationID` |
| Join Attributes | `LocationID` |
| Temporal Attributes | None |
| Categorical Attributes | `Borough`, `Zone`, `service_zone` |
| Likely to Grow? | Slowly / occasionally |

### Observations

- Dataset contains 265 taxi zones.
- `LocationID` was tested and found to be unique.
- No null values were found in `LocationID`.
- `LocationID` is an identifier, not a numerical measurement.
- `Borough`, `Zone`, and `service_zone` describe discrete categories.
- LocationID 265 contains `Unknown` values for Borough and Zone.
- Taxi trips referencing this location should not be removed during integration; a left join should preserve them.


# Final Data Catalog

| Dataset | Primary Entity | Primary Key | Join Attributes | Temporal Attributes | Categorical Attributes | Likely to Grow? |
|---|---|---|---|---|---|---|
| Taxi Trips | One completed yellow taxi trip | None | `PULocationID`, `DOLocationID`, `tpep_pickup_datetime` | `tpep_pickup_datetime`, `tpep_dropoff_datetime` | `VendorID`, `RatecodeID`, `payment_type`, `store_and_fwd_flag`, location IDs | Yes, highly |
| Weather | One hourly weather observation | `(year, month, day, hour)` | Assembled hourly timestamp | `year`, `month`, `day`, `hour` | `*_source`, `coco` | No — static 2024 extract |
| Air Quality | One hourly PM2.5 measurement from one monitor | `(State Code, County Code, Site Num, Parameter Code, POC, Date Local, Time Local)` | Timestamp + site/spatial information | `Date Local`, `Time Local`, `Date GMT`, `Time GMT` | State/county, parameter, method, qualifier, units, codes | Yes — hourly |
| Taxi Zone Lookup | One TLC taxi zone | `LocationID` | `LocationID` | None | `Borough`, `Zone`, `service_zone` | Slowly / occasionally |


# Notes and Findings

## Primary Key Findings

- Taxi Trips has no explicit primary key.
- The tested composite key for Taxi Trips was not unique.
- Weather has a valid hourly composite key.
- Air Quality has a valid composite key containing site, parameter, POC, date, and time.
- Taxi Zone Lookup has `LocationID` as its primary key.
- We should not invent a primary key where the source does not provide one.

## Join Findings

- Taxi Trips → Taxi Zones:
  - `PULocationID → LocationID`
  - `DOLocationID → LocationID`

- Taxi Trips → Weather:
  - Pickup timestamp must be aligned to the hourly weather timestamp.

- Taxi Trips → Air Quality:
  - Timestamp alone is insufficient because Air Quality contains 925 monitoring sites.
  - Both temporal and spatial/site information must be considered.

## Categorical Attribute Findings

A numeric datatype does not automatically mean that an attribute is a numerical measurement.

For example:

- `VendorID = 2` is a vendor category.
- `payment_type = 1` is a payment category.
- `LocationID = 186` identifies a location.
- `State Code = 36` identifies a state.

These should therefore be treated as categorical identifiers rather than continuous numerical variables.

## Growth Findings

- Taxi Trips is the largest fact table and has high expected growth.
- Air Quality is also a high-volume dataset because measurements are continuously collected from many monitoring sites.
- Weather grows hourly when new observations are collected, although the supplied project extract is static for 2024.
- Taxi Zone Lookup changes much more slowly because it is reference data.

## Implications for Later Tasks

1. Large fact tables should be stored differently from small lookup/reference tables.
2. Partitioning should be based on expected query patterns and data growth.
3. Taxi Trips is a strong candidate for time-based partitioning.
4. Taxi Zone Lookup should not be heavily partitioned.
5. Air Quality requires careful spatial and temporal integration.
6. Weather can be joined by hourly timestamp because the supplied dataset represents one implicit weather location.
7. Missing values must be handled consistently during ingestion and integration.
8. Left joins should be considered when enriching taxi trips so that valid trips are not lost because of missing lookup information.