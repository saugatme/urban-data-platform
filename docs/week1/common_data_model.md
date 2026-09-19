# Common Data Model

The silver layer is the common representation used for integration.

| Area | Standard |
|---|---|
| Names | `snake_case` |
| Time | Taxi and air-quality datetimes use Spark `timestamp` in the `America/New_York` session timezone; weather keeps `year`, `month`, `day`, and `hour` |
| Categorical IDs | Integer |
| Measurements | Numeric |
| Labels | String with surrounding whitespace removed |
| Empty columns | Dropped |

Taxi pickup and dropoff values are timestamps. Weather keeps `year`, `month`, `day`, and `hour` because they identify an hourly observation. Air quality combines local date and time into `aq_timestamp`; the source time fields are removed from silver after this transformation. Weather precipitation nulls become `0.0`, and an unknown weather condition becomes `-1`. Air-quality qualifier nulls remain null because their absence is meaningful.
