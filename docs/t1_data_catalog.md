# Data Catalog

| Dataset | Primary entity | Primary key | Join attributes | Temporal attributes | Categorical attributes | Growth |
|---|---|---|---|---|---|---|
| Taxi trips | One yellow taxi trip | No reliable natural key | `pickup_location_id`, `dropoff_location_id`, pickup hour | `pickup_datetime`, `dropoff_datetime` | Vendor, rate code, payment type, location IDs | Continuous |
| Weather | One hourly observation | `year`, `month`, `day`, `hour` | Hourly timestamp | `year`, `month`, `day`, `hour` | Source fields, `condition_code` | New observations |
| Air quality | One site, parameter, POC, and local-time observation | Site, parameter, POC, local date, local time | Selected site and hourly timestamp | `date_local`, `time_local`, `date_gmt`, `time_gmt` | Site and method codes, qualifier, units | New observations |
| Taxi zones | One taxi zone | `location_id` | `location_id` | None | Borough, zone, service zone | Rare changes |

Taxi trips have no reliable primary key, so the platform does not invent one. Air quality requires a site selection before an hourly join; time alone is not unique across monitoring sites.