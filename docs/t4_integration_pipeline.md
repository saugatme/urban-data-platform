# Integration Pipeline

`data/gold/integrated_taxi_trips` contains one accepted taxi trip per row, enriched with weather, hourly PM2.5, pickup zone and borough, and dropoff zone and borough.

| Context | Join rule |
|---|---|
| Weather | Pickup time truncated to the hour matches the assembled weather timestamp |
| Air quality | Pickup hour matches an hourly average from one selected NYC monitoring site |
| Pickup zone | `pickup_location_id` matches `location_id` |
| Dropoff zone | `dropoff_location_id` matches `location_id` |

All joins are left joins, so a missing observation leaves context columns null but keeps the trip. Weather, selected air quality, and taxi zones are broadcast because they are small.

The selected air-quality site makes the join deterministic and prevents multiple sites from creating duplicate trip rows. Its limitation is that one site does not represent conditions at every pickup location. Weather is also a single implicit location. The integrated table is partitioned by `year` and `month`.