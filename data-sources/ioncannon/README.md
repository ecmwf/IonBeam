# ioncannon

Synthetic load generator for ionbeam. It registers a dataset of made-up weather stations and streams generated observations into the Flight endpoint whenever the core triggers it, so the ingestion, windowing, and build pipeline can be exercised without any external API.

Run it with `uv run ioncannon -c config.yaml` (see `config.example.yaml`). The config sets the station count, measurement frequency, the geographic bounds stations are placed in, and `metadata_cardinality`, which bounds the number of distinct `sensor_type`/`location_type` tag values so a load test does not blow up the time-series database's series cardinality.

The declared dataset carries `air_temperature`, `air_pressure`, `relative_humidity`, and `wind_speed` variables with `station_id`, `sensor_type`, and `location_type` tags. Trigger windows arrive over the core scheduler's `await_triggers` subscription; a `scheduler.windows` entry with `source_name: "ioncannon"` in the core config drives it.
