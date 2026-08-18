# sensor_community

Data source for sensor.community, the open network of citizen-operated air-quality and weather sensors. The source uses separate paths for current and historical observations.

The live path polls an API dump containing approximately five minutes of measurements, regenerated each minute. It retains measurement identifiers for the duration of that lookback period so a failed poll can be retried without duplicating observations. The archive path responds to scheduled triggers and reads daily CSV files to backfill historical windows.

Run it with `uv run sensor-community -c config.yaml` (see `config.example.yaml`). The archive crawl is driven by a `scheduler.windows` entry in the core config; the live poll runs for the life of the process.
