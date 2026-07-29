# sensor_community

Data source for sensor.community, the open network of citizen-run air-quality and weather sensors. It runs two paths at once: a continuous poll of the live API dump (the last ~5 minutes of measurements, regenerated every minute), with measurement ids remembered until past the dump's lookback so a failed poll retries them; and a trigger-driven crawl of the daily CSV archive for backfilling historical windows.

Run it with `uv run sensor-community -c config.yaml` (see `config.example.yaml`). The archive crawl is driven by a `scheduler.windows` entry in the core config; the live poll runs for the life of the process.
