# meteotracker

Data source for MeteoTracker: mobile weather sensors whose observations arrive as per-trip sessions. The core's scheduler triggers it with a time window; it fetches the window's session metadata, streams each session's point data, tags rows with the living lab derived from the session author, and ingests over Flight. Sessions upload well after the trip ends; windows stay revisable for the hot-store retention, which is what accommodates the late tail — the source itself fetches exactly the triggered window.

Run it with `uv run meteotracker -c config.yaml` (see `config.example.yaml`). API credentials come from the `METEOTRACKER_USERNAME` and `METEOTRACKER_PASSWORD` environment variables — a Kubernetes Secret in deployment.
