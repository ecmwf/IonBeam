# meteotracker

Data source for MeteoTracker mobile weather sensors, whose observations are grouped into sessions for individual trips. For each scheduled time window, the source retrieves session metadata and point observations, adds the living lab derived from the session author, and sends the rows over Flight.

Sessions may be uploaded after a trip ends. IonBeam can revise the corresponding datasets while their observations remain within hot-store retention. The source itself requests only the scheduled time window.

Run it with `uv run meteotracker -c config.yaml` (see `config.example.yaml`). API credentials come from the `METEOTRACKER_USERNAME` and `METEOTRACKER_PASSWORD` environment variables — a Kubernetes Secret in deployment.
