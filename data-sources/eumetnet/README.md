# eumetnet

Data source for EUMETNET's E-SOH MQTT feed of surface observations. It holds a persistent MQTT session, buffers incoming GeoJSON messages, and flushes them to the Flight endpoint on a size/interval threshold: each flush deduplicates by publication time, pivots per-parameter messages into wide rows with their QC codes, and ingests under a stable id so a failed flush retries as the same data.

Run it with `uv run eumetnet -c config.yaml` (see `config.example.yaml`). Broker credentials come from the `MQTT_USERNAME` and `MQTT_PASSWORD` environment variables — a Kubernetes Secret in deployment. One replica per broker session: the MQTT session is keyed to the client id.
