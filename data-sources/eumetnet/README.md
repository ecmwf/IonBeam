# eumetnet

Data source for EUMETNET's E-SOH MQTT feed of surface observations. It maintains a persistent MQTT session and buffers incoming GeoJSON messages until a size or time threshold is reached. Before sending a batch to the Flight endpoint, it deduplicates messages by publication time and pivots per-parameter messages into rows with their quality-control codes. A stable ingestion identifier allows a failed batch to be retried with the same identity.

Run it with `uv run eumetnet -c config.yaml` (see `config.example.yaml`). Broker credentials come from the `MQTT_USERNAME` and `MQTT_PASSWORD` environment variables and should be provided through a Kubernetes Secret in deployment.

Run one replica per broker session. The MQTT broker associates each session with a client identifier.
