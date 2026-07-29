# acronet

Data source for the CIMA Acronet network: IoT weather stations across Liguria and northern Italy, fetched from CIMA's webdrops API. The core's scheduler triggers it with a time window; it fetches each configured sensor class over the window in bounded chunks, normalises units (including sentinel `-9000` nulls and knots→m/s for gust sensors), and streams the observations in over Flight.

Run it with `uv run acronet -c config.yaml` (see `config.example.yaml`). API credentials come from the `ACRONET_USERNAME`, `ACRONET_PASSWORD`, and `ACRONET_CLIENT_SECRET` environment variables — a Kubernetes Secret in deployment.
