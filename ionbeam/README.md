# ionbeam-core

The core ionbeam service. It serves the public Arrow Flight endpoint that data sources and exporters connect to, and runs:

- **Scheduling** — fires trigger windows at registered data sources; an atomic claim picks one replica to fire each boundary
- **Ingestion** — receives observation streams over Flight `DoPut` and writes canonical rows to the timeseries store
- **Window coordination** — tracks coverage of each dataset's time windows and decides when a window is ready to build
- **Dataset building** — queries the window from the timeseries store, assembles Arrow RecordBatches into the dataset store, and notifies subscribed exporters

State lives in InfluxDB 3 (timeseries), Valkey (coordination) and the dataset store; the service itself runs replicated.

See [config.example.yaml](config.example.yaml) for an annotated configuration, and the [repository README](../README.md) for how sources and exporters attach.
