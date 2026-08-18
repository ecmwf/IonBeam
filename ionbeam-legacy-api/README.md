# ionbeam-legacy-api

The previous public HTTP API, served unchanged from the new system so existing consumers keep working. Response shapes and behaviour are frozen, quirks included; only the connector that reads the new system changes.

Run it with `ionbeam-legacy-api -c config.yaml` (serves on port 8080 by default). New integrations should use Flight instead — see [Using the Data](../docs/using-the-data.rst).
