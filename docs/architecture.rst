Architecture
============

Overview
--------

Ionbeam uses message-driven architecture to decouple data ingestion, aggregation, and export. The platform consists of three primary component types that communicate through asynchronous message passing:

- **Data Sources**: External data collectors that ingest observations from third-party systems
- **Ionbeam Core**: Central processing system that validates, aggregates, and builds time-windowed datasets
- **Exporters**: Downstream consumers that transform and export datasets to external systems and formats

.. mermaid:: architecture-diagram.mmd
  :zoom:

Message Flow
------------

Data flows through three sequential stages, each triggered by messages published from the previous stage:

1. **Ingestion**: Data sources publish :ref:`messaging-interface:IngestDataCommand` messages containing references to raw observation data in object storage. The ingestion handler validates, normalizes, and writes this data to InfluxDB, then publishes :ref:`messaging-interface:DataAvailableEvent` messages.

2. **Coordination**: The coordinator handler receives :ref:`messaging-interface:DataAvailableEvent` messages and tracks which time windows have received new data. It maintains an audit trail of ingestion events in Redis and enqueues windows for rebuilding when their content changes.

3. **Dataset Building**: The builder handler processes queued windows by querying InfluxDB, generating Arrow IPC files in object storage, and publishing :ref:`messaging-interface:DataSetAvailableEvent` messages that fan out to all registered exporters.

Design Benefits
---------------

This staged pipeline separates write concerns (ingestion into the time-series database) from read concerns (generating export formats). Exporters operate independently by reading :ref:`messaging-interface:Dataset Data` in Arrow IPC format from object storage and transforming them into their target formats (ODB, GeoParquet, REST APIs, etc.). Multiple exporters can process the same dataset concurrently without impacting ingestion throughput.

The architecture enables safe evolution: new data sources register by publishing to the ingestion queue, new exporters subscribe to the dataset fanout exchange (see :ref:`messaging-interface:Message Broker` for topology).