Architecture
============

Overview
--------

Ionbeam uses message-driven architecture to decouple data ingestion, aggregation, and export. The platform consists of three primary component types that communicate through asynchronous message passing:

**Data Sources**
  Services that collect observations from external APIs. Each source type (MeteoTracker, NetAtmo, Sensor.Community, etc.) runs independently and publishes ingestion commands when new data is available.

**Ionbeam Core**
  A single service hosting three event handlers that process observations sequentially:
  
  - **Ingestion Handler**: Validates and normalizes raw observations into the time-series database
  - **Coordinator Handler**: Tracks time window completeness and schedules dataset rebuilds
  - **Builder Handler**: Materializes aggregated datasets and publishes to exporters

**Exporters**
  Services that consume processed datasets and write to external systems. Each exporter (ECMWF/ODB, PyGeoAPI, Legacy API, etc.) runs independently and receives all dataset events.

.. mermaid:: architecture-diagram.mmd
  :zoom:

Message Flow
------------

Data flows through four sequential stages, each triggered by messages published from the previous stage:

.. mermaid:: data-flow-diagram.mmd
   :zoom:

1. **Ingestion**: Data sources publish :ref:`messaging-interface:IngestDataCommand` messages containing references to raw observation data in object storage. The ingestion handler validates, normalizes, and writes this data to InfluxDB, then publishes :ref:`messaging-interface:DataAvailableEvent` messages.

2. **Coordination**: The coordinator handler receives :ref:`messaging-interface:DataAvailableEvent` messages and tracks which time windows have received new data. It maintains an audit trail of ingestion events in Redis and enqueues windows for rebuilding when their content changes.

3. **Dataset Building**: The builder handler processes queued windows by querying InfluxDB, generating Arrow IPC files in object storage, and publishing :ref:`messaging-interface:DataSetAvailableEvent` messages that fan out to all registered exporters.

4. **Export**: Exporters listen to :ref:`messaging-interface:DataSetAvailableEvent` messages, retrieve the dataset from object storage, transform it to their target format (ODB, GeoParquet, REST API, etc.), and publish to their respective external systems.

Event Flow Example
------------------

This example shows how multiple ingestion events covering different parts of a single aggregation window trigger dataset rebuilds:

.. mermaid:: event-flow-diagram.mmd
   :zoom:

For more detail around window management and event consolidation, see :ref:`domain:Out-of-Order Processing`.

Message Broker Topology
------------------------

The current implementation uses AMQP with the following topology:

**Queues:**

.. list-table::
   :header-rows: 1
   :widths: 35 20 45

   * - Queue Name
     - Message Type
     - Consumer
   * - ``ionbeam.ingestion``
     - IngestDataCommand
     - Ionbeam Core (Ingestion Handler)
   * - ``ionbeam.source.{source_name}.start``
     - StartSourceCommand
     - Data Source (matching source_name)
   * - ``ionbeam.dataset.available.{exporter_name}``
     - DataSetAvailableEvent
     - Exporter (matching exporter_name)

**Exchanges:**

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Exchange Name
     - Type
     - Purpose
   * - ``ionbeam.ingestion``
     - Direct
     - Routes ingestion commands from data sources to handler
   * - ``ionbeam.data.available``
     - Fanout
     - Internal: broadcasts validated data events (Ingestion → Coordinator)
   * - ``ionbeam.dataset.available``
     - Fanout
     - Broadcast dataset events to all exporters

- All queues are **durable** (survive broker restart)
- All messages are **persistent** (delivery_mode=2)
- Prefetch count of 1 (one message at a time)

Rationale
---------

This staged pipeline separates write concerns (ingestion into the time-series database) from read concerns (generating export formats). Exporters operate independently by reading datasets in Arrow IPC format from object storage and transforming them into their target formats (ODB, GeoParquet, REST APIs, etc.). Multiple exporters can process the same dataset concurrently without impacting ingestion throughput.

The architecture enables safe evolution: new data sources register by publishing to the ingestion queue, new exporters subscribe to the dataset fanout exchange. The message broker topology ensures reliable delivery and independent scaling of each component type.