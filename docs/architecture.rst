Architecture
============

Overview
--------

IonBeam exposes a single Arrow Flight (gRPC) endpoint for ingestion and dataset access. The platform has three component types:

Data sources
  Services that collect observations from external APIs and send them to IonBeam. A source can also open a ``DoExchange`` subscription to receive scheduled trigger commands.

IonBeam core
  The service hosting the Flight endpoint and the handlers behind it:

  - Ingestion handler: validates and normalises raw observations into the time-series database
  - Coordinator handler: tracks window coverage and schedules dataset builds
  - Builder handler: materialises aggregated datasets and publishes them to exporters
  - Source scheduler: publishes trigger commands to data sources on wall-clock-aligned intervals

Exporters
  Services that retrieve completed datasets and write them to external systems. Exporters receive dataset notifications through a Flight subscription.

Built datasets use GeoParquet. Geographic datasets include a WKB ``ib_geometry`` column tagged with the ``geoarrow.wkb`` Arrow extension and a stable ``ib_id`` for each observation. IonBeam reserves the ``ib_`` prefix for columns it adds; source declarations may use any other name.

.. mermaid:: architecture-diagram.mmd
  :zoom:

Solid arrows carry data; dashed arrows carry control events.

The core and supported client services can run with multiple replicas. :ref:`architecture:Scaling` describes the coordination mechanisms and current limits.

Data Flow
---------

Data flows through four stages:

1. Data sources stream Arrow RecordBatches with :ref:`flight-interface:Ingest (DoPut)`. The ingestion handler validates each batch and writes it to InfluxDB. As data arrives, the handler also publishes the coverage claims used to determine whether a window is ready to build (:ref:`domain:Coverage Claims`).

2. The coordinator stores the claims and checks them for gaps. It schedules a first build after the window's settle duration. If later data changes an existing build, it schedules a revision after the configured rebuild debounce (:ref:`domain:Window Readiness`).

3. A builder claims a due window and queries its observations from InfluxDB. After writing the result to the arrow store, it publishes a ``DataSetAvailableEvent`` for subscribed exporters.

4. Exporters receive the event over :ref:`flight-interface:Dataset Events (DoExchange)`, stream the dataset back with ``DoGet``, and transform it to their target format, such as ODB.

Worked examples of the window mechanics, including late data and streaming ingestion, are in :ref:`domain:Coverage Claims` and :ref:`domain:Out-of-Order Processing`.

Flight Endpoint
---------------

All integration happens through the one Arrow Flight endpoint hosted by the core service. Data sources register their dataset and stream observations in with ``DoPut``; exporters subscribe with ``DoExchange`` and stream built datasets back with ``DoGet``. :doc:`flight-interface` specifies the full contract.

.. mermaid:: service-topology.mmd
  :zoom:

Clients initiate every Flight RPC. For subscriptions, a client opens a long-lived ``DoExchange`` stream over which the core sends triggers or dataset notifications.

An internal event bus carries source triggers and dataset notifications. Observation and dataset payloads do not pass through this bus. Single-process deployments use an in-memory implementation; distributed deployments use Valkey streams.

The Flight endpoint connects each client subscription to the event bus and sends messages over the client's ``DoExchange`` stream. Integrators do not access the event bus directly.

Storage
-------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Store
     - Role
   * - InfluxDB 3
     - Time-series database holding normalised observations, queried per window by the builder
   * - Valkey
     - Coordination state: ingestion record audit trail, window build state, the build queue, scheduler trigger claims, and per-window deduplication filters
   * - Arrow store
     - Built datasets, streamed to exporters as Arrow RecordBatches via ``DoGet``. An S3-compatible object store in deployment, a local filesystem directory in development, both behind the same interface.

InfluxDB holds observations only for the hot period in which windows can still be built or revised. The durable output is the built datasets in the arrow store. Valkey keys are small and self-expiring; bulk data never passes through it.

Scaling
-------

The service tier supports horizontal scaling, subject to the limits described in :ref:`architecture:Stateful backends`. Replicas do not hold coordination state locally. Valkey and the storage layer retain the information required for another replica to continue scheduled work.

Trigger claims
~~~~~~~~~~~~~~

Every core replica runs the source scheduler and computes the same wall-clock-aligned boundaries from the schedule alone. Before firing, a replica claims the (schedule, boundary) pair with an atomic ``SET NX`` in Valkey. One replica wins and publishes the trigger; the rest move on. Claims expire after two trigger intervals. The trigger's command id derives deterministically from the schedule and boundary, so even a duplicate fire replays under the same identity.

Event delivery
~~~~~~~~~~~~~~

The event bus runs on Valkey streams with one consumer group per subscriber identity. A client names its identity when it opens its ``DoExchange`` subscription (the ``source_name`` or ``exporter_name`` in the descriptor). Each distinct identity receives every matching event. Replicas sharing an identity form one group and split the events between them, so scaling an exporter to five replicas divides its event stream five ways with no server-side configuration. The groups live in Valkey, outside any core process; a client subscribed through one core replica receives events published through any other.

Trigger and dataset-event delivery is at least once. The Flight server acknowledges an event to the bus only after the client reports that its handler has completed. If a client disconnects before acknowledgement, the event remains pending and becomes available to another consumer after the configured idle threshold.

After eight failed deliveries, the event moves to a bounded dead-letter stream. Clients must handle redelivery idempotently. Ingestion claims use stable identifiers, and a redelivered dataset event refers to the same immutable build files.

Build leases
~~~~~~~~~~~~

Scheduled windows sit in a Valkey sorted set scored by eligibility time. A builder replica atomically moves the earliest due window into a leased set stamped with a deadline; that atomic move is the only lock in the system. Completing the build releases the lease. If the builder crashes, the lease expires and the window returns to the queue for another replica. Each build first compares the window's desired record hash against the recorded observed hash, so a redundant delivery costs one lookup, and a failed build retries with exponential backoff.

Ingestion
~~~~~~~~~

Any core replica serves any ``DoPut``. Observations stream straight into the time-series database, coordination records go to Valkey through atomic operations, and the per-window stored-content sets behind ``dedup_ingestion`` are shared server-side. Replicas ingesting overlapping data concurrently never double-count.

Dataset publication
~~~~~~~~~~~~~~~~~~~

A window's build is one immutable file under its dataset's start-day partition::

    weather_stations/ib_year=2024/ib_month=01/ib_day=01/20240101T120000_PT1H-v1-3f9c2a1b
    └──────┬───────┘ └───────────────┬────────────────┘ └────────┬─────────┘└┬┘└───┬───┘
        dataset          start-day partition (hive)      window start + span │     │
                                                                          version  │
                                                                      record-set hash

The stamp and span name the window (matching its manifest entry), the version orders that window's builds, and the hash ties the file to its provenance (Parquet footer and window manifest).

The day is spelled as hive ``key=value`` segments in the platform's reserved ``ib_`` namespace: an engine pointed at the store can opt into hive parsing and prune on them, no declared column can collide with them, and a reader that does not opt in sees plain path segments. Aggregation spans divide one day and windows are epoch-aligned, so every window nests inside its partition — the rows under an ``ib_day`` are exactly that day's rows, and selecting a partition selects the whole day. Readers prune by the window interval in the name or by the time column's Parquet statistics; each file is written time-sorted.

A rebuild writes a new version without modifying the existing file. Readers select the highest version. After a grace period, a periodic sweep removes older versions of the window.

Local filesystem writes complete with a rename, while S3 writes complete through multipart upload. In both cases, the completed object becomes visible atomically.

Stateful backends
~~~~~~~~~~~~~~~~~

The service tier scales horizontally today; the stateful backends below it run as single instances:

- InfluxDB 3 Core runs as one node, since the open-source edition has no clustering. It also performs no background compaction: every snapshot cuts a new Parquet file per table, and files are never merged.
- Valkey runs as a single instance carrying the control plane: event bus, build queue, and coordination state.
- The arrow store speaks plain S3 and can point at any object storage.

Data sources fed by MQTT (EUMETNET E-SOH) run one replica each because the broker session is keyed to a client id; pull-based sources have no such constraint.
