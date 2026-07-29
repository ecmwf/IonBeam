Architecture
============

Overview
--------

IonBeam decouples data ingestion, aggregation, and export behind a single Arrow Flight (gRPC) endpoint. The platform consists of three component types:

Data sources
  Services that collect observations from external APIs. Each source (MeteoTracker, Acronet, EUMETNET E-SOH, Sensor.Community, ...) runs independently. It streams observations into the Flight endpoint and receives trigger commands pushed over a ``DoExchange`` subscription to the same endpoint.

IonBeam core
  The service hosting the Flight endpoint and the handlers behind it:

  - Ingestion handler: validates and normalises raw observations into the time-series database
  - Coordinator handler: tracks window coverage and schedules dataset builds
  - Builder handler: materialises aggregated datasets and publishes them to exporters
  - Source scheduler: publishes trigger commands to data sources on wall-clock-aligned intervals

Exporters
  Services that consume built datasets and write them to external systems. Each exporter (ECMWF/ODB, ...) runs independently and receives dataset events pushed over its Flight subscription.

Built datasets are canonical GeoParquet. Each geographic dataset carries a WKB ``ib_geometry`` column tagged with the ``geoarrow.wkb`` Arrow extension and a stable per-observation ``ib_id``, so the files are directly queryable by GeoArrow-aware tools. Every platform-synthesized column lives under the ``ib_`` prefix — declared source columns may use any other name. A stateless PyGeoAPI server serves the store as an OGC EDR/Features API, one collection per dataset, with no export step or second copy.

.. mermaid:: architecture-diagram.mmd
  :zoom:

Solid arrows carry data; dashed arrows carry control events.

Every component runs with any number of replicas; :ref:`architecture:Scaling` describes the mechanisms.

Data Flow
---------

Data flows through four stages:

1. Data sources call :ref:`flight-interface:Ingest (DoPut)` and stream Arrow RecordBatches with a JSON envelope. The ingestion handler validates, normalises, and writes each batch to InfluxDB as it arrives, and publishes coverage claims — each naming the per-window records it delivered rows under — while the stream is still open (:ref:`domain:Coverage Claims`).

2. The coordinator stores each claim for gap analysis and folds its records into their windows' desired sets. When a window's content changes, the window is scheduled to build at the moment it becomes worth building: its settle time for a first build, its rebuild debounce for a revision (:ref:`domain:Window Readiness`).

3. A builder claims each due window, queries InfluxDB for its observations, writes the dataset to the arrow store, and publishes a ``DataSetAvailableEvent`` that fans out to all subscribed exporters.

4. Exporters receive the event over :ref:`flight-interface:Dataset Events (DoExchange)`, stream the dataset back with ``DoGet``, and transform it to their target format, such as ODB. PyGeoAPI reads the canonical GeoParquet directly instead.

Worked examples of the window mechanics, including late data and streaming ingestion, are in :ref:`domain:Coverage Claims` and :ref:`domain:Out-of-Order Processing`.

Flight Endpoint
---------------

All integration happens through the one Arrow Flight endpoint hosted by the core service. Data sources register their dataset and stream observations in with ``DoPut``; exporters subscribe with ``DoExchange`` and stream built datasets back with ``DoGet``. :doc:`flight-interface` specifies the full contract.

Internally, an event bus carries control messages: source triggers and dataset availability, never bulk data. The bus is in-memory in single-process deployments and Valkey streams otherwise, so events reach subscribers regardless of which replica produced them. The Flight endpoint subscribes on behalf of connected clients and pushes events down their ``DoExchange`` streams; integrators never interact with the bus directly.

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

Every service runs with any number of replicas. Replicas hold no coordination state of their own: everything needed to pick up a unit of work lives in Valkey or the storage layer, so replicas can be added, removed, or restarted without draining the pipeline.

Trigger claims
~~~~~~~~~~~~~~

Every core replica runs the source scheduler and computes the same wall-clock-aligned boundaries from the schedule alone. Before firing, a replica claims the (schedule, boundary) pair with an atomic ``SET NX`` in Valkey. One replica wins and publishes the trigger; the rest move on. Claims expire after two trigger intervals. The trigger's command id derives deterministically from the schedule and boundary, so even a duplicate fire replays under the same identity.

Event delivery
~~~~~~~~~~~~~~

The event bus runs on Valkey streams with one consumer group per subscriber identity. A client names its identity when it opens its ``DoExchange`` subscription (the ``source_name`` or ``exporter_name`` in the descriptor). Each distinct identity receives every matching event. Replicas sharing an identity form one group and split the events between them, so scaling an exporter to five replicas divides its event stream five ways with no server-side configuration. The groups live in Valkey, outside any core process; a client subscribed through one core replica receives events published through any other.

Delivery is at-least-once. The Flight server acknowledges an event to the bus only after the client reports that its handler finished. A client that dies mid-handler leaves the event pending, and another consumer reclaims it once it has been idle longer than a threshold sized above the slowest handler's run time. An event that fails eight deliveries is parked on a bounded dead-letter stream. Redelivery is safe because the operations behind it are idempotent: ingestion claims carry deterministic ids, and a redelivered dataset event names the same immutable build files.

Build leases
~~~~~~~~~~~~

Scheduled windows sit in a Valkey sorted set scored by eligibility time. A builder replica atomically moves the earliest due window into a leased set stamped with a deadline; that atomic move is the only lock in the system. Completing the build releases the lease. If the builder crashes, the lease expires and the window returns to the queue for another replica. Each build first compares the window's desired record hash against the recorded observed hash, so a redundant delivery costs one lookup, and a failed build retries with exponential backoff.

Ingestion
~~~~~~~~~

Any core replica serves any ``DoPut``. Observations stream straight into the time-series database, coordination records go to Valkey through atomic operations, and the per-window deduplication filters are server-side Bloom filters whose check-and-add is a single atomic command. Replicas ingesting overlapping data concurrently never double-count.

Dataset publication
~~~~~~~~~~~~~~~~~~~

A window's build is one immutable file under its dataset's day directory, at ``<dataset>/<YYYYMMDD>/<window start stamp>_<span>-v<version>-<record-set hash>``: the stamp and span name the window (matching its manifest entry), the version orders that window's builds, and the hash ties the file to its provenance (Parquet footer and window manifest). Readers prune by the window interval in the name or by the time column's Parquet statistics; each file is written time-sorted. A rebuild writes the next version's file beside the current one and never touches an existing file; readers pick the highest version, and a periodic sweep deletes a window's older versions once its current build has stood for a grace period. Local-filesystem writes finish with a rename and S3 writes with a multipart-upload completion, so a reader never sees a partial file.

Stateful backends
~~~~~~~~~~~~~~~~~

The service tier scales horizontally today; the stateful backends below it run as single instances:

- InfluxDB 3 Core runs as one node, since the open-source edition has no clustering. It also performs no background compaction: every snapshot cuts a new Parquet file per table, and files are never merged. The repository includes an offline compactor (``compactor/``) that merges each day's files into one deduplicated file between deploy rolls.
- Valkey runs as a single instance carrying the control plane: event bus, build queue, and coordination state.
- The arrow store speaks plain S3 and can point at any object storage.

Data sources fed by MQTT (EUMETNET E-SOH) run one replica each because the broker session is keyed to a client id; pull-based sources have no such constraint.
