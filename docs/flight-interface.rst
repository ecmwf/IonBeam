Flight Interface
================

.. warning::

   This interface is under active development and may change significantly. Do not rely on it as a stable contract yet.

This document specifies the Arrow Flight interface for implementing data sources and exporters. IonBeam exposes a single Arrow Flight (gRPC) endpoint. Control envelopes are JSON documents carried in Flight command descriptors, tickets, and action bodies. Observation and dataset payloads travel inline on the same connection as Apache Arrow RecordBatch streams; there is no message broker and no shared object storage between IonBeam and its integrators.

The **ionbeam-client** Python library implements this interface (see :ref:`ionbeam-client/index:IonBeam Client`); most integrators use it rather than speaking Flight directly.

RPC Surface
-----------

.. list-table::
   :header-rows: 1
   :widths: 25 30 45

   * - Flight RPC
     - Envelope
     - Purpose
   * - ``DoAction``
     - ``register_dataset``
     - Register a dataset's schema before ingesting (data sources)
   * - ``DoPut``
     - CMD ``{"op": "ingest", ...}``
     - Stream raw observations in (data sources)
   * - ``DoExchange``
     - CMD ``{"op": "await_triggers", ...}``
     - Receive pushed source trigger commands (data sources)
   * - ``DoExchange``
     - CMD ``{"op": "await_datasets", ...}``
     - Receive pushed dataset availability events (exporters)
   * - ``GetFlightInfo``
     - CMD ``{"op": "dataset", ...}``
     - Look up a built dataset window (schema, row count, ticket)
   * - ``DoGet``
     - Ticket ``{"op": "dataset", ...}``
     - Stream a built dataset out (exporters)
   * - ``DoAction``
     - ``health_check`` | ``trigger_source``
     - Liveness probe; manually publish a trigger (ops/testing)

Registration (DoAction)
-----------------------

Direction: data source → IonBeam.

Before a dataset can be ingested, its schema must be registered. The ``register_dataset`` action body is an :ref:`flight-interface:IngestionMetadata` JSON document; the result carries the registered contract's hash:

.. code-block:: json

    {"schema_hash": "9f2cbc61d1f1c6d5"}

Registration validates the declaration. Structural coordinate units are enforced here: a geographic x/y coordinate must declare a unit convertible to degrees, and a z coordinate with altitude semantics one convertible to metres. Registration fails otherwise (see :doc:`dataset-schema`).

Registering the same declaration again is idempotent. ``IonbeamClient.ingest()`` registers automatically before uploading.

Ingest (DoPut)
--------------

Direction: data source → IonBeam.

This is the primary input contract for data source implementors. The command descriptor carries the ingestion envelope as JSON; the observation rows follow as the Arrow RecordBatch stream of the ``DoPut`` call itself. Streams may be long-running: IonBeam claims coverage incrementally and builds completed windows while the stream is still open (see :ref:`domain:Coverage Claims`).

The dataset must be registered first. The server rejects an ingest whose dataset is unknown, and one whose metadata hash does not match the registered declaration.

Command descriptor:

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Field
     - Type
     - Description
   * - ``op``
     - String
     - Always ``"ingest"``
   * - ``id``
     - UUID
     - Unique identifier for this ingestion operation (generated if omitted)
   * - ``metadata``
     - IngestionMetadata
     - The registered declaration: dataset name, version, and schema (see :ref:`flight-interface:IngestionMetadata`)
   * - ``start``
     - ISO 8601 DateTime
     - Start of the temporal range covered by this data (UTC)
   * - ``end``
     - ISO 8601 DateTime
     - End of the temporal range covered by this data (UTC)

.. code-block:: json

    {
      "op": "ingest",
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "metadata": {
        "name": "weather_stations",
        "version": 1,
        "dataset_schema": {
          "time": {"name": "time"},
          "coordinates": [
            {"name": "lat", "axis": "y", "crs": "EPSG:4326",
             "unit": "degrees_north"},
            {"name": "lon", "axis": "x", "crs": "EPSG:4326",
             "unit": "degrees_east"}
          ],
          "variables": [
            {
              "name": "air_temperature",
              "semantics": {
                "scheme": "cf",
                "standard_name": "air_temperature",
                "level": 2.0,
                "cell_method": "point",
                "period": "PT0S"
              },
              "unit": "degC"
            }
          ],
          "tags": [{"name": "station_id"}]
        }
      },
      "start": "2024-01-01T12:00:00Z",
      "end": "2024-01-01T13:00:00Z"
    }

Response: after the client finishes writing, the server replies on the ``DoPut`` metadata channel with ``{"rows": <ingested row count>}``. Batches are ingested as they arrive; a full server-side queue backpressures the client.

If an upload fails mid-stream, the claims already published stand, since their observations are in the database. The source recovers the unclaimed tail by retrying the command: claim and record ids derive deterministically from the ingestion ``id``, so a replay is not mistaken for new data. Under the readiness rules a partially covered window is never published.

Constraints:

- ``id`` must be unique across all ingestion operations
- ``start`` and ``end`` must be valid UTC timestamps
- The RecordBatch stream's column names must be the canonical names declared in ``metadata.dataset_schema`` (see :ref:`flight-interface:Observation Data`)

Source Triggers (DoExchange)
----------------------------

Direction: IonBeam → data source (server push).

Data sources open a long-lived ``DoExchange`` stream to receive trigger commands telling them which time range to fetch and ingest. The source scheduler publishes these on configured wall-clock-aligned intervals; they can also be published manually via the ``trigger_source`` action.

Command descriptor:

.. code-block:: json

    {
      "op": "await_triggers",
      "source_name": "weather_stations"
    }

The server pushes one RecordBatch per trigger with the schema:

.. list-table::
   :header-rows: 1
   :widths: 20 30 50

   * - Column
     - Arrow Type
     - Description
   * - ``id``
     - utf8
     - Deterministic command id, derived from the schedule and boundary; stable across replays
   * - ``start``
     - timestamp[us, tz=UTC]
     - Start of the time range to fetch
   * - ``end``
     - timestamp[us, tz=UTC]
     - End of the time range to fetch

Constraints:

- ``source_name`` must match the name the scheduler is configured to trigger
- The stream stays open until the client disconnects; clients should reconnect on failure
- After handling a trigger, the client acknowledges it by writing the ``id`` back on the exchange's return channel; the server redelivers unacknowledged triggers

Dataset Events (DoExchange)
---------------------------

Direction: IonBeam → exporter (server push).

Exporters open a long-lived ``DoExchange`` stream to be notified when an aggregated dataset is ready. This is the primary input contract for exporter implementors.

Command descriptor:

.. code-block:: json

    {
      "op": "await_datasets",
      "exporter_name": "ecmwf",
      "datasets": ["weather_stations"]
    }

``datasets`` is optional; omit it to receive events for all datasets. The server pushes one single-row RecordBatch per event with one string column, ``event``, containing a JSON-encoded ``DataSetAvailableEvent``. After handling an event, the client acknowledges it by writing the event ``id`` back on the return channel; the server redelivers unacknowledged events.

DataSetAvailableEvent
~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Field
     - Type
     - Description
   * - ``id``
     - UUID
     - Unique identifier for this dataset build
   * - ``metadata``
     - DatasetMetadata
     - The dataset's production descriptor from the server-side dataset registry (name, presentation metadata, windowing)
   * - ``dataset_locations``
     - Array of strings
     - Opaque dataset keys; pass them back in a ``DoGet`` ticket to stream the data
   * - ``start_time``
     - ISO 8601 DateTime
     - Start of the dataset temporal window (UTC)
   * - ``end_time``
     - ISO 8601 DateTime
     - End of the dataset temporal window (UTC)
   * - ``is_final``
     - Boolean
     - The window's finalize delay has passed: this build is immutable and no further revisions will be published

.. code-block:: json

    {
      "id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
      "metadata": {
        "name": "weather_stations",
        "description": "Ground weather station observations",
        "feature_type": "timeSeries",
        "aggregation_span": "PT1H",
        "source_links": [],
        "keywords": ["weather", "temperature"]
      },
      "dataset_locations": ["weather_stations/20240101/20240101T120000_PT1H-v1-3f9c2a1b"],
      "start_time": "2024-01-01T12:00:00Z",
      "end_time": "2024-01-01T13:00:00Z",
      "is_final": false
    }

Constraints:

- The time window ``[start_time, end_time)`` aligns to ``metadata.aggregation_span`` boundaries
- Every subscribed exporter identity receives every event (fanout); a rebuilt window emits a fresh event
- ``dataset_locations`` is opaque: treat it as a ticket, never parse it

Reading Datasets (GetFlightInfo / DoGet)
----------------------------------------

Direction: exporter → IonBeam.

Exporters stream a built dataset with ``DoGet``. The ticket is issued by the server and carries the build files to stream:

.. code-block:: json

    {"op": "dataset", "locations": ["weather_stations/20240101/20240101T120000_PT1H-v1-3f9c2a1b"]}

A single window is looked up via ``GetFlightInfo`` with a command descriptor:

.. code-block:: json

    {"op": "dataset", "dataset": "weather_stations", "start": "2024-01-01T12:00:00Z", "end": "2024-01-01T13:00:00Z"}

``op: dataset_range`` takes the same fields and instead resolves the current build of every window starting in ``[start, end)`` — how an exporter rebuilds a cycle from whatever has been published so far.

The returned ``FlightInfo`` carries the dataset schema, total row count (``-1`` for a range), and the ``DoGet`` ticket. If a requested window has not been built, the call fails.

The streamed data follows the canonical dataset schema; see :ref:`dataset-schema:Dataset Schema`.

Actions (DoAction)
------------------

.. list-table::
   :header-rows: 1
   :widths: 22 38 40

   * - Action
     - Body
     - Result
   * - ``register_dataset``
     - IngestionMetadata JSON
     - ``{"schema_hash": ...}`` (see :ref:`flight-interface:Registration (DoAction)`)
   * - ``health_check``
     - —
     - ``ok`` (liveness/readiness probe)
   * - ``trigger_source``
     - ``{"source_name": ..., "start": ..., "end": ...}``
     - ``ok``; publishes a trigger to the named source's subscription (ops/testing)

A Complete Exchange
-------------------

The sequence below is one bounded upload from a source named ``weather_stations``, and what a subscribed exporter sees. Every call goes to the same endpoint.

1. The source registers its declaration (once; re-registering is idempotent)::

    DoAction register_dataset  {"name": "weather_stations", "version": 1, "dataset_schema": {...}}
    → {"schema_hash": "9f2cbc61d1f1c6d5"}

2. The source uploads an hour of observations::

    DoPut  CMD {"op": "ingest", "id": "550e8400-...", "metadata": {...},
                "start": "2024-01-01T12:00:00Z", "end": "2024-01-01T13:00:00Z"}
           + Arrow RecordBatch stream: time, lat, lon, air_temperature, station_id
    → {"rows": 1440}

3. Once the window ``[12:00, 13:00)`` is complete and its settle time passes, a builder publishes it. Each subscribed exporter receives, on its open exchange::

    DoExchange CMD {"op": "await_datasets", "exporter_name": "ecmwf"}
    ← {"id": "7c9e6679-...", "dataset_locations": ["weather_stations/20240101/20240101T120000_PT1H-v1-3f9c2a1b"],
       "start_time": "2024-01-01T12:00:00Z", "end_time": "2024-01-01T13:00:00Z",
       "is_final": false, "metadata": {...}}

4. The exporter streams the dataset and acknowledges the event::

    DoGet  Ticket {"op": "dataset", "locations": ["weather_stations/20240101/20240101T120000_PT1H-v1-3f9c2a1b"]}
    ← Arrow RecordBatch stream (canonical schema, sorted by time)

If late data for the window arrives while it is still revisable, steps 3 and 4 repeat with a fresh event for the rebuilt window.

Data Payload Format
-------------------

Observation Data
~~~~~~~~~~~~~~~~

Raw observation data is streamed inline in the ``DoPut`` call as Apache Arrow RecordBatches. The stream's column names are the *canonical* names from the declared ``dataset_schema``; a source renames its feed's raw columns in its own transform. The client library's ``canonical_record_batches`` validates the frames against the declaration, coerces dtypes, and stamps the Arrow field metadata before the stream leaves the client (see :doc:`dataset-schema`).

A feed delivering ``obs_time``/``temp_c``/``rh_pct``/``station`` columns renames them to the canonical names it declares::

    DatasetSchema(
        time=TimeCoordinate(),
        coordinates=geographic_point_coordinates(),
        variables=[
            Variable(name="air_temperature",
                     semantics=CfSemantics(standard_name="air_temperature",
                                           level=2.0, cell_method="point", period="PT0S"),
                     unit="degC"),
            Variable(name="relative_humidity",
                     semantics=CfSemantics(standard_name="relative_humidity",
                                           level=2.0, cell_method="point", period="PT0S"),
                     unit="percent"),
        ],
        tags=[Tag(name="station_id")],
    )

The canonical stream then carries::

    time: timestamp[ns, tz=UTC]
    lat: double
    lon: double
    air_temperature: double     # ionbeam.semantics, ionbeam.unit in field metadata
    relative_humidity: double
    station_id: utf8

Dataset Data
~~~~~~~~~~~~

Built datasets streamed via ``DoGet`` carry the declared canonical schema: plain column names, with structure and semantics in Arrow field metadata and the dataset's production descriptor in schema metadata. Consumers read them through ``ionbeam_client.schema_meta``; see :doc:`dataset-schema`.

Metadata Structures
-------------------

IngestionMetadata
~~~~~~~~~~~~~~~~~

What a source declares at registration and ingestion. Dataset-production concerns (aggregation span, finalisation, presentation metadata) live in the server-side dataset registry, keyed by ``name``; a source does not supply them.

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Field
     - Type
     - Description
   * - ``name``
     - String
     - Unique dataset identifier (lowercase, underscores)
   * - ``dataset_schema``
     - DatasetSchema
     - The declared columns: time, coordinates, variables, tags
   * - ``version``
     - Integer
     - The source's contract version; bump on intentional schema change

DatasetSchema
~~~~~~~~~~~~~

The declared columns, each by its canonical ``name``. Raw feed column names never appear here.

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Field
     - Type
     - Description
   * - ``time``
     - TimeCoordinate
     - The single structural time axis windowing operates on
   * - ``coordinates``
     - Array[Coordinate]
     - Columns locating an observation: optional ``axis`` (x/y/z), ``crs``, ``semantics``, ``unit``
   * - ``variables``
     - Array[Variable]
     - Measured value columns: ``dtype``, ``semantics``, ``unit``, ``ancillary_of``
   * - ``tags``
     - Array[Tag]
     - Low-cardinality string identifier columns

Semantics
~~~~~~~~~

A variable's or coordinate's governed identity: a typed object discriminated on ``scheme``. Exporters match on exact equality of the whole object.

**cf**, the CF conventions vocabulary:

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Field
     - Type
     - Description
   * - ``standard_name``
     - String
     - CF Standard Name Table entry (e.g. "air_temperature")
   * - ``level``
     - Float
     - Sensor height in metres (optional)
   * - ``cell_method``
     - String
     - CF Conventions §7.3 method: "point", "sum", "mean", ... (optional)
   * - ``period``
     - ISO 8601 Duration
     - Aggregation period, e.g. "PT0S" for instantaneous (optional)

A column may declare no semantics at all: it is stored and served as an ungoverned named column.
