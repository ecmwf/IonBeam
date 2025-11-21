Messaging Interface Specification
==================================

This document specifies the messaging contracts between components in the ionbeam platform. The interface is transport-agnostic and message-oriented, using structured payloads serialized as JSON with binary data transferred via Apache Arrow IPC format.

.. note::

   For an overview of the system architecture and how these messages flow through the platform, see :doc:`architecture`. This document focuses on the technical contracts for implementing data sources and exporters.

.. note::

   The **ionbeam-client** Python library provides a reference implementation of these contracts. While this specification is language-agnostic, the ionbeam-client demonstrates how to correctly implement data sources and exporters that conform to these messaging interfaces. See :doc:`ionbeam-client/index` for the client library documentation.

Message Contracts
-----------------

IngestDataCommand
~~~~~~~~~~~~~~~~~

**Direction:** Data Source → Ionbeam Core

**Purpose:** Submit raw observation data for ingestion.

**Schema:**

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Field
     - Type
     - Description
   * - ``id``
     - UUID
     - Unique identifier for this ingestion operation
   * - ``metadata``
     - IngestionMetadata
     - Complete metadata describing dataset and variable mappings (see below)
   * - ``payload_location``
     - String
     - Object storage URI where Arrow IPC data is stored
   * - ``start_time``
     - ISO 8601 DateTime
     - Start of the temporal range covered by this data (UTC)
   * - ``end_time``
     - ISO 8601 DateTime
     - End of the temporal range covered by this data (UTC)

**JSON Example:**

.. code-block:: json

    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "metadata": {
        "dataset": {
          "name": "weather_stations",
          "description": "Ground weather station observations",
          "aggregation_span": "PT1H",
          "source_links": [],
          "keywords": ["weather", "temperature"],
          "subject_to_change_window": "PT0S"
        },
        "ingestion_map": {
          "datetime": {
            "from_col": "timestamp",
            "dtype": "datetime64[ns, UTC]"
          },
          "lat": {
            "standard_name": "latitude",
            "cf_unit": "degrees_north",
            "from_col": "lat",
            "dtype": "float64"
          },
          "lon": {
            "standard_name": "longitude",
            "cf_unit": "degrees_east",
            "from_col": "lon",
            "dtype": "float64"
          },
          "canonical_variables": [
            {
              "column": "temperature",
              "standard_name": "air_temperature",
              "cf_unit": "degC",
              "level": 2.0,
              "method": "point",
              "period": "PT0S",
              "dtype": "float64"
            }
          ],
          "metadata_variables": [
            {
              "column": "station_id",
              "dtype": "string"
            }
          ]
        },
        "version": 1
      },
      "payload_location": "raw/weather_stations/20240101T120000Z-20240101T130000Z_20240101T130500Z",
      "start_time": "2024-01-01T12:00:00Z",
      "end_time": "2024-01-01T13:00:00Z"
    }

**Constraints:**

- ``id`` must be unique across all ingestion operations
- ``payload_location`` must reference valid Arrow IPC data in object storage
- ``start_time`` and ``end_time`` must be valid UTC timestamps
- ``metadata.version`` must match the supported version (currently 1)

DataAvailableEvent
~~~~~~~~~~~~~~~~~~

**Direction:** Ionbeam Core (Ingestion Handler) → Ionbeam Core (Coordinator Handler)

**Purpose:** Signal that raw observations have been validated and stored in the time series database.

**Schema:**

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Field
     - Type
     - Description
   * - ``id``
     - UUID
     - Matches the ``IngestDataCommand.id`` that triggered this event
   * - ``metadata``
     - IngestionMetadata
     - Complete metadata from the ingestion command
   * - ``start_time``
     - ISO 8601 DateTime
     - Actual start time of the stored data (may differ from command)
   * - ``end_time``
     - ISO 8601 DateTime
     - Actual end time of the stored data (may differ from command)

**JSON Example:**

.. code-block:: json

    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "metadata": { "...": "same as IngestDataCommand" },
      "start_time": "2024-01-01T12:00:00Z",
      "end_time": "2024-01-01T13:00:00Z"
    }

**Note:** This is an internal message not visible to external data sources or exporters.

DataSetAvailableEvent
~~~~~~~~~~~~~~~~~~~~~

**Direction:** Ionbeam Core (Builder Handler) → Exporters

**Purpose:** Notify exporters that a complete, aggregated dataset is ready for consumption.

**Schema:**

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Field
     - Type
     - Description
   * - ``id``
     - UUID
     - Unique identifier for this dataset
   * - ``metadata``
     - DatasetMetadata
     - Dataset-level metadata (subset of IngestionMetadata)
   * - ``dataset_location``
     - String
     - Object storage URI where the dataset file is stored
   * - ``start_time``
     - ISO 8601 DateTime
     - Start of the dataset temporal window (UTC)
   * - ``end_time``
     - ISO 8601 DateTime
     - End of the dataset temporal window (UTC)

**JSON Example:**

.. code-block:: json

    {
      "id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
      "metadata": {
        "name": "weather_stations",
        "description": "Ground weather station observations",
        "aggregation_span": "PT1H",
        "source_links": [],
        "keywords": ["weather", "temperature"],
        "subject_to_change_window": "PT0S"
      },
      "dataset_location": "weather_stations/20240101T120000_PT1H_a3f2bc9d",
      "start_time": "2024-01-01T12:00:00Z",
      "end_time": "2024-01-01T13:00:00Z"
    }

**Constraints:**

- ``dataset_location`` references an Arrow IPC file in object storage
- Time window ``[start_time, end_time)`` aligns to ``metadata.aggregation_span`` boundaries
- Multiple exporters may receive the same event (fanout pattern)

StartSourceCommand
~~~~~~~~~~~~~~~~~~

**Direction:** External Scheduler → Data Source

**Purpose:** Trigger a data source to fetch and ingest data for a specific time range.

**Schema:**

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Field
     - Type
     - Description
   * - ``id``
     - UUID
     - Unique identifier for this trigger command
   * - ``source_name``
     - String
     - Name of the data source to activate
   * - ``start_time``
     - ISO 8601 DateTime
     - Start of the time range to fetch (UTC)
   * - ``end_time``
     - ISO 8601 DateTime
     - End of the time range to fetch (UTC)

**JSON Example:**

.. code-block:: json

    {
      "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
      "source_name": "weather_stations",
      "start_time": "2024-01-01T12:00:00Z",
      "end_time": "2024-01-01T13:00:00Z"
    }

**Constraints:**

- ``source_name`` must match a registered data source
- ``start_time`` must be before ``end_time``
- Time range should align with source's natural update frequency

Data Payload Format
-------------------

Observation Data
~~~~~~~~~~~~~~~~

Raw observation data referenced by ``IngestDataCommand.payload_location`` must be stored as Apache Arrow IPC files. **The Arrow schema must align with the column names specified in the ingestion map metadata.**

Schema Alignment Rules
^^^^^^^^^^^^^^^^^^^^^^

The Arrow IPC column names must match the ``from_col`` values defined in the ``IngestionMetadata.ingestion_map``:

1. **Time Column**: Must match ``ingestion_map.datetime.from_col`` (or use default ``"timestamp"`` if ``from_col`` is ``null``)
2. **Latitude Column**: Must match ``ingestion_map.lat.from_col`` (or use default ``"latitude"`` if ``from_col`` is ``null``)
3. **Longitude Column**: Must match ``ingestion_map.lon.from_col`` (or use default ``"longitude"`` if ``from_col`` is ``null``)
4. **Canonical Variables**: Must match the ``column`` field for each entry in ``ingestion_map.canonical_variables``
5. **Metadata Variables**: Must match the ``column`` field for each entry in ``ingestion_map.metadata_variables``

**Required Columns:**

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Ingestion Map Field
     - Expected Arrow Type
     - Description
   * - ``datetime.from_col``
     - timestamp[ns, tz=UTC]
     - Observation timestamp column name from source data
   * - ``lat.from_col``
     - float64
     - Latitude column name from source data (range: [-90, 90])
   * - ``lon.from_col``
     - float64
     - Longitude column name from source data (range: [-180, 180])

**Variable Columns:**

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Ingestion Map Field
     - Expected Arrow Type
     - Description
   * - ``canonical_variables[].column``
     - Numeric types (as specified by ``dtype``)
     - Observed quantities - column names as they appear in source data
   * - ``metadata_variables[].column``
     - String or numeric (as specified by ``dtype``)
     - Tag/identifier columns - names as they appear in source data

**Example: Source Data to Ingestion Map Alignment**

Source Arrow IPC schema::

    obs_time: timestamp[ns, tz=UTC]
    lat: double
    lon: double
    temp_c: double
    rh_pct: double
    station: utf8

Corresponding ingestion map::

    {
      "datetime": {"from_col": "obs_time"},
      "lat": {"from_col": "lat", "standard_name": "latitude", "cf_unit": "degrees_north"},
      "lon": {"from_col": "lon", "standard_name": "longitude", "cf_unit": "degrees_east"},
      "canonical_variables": [
        {
          "column": "temp_c",
          "standard_name": "air_temperature",
          "cf_unit": "degC",
          "level": 2.0,
          "method": "point",
          "period": "PT0S",
          "dtype": "float64"
        },
        {
          "column": "rh_pct",
          "standard_name": "relative_humidity",
          "cf_unit": "percent",
          "level": 2.0,
          "method": "point",
          "period": "PT0S",
          "dtype": "float64"
        }
      ],
      "metadata_variables": [
        {"column": "station", "dtype": "string"}
      ]
    }

After ingestion, data is stored in InfluxDB with canonical column names::

    timestamp: timestamp[ns, tz=UTC]
    latitude: double
    longitude: double
    air_temperature__degC__2.0__point__PT0S: double
    relative_humidity__percent__2.0__point__PT0S: double
    station: utf8

**Requirement**: The column names in your Arrow IPC file must exactly match the ``from_col`` and ``column`` values in your ingestion map. Mismatches will result in ingestion failures with missing column errors.

Dataset Data
~~~~~~~~~~~~

Processed datasets referenced by ``DataSetAvailableEvent.dataset_location`` use Arrow IPC format with a standardized schema:

**Fixed Columns:**

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Column
     - Type
     - Description
   * - ``timestamp``
     - timestamp[ns, tz=UTC]
     - Observation timestamp
   * - ``latitude``
     - float64
     - Station latitude in decimal degrees
   * - ``longitude``
     - float64
     - Station longitude in decimal degrees

**Variable Columns:**

Columns use canonical naming convention: ``{standard_name}__{cf_unit}__{level}__{method}__{period}``

**Example:**

- ``air_temperature__degC__2.0__point__PT0S`` - Air temperature at 2m, instantaneous
- ``wind_speed__m_s-1__10.0__mean__PT1H`` - Wind speed at 10m, 1-hour mean
- ``relative_humidity__percent__2.0__point__PT0S`` - Relative humidity at 2m, instantaneous

Metadata Structures
-------------------

IngestionMetadata
~~~~~~~~~~~~~~~~~

Complete metadata for an ingestion operation.

**Fields:**

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Field
     - Type
     - Description
   * - ``dataset``
     - DatasetMetadata
     - Dataset-level information
   * - ``ingestion_map``
     - DataIngestionMap
     - Mapping from source columns to canonical format
   * - ``version``
     - Integer
     - Metadata schema version (currently 1)

DatasetMetadata
~~~~~~~~~~~~~~~

Dataset-level metadata.

**Fields:**

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Field
     - Type
     - Description
   * - ``name``
     - String
     - Unique dataset identifier (lowercase, underscores)
   * - ``description``
     - String
     - Human-readable description
   * - ``aggregation_span``
     - ISO 8601 Duration
     - Time window duration for dataset aggregation (e.g., "PT1H" for 1 hour)
   * - ``source_links``
     - Array[Link]
     - Links to data source documentation/homepage
   * - ``keywords``
     - Array[String]
     - Searchable keywords
   * - ``subject_to_change_window``
     - ISO 8601 Duration
     - Duration after window end during which data may still arrive (default: "PT0S")

DataIngestionMap
~~~~~~~~~~~~~~~~

Defines how source data maps to the canonical schema.

**Fields:**

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Field
     - Type
     - Description
   * - ``datetime``
     - TimeAxis
     - Timestamp column mapping
   * - ``lat``
     - LatitudeAxis
     - Latitude column mapping
   * - ``lon``
     - LongitudeAxis
     - Longitude column mapping
   * - ``canonical_variables``
     - Array[CanonicalVariable]
     - Observed quantity mappings
   * - ``metadata_variables``
     - Array[MetadataVariable]
     - Tag/identifier mappings

CanonicalVariable
~~~~~~~~~~~~~~~~~

Mapping for an observed quantity following CF conventions.

**Fields:**

.. list-table::
   :header-rows: 1
   :widths: 25 20 55

   * - Field
     - Type
     - Description
   * - ``column``
     - String
     - Source column name
   * - ``standard_name``
     - String
     - CF standard name (e.g., "air_temperature")
   * - ``cf_unit``
     - String
     - CF-compliant unit (e.g., "degC", "m_s-1")
   * - ``level``
     - Float
     - Measurement height/depth in meters (default: 0.0)
   * - ``method``
     - String
     - Aggregation method: "point", "mean", "sum", "min", "max" (default: "point")
   * - ``period``
     - ISO 8601 Duration
     - Averaging period for aggregated values (default: "PT0S" for instantaneous)
   * - ``dtype``
     - String
     - Data type: "float64", "int64", etc. (default: "float64")

Transport Protocol
------------------

Message Broker
~~~~~~~~~~~~~~

The reference implementation uses AMQP with the following topology:

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

**Properties:**

- All queues are **durable** (survive broker restart)
- All messages are **persistent** (delivery_mode=2)
- Manual acknowledgment after successful processing
- Prefetch count of 1 (one message at a time)

Object Storage
~~~~~~~~~~~~~~

Binary data payloads use an S3-compatible object storage interface:

**Operations:**

- ``PUT``: Store Arrow IPC stream (used by data sources and builders)
- ``GET``: Retrieve Arrow IPC stream (used by ingestion handlers and exporters)

**Key Patterns:**

- Raw data: ``raw/{dataset_name}/{start}_{end}_{ingestion_time}``
- Datasets: ``{dataset_name}/{window_start}_{aggregation}_{content_hash}``

Quality of Service
------------------

Message Delivery
~~~~~~~~~~~~~~~~

- **At-least-once delivery**: Messages may be redelivered on failure
- **Idempotency required**: Consumers must handle duplicate messages
- **Ordering**: No guaranteed order between independent messages
- **Acknowledgment**: Consumers must explicitly acknowledge successful processing

Error Handling
~~~~~~~~~~~~~~

**Message Processing Failures:**

1. Handler raises exception
2. Message is **not acknowledged**
3. Message returns to queue after timeout
4. Retry with exponential backoff
5. After max retries, route to dead-letter queue (if configured)

**Connection Failures:**

1. Automatic reconnection with exponential backoff
2. Queue subscriptions automatically restored
3. Unacknowledged messages redelivered

**Related Documentation:**

- :doc:`ionbeam-client/index` - Client library documentation