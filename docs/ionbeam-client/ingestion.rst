Data Ingestion
==============

The client provides two ingestion patterns: direct ingestion for standalone scripts and trigger handlers for scheduled collection managed by Ionbeam Core.

Direct Ingestion
----------------

Use for standalone data ingestion:

.. code-block:: python

   import asyncio
   from datetime import datetime, timezone, timedelta
   import pyarrow as pa
   from ionbeam_client import IonbeamClient, IonbeamClientConfig
   from ionbeam_client.models import (
       IngestionMetadata,
       DatasetMetadata,
       DataIngestionMap,
       TimeAxis,
       LatitudeAxis,
       LongitudeAxis,
       CanonicalVariable,
       MetadataVariable,
   )
   from ionbeam_client.arrow_tools import schema_from_ingestion_map

   config = IonbeamClientConfig(
       amqp_url="amqp://localhost:5672/"
   )

   # Define your ingestion map first
   ingestion_map = DataIngestionMap(
       datetime=TimeAxis(from_col="timestamp"),
       lat=LatitudeAxis(
           from_col="lat",
           standard_name="latitude",
           cf_unit="degrees_north",
       ),
       lon=LongitudeAxis(
           from_col="lon",
           standard_name="longitude",
           cf_unit="degrees_east",
       ),
       canonical_variables=[
           CanonicalVariable(
               column="temperature",
               standard_name="air_temperature",
               cf_unit="degC",
               level=2.0,
               method="point",
               period="PT0S",
           ),
       ],
       metadata_variables=[
           MetadataVariable(column="station_id", dtype="string"),
       ],
   )

   # Generate Arrow schema from ingestion map
   # This ensures schema alignment with metadata
   schema = schema_from_ingestion_map(ingestion_map)

   async def generate_batches():
       """Yield Arrow RecordBatches with observation data."""
       # Data uses source column names from ingestion map
       data = {
           "timestamp": [datetime(2024, 1, 1, i, tzinfo=timezone.utc) for i in range(24)],
           "lat": [52.5] * 24,
           "lon": [13.4] * 24,
           "temperature": [20.0 + i * 0.5 for i in range(24)],
           "station_id": ["STATION_001"] * 24
       }
       
       yield pa.RecordBatch.from_pydict(data, schema=schema)

   async def main():
       metadata = IngestionMetadata(
           dataset=DatasetMetadata(
               name="weather_stations",
               description="Ground weather station observations",
               aggregation_span=timedelta(hours=1),
               source_links=[],
               keywords=["weather", "temperature"],
           ),
           ingestion_map=ingestion_map,
           version=1,
       )

       async with IonbeamClient(config) as client:
           await client.ingest(
               batch_stream=generate_batches(),
               metadata=metadata,
               start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
               end_time=datetime(2024, 1, 2, tzinfo=timezone.utc)
           )

   asyncio.run(main())

Trigger Handlers
----------------

Register handlers that respond to scheduled triggers from Ionbeam Core:

.. code-block:: python

   client = IonbeamClient(config)

   async def fetch_and_ingest(start_time: datetime, end_time: datetime):
       """Called by scheduler for the requested time window."""
       async def generate_batches():
           data = await fetch_from_api(start_time, end_time)
           yield pa.RecordBatch.from_pydict(data, schema)
       
       await client.ingest(
           batch_stream=generate_batches(),
           metadata=metadata,
           start_time=start_time,
           end_time=end_time
       )

   # Register handler - source_name must match Core config
   client.register_trigger_handler("weather_source", fetch_and_ingest)

   async def main():
       async with client:
           await asyncio.Event().wait()

   asyncio.run(main())

Ingestion Metadata
------------------

Data sources must provide metadata that describes the dataset and maps source data columns to canonical variables following CF conventions.

Metadata Structure
~~~~~~~~~~~~~~~~~~

The ``IngestionMetadata`` consists of two main components:

1. **Dataset Metadata** - Describes the dataset itself
2. **Ingestion Map** - Defines how source columns map to the canonical schema

Dataset Metadata
^^^^^^^^^^^^^^^^

Defines dataset-level information:

.. code-block:: python

    from ionbeam_client.models import DatasetMetadata, Link
    from datetime import timedelta
    
    dataset_metadata = DatasetMetadata(
        name="weather_stations",
        description="Ground weather station observations",
        aggregation_span=timedelta(hours=1),
        source_links=[
            Link(
                mime_type="text/html",
                title="Data Source Homepage",
                href="https://example.com/weather"
            )
        ],
        keywords=["weather", "temperature", "humidity"],
        subject_to_change_window=timedelta(hours=0)
    )

**Fields:**

- ``name``: Unique dataset identifier (lowercase, underscores)
- ``description``: Human-readable description
- ``aggregation_span``: Time window for dataset aggregation (e.g., 1 hour)
- ``source_links``: Links to data source documentation
- ``keywords``: Searchable keywords
- ``subject_to_change_window``: Duration after window end during which late data may arrive

Ingestion Map
^^^^^^^^^^^^^

Defines the mapping from source columns to the canonical schema:

.. code-block:: python

    from ionbeam_client.models import (
        DataIngestionMap,
        TimeAxis,
        LatitudeAxis,
        LongitudeAxis,
        CanonicalVariable,
        MetadataVariable
    )
    
    ingestion_map = DataIngestionMap(
        datetime=TimeAxis(
            from_col="timestamp",
            dtype="datetime64[ns, UTC]"
        ),
        lat=LatitudeAxis(
            standard_name="latitude",
            cf_unit="degrees_north",
            from_col="lat",
            dtype="float64"
        ),
        lon=LongitudeAxis(
            standard_name="longitude",
            cf_unit="degrees_east",
            from_col="lon",
            dtype="float64"
        ),
        canonical_variables=[
            CanonicalVariable(
                column="temperature",
                standard_name="air_temperature",
                cf_unit="degC",
                level=2.0,
                method="point",
                period="PT0S",
                dtype="float64"
            ),
            CanonicalVariable(
                column="wind_speed",
                standard_name="wind_speed",
                cf_unit="m_s-1",
                level=10.0,
                method="mean",
                period="PT1H",
                dtype="float64"
            )
        ],
        metadata_variables=[
            MetadataVariable(
                column="station_id",
                dtype="string"
            )
        ]
    )

Canonical Variables
^^^^^^^^^^^^^^^^^^^

Canonical variables represent observed quantities following CF conventions. Each variable includes:

**Required Fields:**

- ``column``: Name of the column in your source data
- ``standard_name``: CF standard name (e.g., "air_temperature", "wind_speed")
- ``cf_unit``: CF-compliant unit (e.g., "degC", "m_s-1", "percent")

**Optional Fields:**

- ``level``: Measurement height/depth in meters (default: 0.0)
- ``method``: Aggregation method - "point", "mean", "sum", "min", "max" (default: "point")
- ``period``: ISO 8601 duration for averaged values (default: "PT0S" for instantaneous)
- ``dtype``: Data type (default: "float64")

**Important:** Your source data uses the ``column`` field names (e.g., "temp", "T0", "temperature"). The system internally transforms these to a standardized format based on the CF metadata you provide. For details on the internal data format, see :doc:`../messaging-interface`.

Metadata Variables
^^^^^^^^^^^^^^^^^^

Metadata variables are tags and identifiers (not observed quantities):

.. code-block:: python

    MetadataVariable(
        column="station_id",
        dtype="string"
    )

Common examples: ``station_id``, ``sensor_type``, ``quality_flag``

Complete Example
^^^^^^^^^^^^^^^^

.. code-block:: python

    from ionbeam_client.models import IngestionMetadata
    from datetime import timedelta
    
    metadata = IngestionMetadata(
        dataset=DatasetMetadata(
            name="weather_stations",
            description="Ground weather station observations",
            aggregation_span=timedelta(hours=1),
            source_links=[],
            keywords=["weather", "meteorology"],
            subject_to_change_window=timedelta(hours=0)
        ),
        ingestion_map=DataIngestionMap(
            datetime=TimeAxis(),
            lat=LatitudeAxis(
                standard_name="latitude",
                cf_unit="degrees_north"
            ),
            lon=LongitudeAxis(
                standard_name="longitude",
                cf_unit="degrees_east"
            ),
            canonical_variables=[
                CanonicalVariable(
                    column="temp",
                    standard_name="air_temperature",
                    cf_unit="degC",
                    level=2.0
                ),
                CanonicalVariable(
                    column="rh",
                    standard_name="relative_humidity",
                    cf_unit="percent",
                    level=2.0
                )
            ],
            metadata_variables=[
                MetadataVariable(column="station_id")
            ]
        ),
        version=1
    )

For complete details on the metadata contracts and message formats, see :doc:`../messaging-interface`.