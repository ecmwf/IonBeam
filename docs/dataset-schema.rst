Dataset Schema
==============

Datasets produced by IonBeam are stored in object storage with an Arrow RecordBatch interface (implemented as Parquet internally). Each dataset represents a single time window of aggregated observations following a canonical schema.

Arrow Schema
------------

All datasets conform to a fixed schema with required columns and variable columns based on the dataset's ingestion metadata.

Required Columns
~~~~~~~~~~~~~~~~

Every dataset includes these spatial and temporal dimensions:

.. list-table::
   :header-rows: 1
   :widths: 20 20 60

   * - Column
     - Arrow Type
     - Description
   * - ``timestamp``
     - timestamp[ns, tz=UTC]
     - Observation timestamp
   * - ``latitude``
     - float64
     - Decimal degrees, range [-90, 90]
   * - ``longitude``
     - float64
     - Decimal degrees, range [-180, 180]

Variable Columns
~~~~~~~~~~~~~~~~

Variable columns follow the canonical naming convention defined during ingestion. Each column name encodes CF metadata:

**Format**: ``{standard_name}__{cf_unit}__{level}__{method}__{period}``

**Components**:

- ``standard_name``: CF standard name (e.g., ``air_temperature``, ``wind_speed``)
- ``cf_unit``: CF-compliant unit (e.g., ``degC``, ``m_s-1``, ``percent``)
- ``level``: Measurement height/depth in meters (e.g., ``2.0``, ``10.0``)
- ``method``: Aggregation method (``point``, ``mean``, ``sum``, ``min``, ``max``)
- ``period``: Averaging period in ISO 8601 duration (e.g., ``PT0S`` for instantaneous, ``PT1H`` for 1-hour mean)

**Examples**:

- ``air_temperature__degC__2.0__point__PT0S``: Air temperature at 2m, instantaneous observation
- ``wind_speed__m_s-1__10.0__mean__PT1H``: Wind speed at 10m, 1-hour mean
- ``relative_humidity__percent__2.0__point__PT0S``: Relative humidity at 2m, instantaneous
- ``precipitation_amount__mm__0.0__sum__PT1H``: Precipitation accumulation at surface, hourly sum

Metadata Columns
~~~~~~~~~~~~~~~~

Datasets may include metadata columns (tags/identifiers) that do not follow the canonical naming:

- ``station_id``: Station identifier (string)
- ``sensor_id``: Sensor identifier (string)
- Other source-specific metadata as defined in ``metadata_variables``

These columns are preserved from ingestion but do not include CF metadata in their names.

Reading Datasets
----------------

From Export Handler
~~~~~~~~~~~~~~~~~~~

Exporters registered via ``IonbeamClient.register_export_handler()`` receive datasets as streaming Arrow batches:

.. code-block:: python

    from ionbeam_client.models import DataSetAvailableEvent
    
    async def export_handler(event: DataSetAvailableEvent, batch_stream):
        # event.dataset_location: object storage key
        # event.metadata.name: dataset name
        # event.start_time / end_time: window bounds
        
        async for batch in batch_stream:
            # batch is pyarrow.RecordBatch
            schema = batch.schema
            # schema.names: ['timestamp', 'latitude', 'longitude', 'air_temperature__degC__2.0__point__PT0S', ...]

Parsing Column Names
--------------------

The ``CanonicalVariable`` model parses column names back into structured metadata:

.. code-block:: python

    from ionbeam_client.models import CanonicalVariable
    
    col = "air_temperature__degC__2.0__point__PT0S"
    var = CanonicalVariable.from_canonical_name(col)
    
    var.standard_name  # "air_temperature"
    var.cf_unit        # "degC"
    var.level          # 2.0
    var.method         # "point"
    var.period         # timedelta(0)

This enables exporters to:

- Filter columns by variable type
- Convert units using CF conventions
- Map to target schema requirements (e.g., ODB varno)

Unit Conversion
---------------

CF units can be converted using the ``cf_units`` library:

.. code-block:: python

    import cf_units
    from ionbeam_client.models import CanonicalVariable
    
    col = "air_temperature__degC__2.0__point__PT0S"
    var = CanonicalVariable.from_canonical_name(col)
    
    from_unit = cf_units.Unit(var.cf_unit)  # degC
    to_unit = cf_units.Unit("K")             # Kelvin
    
    values_k = from_unit.convert(df[col].to_numpy(), to_unit)

See the ECMWF exporter for a complete example of unit conversion to ODB format.