Dataset Schema
==============

.. warning::

   This schema is under active development and may change significantly. Do not rely on it as a stable contract yet.

Datasets produced by IonBeam are streamed to exporters as Arrow RecordBatches over the Flight endpoint (see :ref:`flight-interface:Reading Datasets (GetFlightInfo / DoGet)`). Each dataset represents a single time window of aggregated observations. Column names are plain identifiers; everything a consumer needs to interpret a column travels as Arrow field metadata, so a batch is self-describing without access to the source's code.

Declaring a Schema
------------------

A source declares its dataset with ``IngestionMetadata``: the dataset name, a version, and a ``DatasetSchema`` listing the time axis, coordinates, variables, and tags, all by canonical name. A source renames its feed's raw columns inside its own transform, before the frames reach the client library.

The ``ib_`` prefix is the platform's namespace and no declared name may use it. The declared time column is the phenomenon time — the actual UTC instant each observation is about, never a nominal, receipt, or reference time — and it keeps its declared name end-to-end; consumers locate it by its ``role=time`` field metadata, exactly as latitude and longitude are located by their axis and CRS metadata. Geographic datasets additionally gain the synthesized ``ib_geometry`` and ``ib_id`` columns at build time. Any other name is free, including the plain words a source's own standard uses (``time``, ``year``, ``source``, …), so a feed's native schema can be described as closely as its standard allows.

.. code-block:: python

    from ionbeam_client.models import (
        CfSemantics, DatasetSchema, IngestionMetadata, Tag,
        TimeCoordinate, Variable, geographic_point_coordinates,
    )

    metadata = IngestionMetadata(
        version=1,
        name="weather",
        dataset_schema=DatasetSchema(
            time=TimeCoordinate(),
            coordinates=geographic_point_coordinates(),
            variables=[
                Variable(
                    name="air_temperature",
                    semantics=CfSemantics(standard_name="air_temperature", level=2.0,
                                          cell_method="point", period="PT0S"),
                    unit="degC",
                ),
            ],
            tags=[Tag(name="station_id")],
        ),
    )

Semantics
---------

A variable's governed identity is its ``semantics``: a typed model discriminated on ``scheme``. Each scheme uses its own standard's vocabulary and validates its own shape at declaration time.

``CfSemantics``
   ``standard_name`` from the CF Standard Name Table, with optional ``level`` (sensor height in metres), ``cell_method`` (CF Conventions §7.3), and ``period`` (ISO-8601 duration).

A variable with no semantics is an ungoverned named column: it is stored and served normally, and exporters that match on semantics skip it.

Exporters match variables on the *quantity* a declaration denotes. The ODB exporter reduces semantics through ``ecmwf.varno_map.quantity``, which drops ``level``, ``period``, and the point-vs-mean distinction while keeping quantity-changing methods (``sum``, ``minimum``, ``maximum``), then looks the result up in its in-code varno map. The map therefore never mirrors any source's declaration flavour. The declared ``unit`` is a sibling field, converted to each target's expected unit with ``cf_units``.

Coordinates: CRS and Units
--------------------------

IonBeam interprets geographic coordinates in ``EPSG:4326``/``CRS84`` only, and does not reproject; a source in another CRS reprojects before ingesting. Coordinates declared with x/y axes in any other CRS are stored and served untouched, but every geo product (the GeoParquet projection, EDR, ODB geolocation) skips them, and registration logs a warning saying so.

Coordinates whose values IonBeam interprets are structural, the same tier as the time axis, and their units are enforced at registration: a geographic x/y coordinate must declare a unit convertible to degrees, and a z coordinate carrying ``CfSemantics(standard_name="altitude")`` one convertible to metres. Registration fails otherwise. Exporters convert from the declared unit to their target's expected unit (the ODB exporter writes ``lat@hdr``/``lon@hdr`` in degrees and ``stalt@hdr`` in metres), so an altitude declared in feet is legal and arrives converted. Units on all other coordinates are validated best-effort: a warning when they do not parse, never a rejection.

Arrow Field Metadata
--------------------

``canonical_record_batches`` aligns a source's frames to the declared schema and stamps each Arrow field:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Key
     - Content
   * - ``ionbeam.role``
     - ``time`` | ``coordinate`` | ``value`` | ``tag``
   * - ``ionbeam.axis`` / ``ionbeam.crs``
     - Spatial role (``x``/``y``/``z``) and CRS for coordinates
   * - ``ionbeam.semantics``
     - The semantics model as canonical JSON, e.g. ``{"scheme":"cf","standard_name":"air_temperature","level":2.0}``
   * - ``ionbeam.unit``
     - Declared unit (UDUNITS string)
   * - ``ionbeam.ancillary_of``
     - Names of the variables this column qualifies (QC flags)

Schema-level metadata carries ``ionbeam.schema_hash`` (the declared contract's hash, as returned by registration) and, on built datasets, ``ionbeam.dataset``, the server-side production descriptor.

Reading Datasets
----------------

Exporters registered via ``IonbeamClient.register_export_handler()`` receive datasets as streaming Arrow batches and read structure and semantics back through ``ionbeam_client.schema_meta``:

.. code-block:: python

    from ionbeam_client.models import CfSemantics, DataSetAvailableEvent
    from ionbeam_client.schema_meta import (
        find_coordinates, semantics, time_field, unit, value_fields,
    )

    async def export_handler(event: DataSetAvailableEvent, batch_stream):
        async for batch in batch_stream:
            schema = batch.schema
            t = time_field(schema)
            lon = find_coordinates(schema, axis="x", crs_kind="geographic")
            for field in value_fields(schema, primary_only=True):
                sem = semantics(field)          # CfSemantics | None
                declared_unit = unit(field)     # UDUNITS string

Unit Conversion
---------------

Declared units convert with the ``cf_units`` library:

.. code-block:: python

    import cf_units

    values_k = cf_units.Unit("degC").convert(values, cf_units.Unit("K"))

See the ECMWF exporter for a complete example of unit conversion to ODB format.
