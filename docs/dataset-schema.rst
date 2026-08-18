Dataset Schema
==============

.. warning::

   This schema is under active development and may change significantly. Do not rely on it as a stable contract yet.

IonBeam streams built datasets as Arrow RecordBatches over the Flight endpoint (see :ref:`flight-interface:Reading Datasets (GetFlightInfo / DoGet)`). Each build represents one time window. Arrow field metadata describes the role, semantics, and unit of each column, so consumers do not need access to source-specific code.

Declaring a Schema
------------------

A source registers ``IngestionMetadata`` containing a dataset name, a version, and a ``DatasetSchema``. The schema lists the time axis, coordinates, variables, and tags by their canonical names. Source-specific transformations must rename input columns before passing frames to the client library.

IonBeam reserves the ``ib_`` prefix, and dataset declarations cannot use it. The declared time column represents phenomenon time: the UTC instant to which an observation applies, rather than its receipt or reference time. Its name is preserved throughout the pipeline. Consumers locate it through the ``role=time`` field metadata, and locate spatial coordinates through axis and CRS metadata.

At build time, IonBeam adds ``ib_geometry`` and ``ib_id`` to geographic datasets. All other names are available to the source declaration, including names defined by the source's native standard.

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

The optional ``semantics`` field gives a variable an identity from a governed vocabulary. Its typed model is selected by ``scheme`` and validated when the dataset is registered.

``CfSemantics``
   ``standard_name`` from the CF Standard Name Table, with optional ``level`` (sensor height in metres), ``cell_method`` (CF Conventions §7.3), and ``period`` (ISO-8601 duration).

A variable with no semantics is an ungoverned named column: it is stored and served normally, and exporters that match on semantics skip it.

Exporters match variables by the *quantity* represented by their declarations. The ODB exporter derives this quantity with ``ecmwf.varno_map.quantity``. It ignores ``level``, ``period``, and the distinction between point and mean values, but retains methods that change the quantity, such as ``sum``, ``minimum``, and ``maximum``. The result is matched against the exporter's varno map.

Units are declared separately from semantics. Exporters use ``cf_units`` to convert values to the units required by their target format.

Coordinates: CRS and Units
--------------------------

IonBeam interprets geographic coordinates only in ``EPSG:4326`` or ``CRS84`` and does not perform reprojection. Sources using another CRS must reproject their coordinates before ingestion. Coordinates declared with x/y axes in another CRS are stored unchanged but excluded from GeoParquet geometry and ODB geolocation. Registration logs a warning for these coordinates.

Registration validates units for coordinates that IonBeam interprets. Geographic x/y coordinates must use units convertible to degrees. A z coordinate with ``CfSemantics(standard_name="altitude")`` must use a unit convertible to metres. The registration fails if either requirement is not met.

Exporters convert declared coordinate units to the units required by their target. For example, the ODB exporter writes ``lat@hdr`` and ``lon@hdr`` in degrees and ``stalt@hdr`` in metres, so an altitude may be declared in feet. For other coordinates, an invalid unit produces a warning but does not prevent registration.

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

An export handler registered with ``IonbeamClient.register_export_handler()`` receives each availability notification and a Flight connection. The notification contains a ``FlightInfo`` with a ticket for the referenced build. Pass that ticket to ``DoGet``, then use ``ionbeam_client.schema_metadata`` to inspect the streamed schema:

.. code-block:: python

    import pyarrow.flight as flight

    from ionbeam_client import AvailableDataset
    from ionbeam_client.schema_metadata import (
        find_coordinates, semantics, time_field, unit, value_fields,
    )

    def export_handler(connection: flight.FlightClient, event: AvailableDataset) -> None:
        reader = connection.do_get(event.info.endpoints[0].ticket)

        schema = reader.schema
        t = time_field(schema)
        [lon] = find_coordinates(schema, axis="x", crs_kind="geographic")
        for field in value_fields(schema, primary_only=True):
            sem = semantics(field)          # CfSemantics | None
            declared_unit = unit(field)     # UDUNITS string

        for chunk in reader:
            ...                             # chunk.data, sorted by time

Unit Conversion
---------------

Declared units convert with the ``cf_units`` library:

.. code-block:: python

    import cf_units

    values_k = cf_units.Unit("degC").convert(values, cf_units.Unit("K"))

See the ECMWF exporter for a complete example of unit conversion to ODB format.
