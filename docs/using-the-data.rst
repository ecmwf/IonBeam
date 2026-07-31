Using the Data
==============

Built datasets are canonical GeoParquet files in the object store (:ref:`architecture:Dataset publication`). The primary access path is the Flight endpoint, which streams any dataset range as Arrow record batches and pushes build events to subscribers; every exporter is built on it. Bulk analytics can read the store directly.

Over Flight
-----------

The Flight endpoint serves resolved builds: the server picks each window's current build and streams it as Arrow record batches carrying the declared schema. Resolve a range with ``GetFlightInfo`` (op ``dataset_range``) and stream it with ``DoGet`` (:ref:`flight-interface:Reading Datasets (GetFlightInfo / DoGet)`).

Programs that follow a dataset as it publishes subscribe to build events over :ref:`flight-interface:Dataset Events (DoExchange)` and fetch each new build as it lands. The client library packages this loop — an exporter is a subscription plus a transform to the target system (:ref:`ionbeam-client/index:Writing an Exporter`); the ECMWF/ODB exporter works this way.

A separate, non-public PyGeoAPI deployment serves the store as an `OGC API — Features <https://ogcapi.ogc.org/features/>`__ service for internal tooling; it is not an integration surface.

Reading the Store Directly
--------------------------

Bulk analytics can read the GeoParquet files with any Parquet engine; stored objects carry a ``.parquet`` suffix on top of the build key. Flight resolves windows and versions server-side; a direct reader takes on both:

- **Select the partitions yourself.** Predicates on the declared time column do not prune the ``ib_year``/``ib_month``/``ib_day`` partitions; constrain the path (or hive filter) to the days you want, then filter rows by time.
- **Resolve build versions.** A window's current build is its highest ``-v<N>-``; superseded files stay beside it until the sweep removes them, so a bare glob double-counts revised windows.

With DuckDB, both in one query::

    WITH builds AS (
        SELECT *,
            regexp_extract(filename, '/(\d{8}T\d{6}_[^-]+)-v', 1) AS window_name,
            CAST(regexp_extract(filename, '-v(\d+)-', 1) AS INTEGER) AS build_version
        FROM read_parquet('s3://<bucket>/<prefix>/meteotracker/ib_year=2026/ib_month=07/ib_day=*/*.parquet',
                          filename = true, hive_partitioning = true)
    )
    SELECT * EXCLUDE (filename, window_name, build_version)
    FROM builds
    QUALIFY build_version = max(build_version) OVER (PARTITION BY window_name);

Each file is one window, written time-sorted, with the declared schema in Arrow field metadata and the build's provenance in the Parquet footer (key ``ionbeam.build``).

The Legacy HTTP API
-------------------

``ionbeam-legacy-api`` serves the previous public HTTP API, unchanged, from the new system, so consumers built against the old contract keep working. New integrations should use Flight.
