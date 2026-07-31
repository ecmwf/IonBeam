Using the Data
==============

Built datasets are canonical GeoParquet files in the object store (:ref:`architecture:Dataset publication`). Every consumer path reads that store or a stream of it: the web API for interactive and geospatial queries, direct Parquet reads for bulk analytics, and Flight for programs that want resolved builds streamed or pushed.

The Web API
-----------

A PyGeoAPI server serves the store as an `OGC API — Features <https://ogcapi.ogc.org/features/>`__ service, one collection per dataset. Each observation is a feature: the declared columns are its properties and the synthesized ``ib_geometry`` its geometry. The base URL is deployment-specific.

List the collections::

    curl "https://<host>/collections?f=json"

Fetch observations for an area and time range::

    curl "https://<host>/collections/meteotracker/items?f=json&limit=100&bbox=5,44,16,55&datetime=2026-07-29T00:00:00Z/2026-07-29T23:59:59Z"

The response is a GeoJSON ``FeatureCollection``; ``numberMatched`` carries the total for paging with ``offset``.

Reading the Store Directly
--------------------------

Bulk consumers can read the GeoParquet files with any Parquet engine; stored objects carry a ``.parquet`` suffix on top of the build key. The layout asks two things of a direct reader:

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

Over Flight
-----------

The Flight endpoint streams resolved builds — the server picks each window's current build for you. Resolve a range with ``GetFlightInfo`` (op ``dataset_range``) and stream it with ``DoGet`` (:ref:`flight-interface:Reading Datasets (GetFlightInfo / DoGet)`), or subscribe to builds as they publish with the client library (:ref:`ionbeam-client/index:Writing an Exporter`).

The Legacy HTTP API
-------------------

``ionbeam-legacy-api`` serves the previous public HTTP API, unchanged, from the new system, so consumers built against the old contract keep working. New integrations should use the web API or Flight.
