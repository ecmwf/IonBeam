IonBeam Client
==============

.. warning::

   This library is under active development and may change significantly. Do not rely on it as a stable interface yet.

Python client library for ingesting observations into IonBeam and consuming built datasets. It implements the :doc:`Flight interface <../flight-interface>`: registration, ingestion, trigger subscriptions, and dataset export handlers.

Installation
------------

Wheels are published to ECMWF's package index:

.. code-block:: bash

   pip install ionbeam-client --extra-index-url https://get.ecmwf.int/repository/pypi-private-hosted/simple/

Inside the IonBeam repository the `uv <https://docs.astral.sh/uv/>`__ workspace already provides it. Python 3.12 or later is required.

Writing a Data Source
---------------------

A source declares its dataset once — a name, a contract version, and the schema of the columns it streams. Everything about how the output dataset is built and presented lives server-side, keyed by the dataset name (:ref:`domain:Dataset Configuration`).

.. code-block:: python

    from ionbeam_client.models import (
        DatasetSchema,
        IngestionMetadata,
        Tag,
        TimeCoordinate,
        cf,
        geographic_point_coordinates,
    )

    metadata = IngestionMetadata(
        name="weather_stations",
        version=1,
        dataset_schema=DatasetSchema(
            time=TimeCoordinate(),  # phenomenon time; keeps its declared name ("time")
            coordinates=geographic_point_coordinates(),  # lat/lon in EPSG:4326
            variables=[cf("air_temperature", "degC")],
            tags=[Tag(name="station_id")],
        ),
    )

``cf(name, unit)`` declares a variable whose canonical name is its CF standard name; variables under other vocabularies use ``Variable(name=..., semantics=..., unit=...)`` directly (:doc:`../dataset-schema`). Bump ``version`` on intentional schema changes — the server rejects a changed schema under an unchanged version.

``IonbeamClient.ingest`` registers the dataset and streams batches for a declared time range. Frames from the upstream API become canonical RecordBatches with ``canonical_record_batches``, which projects each frame onto the declared schema, coerces dtypes, and stamps the schema hash the server verifies:

.. code-block:: python

    import asyncio
    from datetime import datetime, timezone

    import pandas as pd
    from ionbeam_client import IonbeamClient, IonbeamClientConfig
    from ionbeam_client.canonical_stream import canonical_record_batches

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 2, tzinfo=timezone.utc)


    def fetch_frames():
        yield pd.DataFrame(
            {
                "time": pd.date_range(start, end, freq="1h", tz="UTC")[:-1],
                "lat": 52.5,
                "lon": 13.4,
                "air_temperature": [20.0 + i * 0.5 for i in range(24)],
                "station_id": "st-001",
            }
        )


    async def main():
        client = IonbeamClient(IonbeamClientConfig(flight_url="grpc://localhost:8815"))
        async with client:
            await client.ingest(
                batch_stream=canonical_record_batches(fetch_frames(), metadata),
                metadata=metadata,
                start_time=start,
                end_time=end,
            )


    asyncio.run(main())

The declared ``start_time``/``end_time`` is the range this operation claims to have swept — the server tracks coverage against it, so a range with no rows still counts as checked (:ref:`domain:Coverage Claims`). Long streams are fine: the server claims coverage and builds completed windows while the stream is still open.

Running a Triggered Source
--------------------------

The core's scheduler can drive a source: it pushes trigger commands naming the time range to fetch, so scheduling and backfills are configured centrally. Register the handler before connecting; ``run_source`` wires config, signal handling, a liveness endpoint, and the connection lifecycle:

.. code-block:: python

    import asyncio

    from ionbeam_client import IonbeamClient, run_source


    def setup(client: IonbeamClient, shutdown: asyncio.Event) -> None:
        async def handle_window(start, end, trigger_id) -> None:
            await client.ingest(
                batch_stream=canonical_record_batches(fetch_frames(start, end), metadata),
                metadata=metadata,
                start_time=start,
                end_time=end,
                ingestion_id=trigger_id,
            )

        client.register_trigger_handler("weather_stations", handle_window)


    asyncio.run(run_source("weather_stations", {"ionbeam": {"flight_url": "grpc://localhost:8815"}}, setup))

The ``source_name`` must match a ``scheduler.windows`` entry in the core config. A trigger is acknowledged only after the handler completes, so a source that dies mid-fetch has the trigger redelivered.

Writing an Exporter
-------------------

An exporter subscribes to dataset availability. The handler receives each event with a live Flight connection and streams the data itself — :ref:`dataset-schema:Reading Datasets` shows a complete handler, including how to read structure and semantics from the streamed schema:

.. code-block:: python

    client = IonbeamClient(IonbeamClientConfig(flight_url="grpc://localhost:8815"))
    client.register_export_handler(
        exporter_name="my_exporter",
        handler=export_handler,
        dataset_filter={"weather_stations"},  # omit to receive every dataset
    )

Run it under ``run_source`` like a data source. The event is acknowledged only after the handler returns; raising leaves it pending for redelivery, and a revisable window that rebuilds arrives as a fresh event, so handlers must be idempotent. Replicas sharing an ``exporter_name`` split the event stream between them.

The bundled sources and exporters under ``data-sources/`` and ``exporters/`` in the repository are complete working integrations to crib from.

Configuration
-------------

.. autoclass:: ionbeam_client.config.IonbeamClientConfig
   :members:
   :exclude-members: model_config, model_fields, model_computed_fields, flight_url, retry_delay, write_batch_size

Client API
----------

.. autoclass:: ionbeam_client.client.IonbeamClient
   :members:
   :special-members: __init__, __aenter__, __aexit__
