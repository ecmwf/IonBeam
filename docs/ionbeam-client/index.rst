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

A source declares a dataset name, a contract version, and the schema of the columns it sends. The core stores build and presentation settings separately, keyed by dataset name (:ref:`domain:Dataset Configuration`).

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

``cf(name, unit)`` declares a variable whose canonical name is its CF standard name. For other vocabularies, construct ``Variable(name=..., semantics=..., unit=...)`` directly (:doc:`../dataset-schema`). Increment ``version`` when changing the schema; registration rejects a changed schema that retains the previous version.

``IonbeamClient.ingest`` registers the dataset and streams batches for a declared time range. Use ``canonical_record_batches`` to project upstream frames onto the declared schema, coerce data types, and attach the schema hash required by the server:

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

``start_time`` and ``end_time`` define the range checked by the ingestion operation. The core records coverage for that range even if it contains no rows (:ref:`domain:Coverage Claims`). A long-running stream can produce completed windows before it closes.

Running a Triggered Source
--------------------------

The core scheduler can send a source the time ranges it should fetch. This allows schedules and backfills to be configured centrally. Register the trigger handler before connecting. ``run_source`` loads configuration and manages signals, the liveness endpoint, and the connection lifecycle:

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

An exporter subscribes to dataset notifications. Its handler receives each event with a Flight connection and retrieves the referenced data. :ref:`dataset-schema:Reading Datasets` provides a complete handler, including schema and semantics access:

.. code-block:: python

    client = IonbeamClient(IonbeamClientConfig(flight_url="grpc://localhost:8815"))
    client.register_export_handler(
        exporter_name="my_exporter",
        handler=export_handler,
        dataset_filter={"weather_stations"},  # omit to receive every dataset
    )

Run an exporter with ``run_source``, as for a data source. The client acknowledges an event after the handler returns. If the handler raises an exception, the event remains pending for redelivery. A revision is delivered as a new event, so handlers must be idempotent. Replicas that share an ``exporter_name`` divide that exporter's events between them.

The bundled sources and exporters under ``data-sources/`` and ``exporters/`` provide complete integration examples.

Configuration
-------------

.. autoclass:: ionbeam_client.config.IonbeamClientConfig
    :no-members:

Client API
----------

.. autoclass:: ionbeam_client.client.IonbeamClient
   :members:
   :special-members: __init__, __aenter__, __aexit__
