Using the Data
==============

Built datasets are available through the Flight endpoint. Clients can request the current builds for a time range or subscribe to notifications as new builds are published.

Reading a Range
---------------

Send a ``dataset_range`` command with ``GetFlightInfo`` to resolve the current build of every window that starts in ``[start, end)``. The returned ``FlightInfo`` contains the dataset schema and a ticket. Pass that ticket to ``DoGet`` to stream the rows (:ref:`flight-interface:Reading Datasets (GetFlightInfo / DoGet)`).

.. code-block:: python

    import json

    import pyarrow.flight as flight

    connection = flight.connect("grpc://localhost:8815")
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "op": "dataset_range",
                "dataset": "weather_stations",
                "start": "2026-08-18T04:00:00Z",
                "end": "2026-08-18T05:00:00Z",
            }
        ).encode()
    )

    info = connection.get_flight_info(descriptor)
    for chunk in connection.do_get(info.endpoints[0].ticket):
        ...  # chunk.data: a RecordBatch in the declared schema, sorted by time

The ticket identifies the builds selected when ``GetFlightInfo`` runs. A concurrent rebuild does not change an active read. A later request for the same range resolves the newer build. ``GetFlightInfo`` fails if the range contains no builds.

Arrow field metadata describes the structure and semantics of each batch. Use ``ionbeam_client.schema_metadata`` to read it (:ref:`dataset-schema:Reading Datasets`). Geographic datasets also contain the generated ``ib_geometry`` and ``ib_id`` columns.

Following a Dataset
-------------------

To process builds as they are published, subscribe to :ref:`flight-interface:Dataset Events (DoExchange)`. On each notification, retrieve the referenced build and write it to the target system. The client library implements this workflow for exporters (:ref:`ionbeam-client/index:Writing an Exporter`). The ECMWF/ODB exporter is one example.

A window remains revisable until ``revisable_until``. If late data causes a rebuild before that time, IonBeam publishes a notification with a higher ``version``. Consumers should retain the highest version received for each window (:ref:`domain:Window Lifecycle`).
