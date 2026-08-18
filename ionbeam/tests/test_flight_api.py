# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""End-to-end behavior over the public Arrow Flight interface.

A real ``IonbeamFlightServer`` + ``IonbeamCore`` on in-memory backends, exercised
exclusively through the ``ionbeam_client`` SDK (plus raw Flight calls for the
wire-contract verbs the SDK does not wrap).
"""

import asyncio
import json
from typing import AsyncIterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight
import pytest
from prometheus_client import CollectorRegistry

from ionbeam_client import AvailableDataset, IonbeamClient, IonbeamClientConfig
from ionbeam_client.models import (
    Coordinate,
    DatasetSchema,
    IngestionMetadata,
    Tag,
    TimeCoordinate,
    Variable,
    geographic_point_coordinates,
)
from ionbeam_client.canonical_stream import canonical_record_batches
from ionbeam_client.alignment import align_to_schema
from ionbeam_client.schema_metadata import SCHEMA_HASH, dataset_metadata
from conftest import observation_frame, weather_metadata
from ionbeam.application.core import IonbeamCore
from ionbeam.datasets import DatasetBuildConfig, DatasetRegistry
from ionbeam.flight.server import IonbeamFlightServer
from ionbeam.handlers.dataset_builder import (
    DatasetBuilderConfig,
    DatasetBuilder,
)
from ionbeam.handlers.dataset_coordinator import (
    DatasetCoordinatorConfig,
    DatasetCoordinator,
)
from ionbeam.provenance import RegisteredDatasetMetadata
from ionbeam.handlers.ingestion import Ingestion
from ionbeam.messaging import InMemoryEventBus
from ionbeam.observability import FlightMetrics
from ionbeam.storage.arrow_store import LocalFileSystemStore
from ionbeam.storage.memory_coordination import (
    InMemoryBuildQueue,
    InMemoryCoordinationStore,
)
from ionbeam.storage.memory_timeseries import InMemoryTimeSeriesDatabase

DATASET = "e2e_test"
CANONICAL_TEMP = "temperature"
WINDOW_START = datetime(2024, 1, 1, 10, tzinfo=timezone.utc)
WINDOW_END = datetime(2024, 1, 1, 11, tzinfo=timezone.utc)
TEMPERATURES = [20.0 + i * 0.5 for i in range(10)]
REGISTRY = DatasetRegistry(
    {DATASET: DatasetBuildConfig(aggregation_span=timedelta(hours=1))}
)


@dataclass
class RunningIonbeam:
    url: str
    server: IonbeamFlightServer
    event_bus: InMemoryEventBus


@contextmanager
def _running_ionbeam(
    tmp_path,
    ingestion_metrics,
    coordinator_metrics,
    builder_metrics,
):
    event_bus = InMemoryEventBus()
    record_store = InMemoryCoordinationStore()
    queue = InMemoryBuildQueue()
    timeseries_db = InMemoryTimeSeriesDatabase()
    arrow_store = LocalFileSystemStore(tmp_path / "datasets")

    core = IonbeamCore(
        ingestion=Ingestion(
            timeseries_db, ingestion_metrics, record_store, REGISTRY
        ),
        coordinator=DatasetCoordinator(
            # fixture windows are dated 2024; a huge retention keeps them provisional
            DatasetCoordinatorConfig(retention=timedelta(days=3650)),
            record_store,
            queue,
            coordinator_metrics,
            REGISTRY,
        ),
        builder=DatasetBuilder(
            DatasetBuilderConfig(poll_interval_seconds=0.05),
            record_store,
            queue,
            timeseries_db,
            builder_metrics,
            arrow_store,
            event_publisher=event_bus.publish_dataset_available,
            registry=REGISTRY,
        ),
        record_store=record_store,
        arrow_store=arrow_store,
        event_bus=event_bus,
    )
    server = IonbeamFlightServer(
        "grpc://localhost:0", core, FlightMetrics(CollectorRegistry())
    )
    server.spawn(core.start()).result(timeout=5)
    try:
        yield RunningIonbeam(
            url=f"grpc://localhost:{server.port}", server=server, event_bus=event_bus
        )
    finally:
        server.spawn(core.stop()).result(timeout=5)
        server.shutdown()


@pytest.fixture
def ionbeam(
    tmp_path,
    ingestion_metrics,
    coordinator_metrics,
    builder_metrics,
):
    with _running_ionbeam(
        tmp_path,
        ingestion_metrics,
        coordinator_metrics,
        builder_metrics,
    ) as running:
        yield running


def _metadata() -> IngestionMetadata:
    return weather_metadata(name=DATASET)


def _observation_frame() -> pd.DataFrame:
    return observation_frame(
        pd.date_range(WINDOW_START, periods=len(TEMPERATURES), freq="6min", tz="UTC"),
        TEMPERATURES,
    )


def _observation_batches(metadata: IngestionMetadata) -> AsyncIterator[pa.RecordBatch]:
    return canonical_record_batches([_observation_frame()], metadata)


def _batch_with_schema_hash(
    batch: pa.RecordBatch, metadata: IngestionMetadata
) -> pa.RecordBatch:
    schema = batch.schema.with_metadata(
        {**(batch.schema.metadata or {}), SCHEMA_HASH.encode(): metadata.schema_hash().encode()}
    )
    return pa.RecordBatch.from_arrays(
        [batch.column(i) for i in range(batch.num_columns)], schema=schema
    )


def _canonical_batch(metadata: IngestionMetadata) -> pa.RecordBatch:
    """A canonical-named, hash-stamped batch for raw do_put tests."""
    df = align_to_schema(_observation_frame(), metadata)
    return _batch_with_schema_hash(
        pa.RecordBatch.from_pandas(df, preserve_index=False), metadata
    )


def _ingest_descriptor(metadata: IngestionMetadata) -> flight.FlightDescriptor:
    return flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "op": "ingest",
                "id": "00000000-0000-0000-0000-000000000001",
                "metadata": metadata.model_dump(mode="json"),
                "start": WINDOW_START.isoformat(),
                "end": WINDOW_END.isoformat(),
            }
        ).encode("utf-8")
    )


async def _stream(*batches):
    for batch in batches:
        yield batch


async def _poll(condition, timeout: float = 15.0, message: str = "condition"):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if condition():
            return
        await asyncio.sleep(0.05)
    pytest.fail(f"Timed out waiting for {message}")


def _dataset_descriptor(start: datetime, end: datetime) -> flight.FlightDescriptor:
    return flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "op": "dataset_range",
                "dataset": DATASET,
                "start": start.isoformat(),
                "end": end.isoformat(),
            }
        ).encode("utf-8")
    )


def _read_event(connection, event):
    reader = connection.do_get(event.info.endpoints[0].ticket)
    return [chunk.data for chunk in reader]


async def test_ingested_window_is_built_and_pushed_to_export_handler(ionbeam):
    received = []

    def export_handler(connection, event):
        received.append((event, _read_event(connection, event)))

    client = IonbeamClient(IonbeamClientConfig(flight_url=ionbeam.url))
    client.register_export_handler(
        "e2e-exporter", export_handler, dataset_filter={DATASET}
    )

    async with client:
        # test-only sync: the DoExchange subscription must be registered on the
        # bus before the builder publishes, or the event is lost
        await _poll(
            lambda: ionbeam.event_bus._dataset_subs, message="export subscription"
        )

        metadata = _metadata()
        command = await client.ingest(
            _observation_batches(metadata), metadata, WINDOW_START, WINDOW_END
        )
        assert command.start_time == WINDOW_START
        assert command.end_time == WINDOW_END

        await _poll(lambda: received, message="dataset available push")

    event, batches = received[0]
    assert isinstance(event, AvailableDataset)
    assert event.dataset == DATASET
    assert (
        event.start_time,
        event.end_time,
        event.version,
        event.revisable_until,
    ) == (
        WINDOW_START,
        WINDOW_END,
        1,
        WINDOW_END + DatasetBuilderConfig().retention,
    )
    assert CANONICAL_TEMP in event.info.schema.names
    assert dataset_metadata(event.info.schema).name == DATASET

    df = pa.Table.from_batches(batches).to_pandas()
    delivered = df[["time", "lat", "lon", CANONICAL_TEMP, "station_id"]].rename(
        columns={CANONICAL_TEMP: "temperature"}
    )
    pd.testing.assert_frame_equal(
        delivered, _observation_frame(), check_dtype=False
    )


async def test_dataset_is_built_and_exported_while_ingest_stream_is_still_open(ionbeam):
    """A continuous stream yields datasets as windows settle, while still open."""
    received = []

    def export_handler(connection, event):
        received.append((event, _read_event(connection, event)))

    client = IonbeamClient(IonbeamClientConfig(flight_url=ionbeam.url))
    client.register_export_handler(
        "live-exporter", export_handler, dataset_filter={DATASET}
    )

    def _frame(timestamps: list[datetime]) -> pd.DataFrame:
        n = len(timestamps)
        return pd.DataFrame(
            {
                "time": timestamps,
                "lat": [52.5] * n,
                "lon": [13.4] * n,
                "temperature": TEMPERATURES[:n],
                "station_id": ["test_station"] * n,
            }
        )

    metadata = _metadata()
    stream_may_end = asyncio.Event()

    async def continuous_batches():
        first_window = list(
            pd.date_range(WINDOW_START, periods=10, freq="6min", tz="UTC")
        )
        async for batch in canonical_record_batches([_frame(first_window)], metadata):
            yield batch
        # crossing the 11:00 boundary publishes the first window's claim
        crossing = [WINDOW_END, WINDOW_END + timedelta(minutes=6)]
        async for batch in canonical_record_batches([_frame(crossing)], metadata):
            yield batch
        await stream_may_end.wait()
        tail = [WINDOW_END + timedelta(minutes=12)]
        async for batch in canonical_record_batches([_frame(tail)], metadata):
            yield batch

    async with client:
        await _poll(
            lambda: ionbeam.event_bus._dataset_subs, message="export subscription"
        )

        ingest_task = asyncio.create_task(
            client.ingest(
                continuous_batches(),
                metadata,
                WINDOW_START,
                WINDOW_END + timedelta(hours=1),
            )
        )
        try:
            await _poll(lambda: received, message="dataset built mid-stream")
            assert not ingest_task.done(), "stream must still be open"
        finally:
            stream_may_end.set()
            await ingest_task

    event, batches = received[0]
    assert isinstance(event, AvailableDataset)
    assert event.start_time == WINDOW_START
    assert event.end_time == WINDOW_END

    df = pa.Table.from_batches(batches).to_pandas()
    assert len(df) == 10
    assert df["time"].iloc[0] == WINDOW_START
    assert df["time"].max() < WINDOW_END


async def test_built_window_is_fetchable_via_flight_info_and_do_get(ionbeam):
    async with IonbeamClient(IonbeamClientConfig(flight_url=ionbeam.url)) as client:
        metadata = _metadata()
        await client.ingest(
            _observation_batches(metadata), metadata, WINDOW_START, WINDOW_END
        )

    raw = flight.connect(ionbeam.url)
    try:
        info = None

        def built():
            nonlocal info
            try:
                info = raw.get_flight_info(
                    _dataset_descriptor(WINDOW_START, WINDOW_END)
                )
                return True
            except flight.FlightError:
                return False

        await _poll(built, message="window build")

        assert CANONICAL_TEMP in info.schema.names

        table = raw.do_get(info.endpoints[0].ticket).read_all()
        assert table.num_rows == len(TEMPERATURES)
        assert sorted(table.column(CANONICAL_TEMP).to_pylist()) == TEMPERATURES
    finally:
        raw.close()


async def test_flight_info_for_unbuilt_window_errors(ionbeam):
    raw = flight.connect(ionbeam.url)
    try:
        with pytest.raises(flight.FlightError, match="no builds in range"):
            raw.get_flight_info(_dataset_descriptor(WINDOW_START, WINDOW_END))
    finally:
        raw.close()


async def test_trigger_source_action_reaches_registered_trigger_handler(ionbeam):
    triggers = []

    async def trigger_handler(start, end, trigger_id):
        triggers.append((start, end))

    client = IonbeamClient(IonbeamClientConfig(flight_url=ionbeam.url))
    client.register_trigger_handler("e2e_source", trigger_handler)

    async with client:
        # test-only sync: the trigger subscription must be registered on the bus
        # before the action publishes, or the trigger is lost
        await _poll(
            lambda: ionbeam.event_bus._trigger_subs.get("e2e_source"),
            message="trigger subscription",
        )

        raw = flight.connect(ionbeam.url)
        try:
            results = list(
                raw.do_action(
                    flight.Action(
                        "trigger_source",
                        json.dumps(
                            {
                                "source_name": "e2e_source",
                                "start": WINDOW_START.isoformat(),
                                "end": WINDOW_END.isoformat(),
                            }
                        ).encode("utf-8"),
                    )
                )
            )
            assert results[0].body.to_pybytes() == b"ok"
        finally:
            raw.close()

        await _poll(lambda: triggers, message="trigger delivery")

    assert triggers[0] == (WINDOW_START, WINDOW_END)


async def test_empty_stream_ingest_raises_client_side(ionbeam):
    async with IonbeamClient(IonbeamClientConfig(flight_url=ionbeam.url)) as client:
        with pytest.raises(ValueError, match="Cannot ingest empty data stream"):
            await client.ingest(_stream(), _metadata(), WINDOW_START, WINDOW_END)


@contextmanager
def _raw(ionbeam):
    """A raw Flight connection, for the wire verbs the SDK does not wrap."""
    connection = flight.connect(ionbeam.url)
    try:
        yield connection
    finally:
        connection.close()


def _register(raw, metadata) -> dict:
    results = list(
        raw.do_action(
            flight.Action("register_dataset", metadata.model_dump_json().encode("utf-8"))
        )
    )
    return json.loads(results[0].body.to_pybytes())


def test_registration_lifecycle_then_ingest_over_raw_flight(ionbeam, tmp_path):
    """Registering answers with the schema hash and durably logs the metadata;
    registering the identical schema again answers the same; registering a changed
    schema under the same name is rejected. A registered dataset then accepts rows."""
    metadata = _metadata()
    changed = metadata.model_copy(
        update={
            "dataset_schema": DatasetSchema(
                time=TimeCoordinate(),
                coordinates=geographic_point_coordinates(),
                variables=[Variable(name="temperature", unit="K")],
                tags=[Tag(name="station_id")],
            )
        }
    )

    with _raw(ionbeam) as raw:
        first = _register(raw, metadata)
        assert first["schema_hash"] == metadata.schema_hash()
        assert _register(raw, metadata) == first

        # registration lands in the durable log, content-addressed by its hash
        log = (
            tmp_path / "datasets" / "registrations"
            / metadata.name / f"{metadata.schema_hash()}.json"
        )
        registered = RegisteredDatasetMetadata.model_validate_json(log.read_text())
        assert registered.metadata == metadata
        assert registered.registered_at is not None

        with pytest.raises(Exception, match="different schema hash"):
            _register(raw, changed)

        batch = _canonical_batch(metadata)
        writer, reader = raw.do_put(_ingest_descriptor(metadata), batch.schema)
        writer.write_batch(batch)
        writer.done_writing()
        response = json.loads(reader.read().to_pybytes().decode("utf-8"))
        assert response["rows"] == len(TEMPERATURES)


def test_drifted_stream_schema_rejected_with_column_named(ionbeam):
    metadata = _metadata()
    with _raw(ionbeam) as raw:
        _register(raw, metadata)
        drifted = pa.RecordBatch.from_pydict(
            {
                "time": [WINDOW_START],
                "lat": [52.5],
                "lon": [13.4],
                "station_id": ["test_station"],
            }
        )
        drifted = _batch_with_schema_hash(drifted, metadata)

        with pytest.raises(Exception, match="temperature"):
            writer, reader = raw.do_put(_ingest_descriptor(metadata), drifted.schema)
            writer.write_batch(drifted)
            writer.done_writing()
            reader.read()
            writer.close()


def test_registration_rejects_uninterpretable_structural_coordinate_units(ionbeam):
    """Geographic x/y are structural: core interprets their values for GeoParquet
    geometry and exporter geolocation. A unit that cannot mean degrees is rejected
    at registration."""
    bad = _metadata().model_copy(
        update={
            "dataset_schema": DatasetSchema(
                time=TimeCoordinate(),
                coordinates=[
                    Coordinate(name="lat", axis="y", crs="EPSG:4326", unit="m"),
                    Coordinate(name="lon", axis="x", crs="EPSG:4326",
                               unit="degrees_east"),
                ],
                variables=[Variable(name="temperature", unit="K")],
                tags=[Tag(name="station_id")],
            )
        }
    )
    with _raw(ionbeam) as raw:
        with pytest.raises(Exception, match="convertible to degrees"):
            _register(raw, bad)


