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

from ionbeam_client import IonbeamClient, IonbeamClientConfig
from ionbeam_client.models import (
    CfSemantics,
    Coordinate,
    DatasetSchema,
    DataSetAvailableEvent,
    IngestionMetadata,
    Tag,
    TimeCoordinate,
    Variable,
    geographic_point_coordinates,
)
from ionbeam_client.arrow_tools import canonical_record_batches
from ionbeam_client.dataframe_tools import align_to_schema
from ionbeam_client.schema_meta import SCHEMA_HASH
from ionbeam.application.core import IonbeamCore
from ionbeam.datasets import DatasetProductionConfig, DatasetRegistry
from ionbeam.flight.server import IonbeamFlightServer
from ionbeam.handlers.dataset_builder_handler import (
    DatasetBuilderConfig,
    DatasetBuilderHandler,
)
from ionbeam.handlers.dataset_coordinator_handler import (
    DatasetCoordinatorConfig,
    DatasetCoordinatorHandler,
)
from ionbeam.models import RegisteredDatasetMetadata, align_to_aggregation
from ionbeam.handlers.ingestion_handler import IngestionHandler
from ionbeam.messaging import InMemoryEventBus
from ionbeam.scheduler import SourceSchedule, SourceScheduler
from ionbeam.storage.arrow_store import LocalFileSystemStore
from ionbeam.storage.memory_coordination import (
    InMemoryBuildQueue,
    InMemoryRecordStore,
    InMemoryTriggerClaims,
)
from ionbeam.storage.memory_timeseries import InMemoryTimeSeriesDatabase

DATASET = "e2e_test"
CANONICAL_TEMP = "temperature"
WINDOW_START = datetime(2024, 1, 1, 10, tzinfo=timezone.utc)
WINDOW_END = datetime(2024, 1, 1, 11, tzinfo=timezone.utc)
TEMPERATURES = [20.0 + i * 0.5 for i in range(10)]
REGISTRY = DatasetRegistry(
    {DATASET: DatasetProductionConfig(aggregation_span=timedelta(hours=1))}
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
    schedules: list[SourceSchedule],
):
    event_bus = InMemoryEventBus()
    record_store = InMemoryRecordStore()
    queue = InMemoryBuildQueue()
    timeseries_db = InMemoryTimeSeriesDatabase()
    arrow_store = LocalFileSystemStore(tmp_path / "datasets")

    core = IonbeamCore(
        ingestion=IngestionHandler(
            timeseries_db, ingestion_metrics, record_store, REGISTRY
        ),
        coordinator=DatasetCoordinatorHandler(
            DatasetCoordinatorConfig(), record_store, queue, coordinator_metrics, REGISTRY
        ),
        builder=DatasetBuilderHandler(
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
    scheduler = SourceScheduler(
        schedules, core.trigger_source, InMemoryTriggerClaims().try_claim
    )

    server = IonbeamFlightServer("grpc://localhost:0", core)
    server.spawn(core.start()).result(timeout=5)
    server.spawn(scheduler.start()).result(timeout=5)
    try:
        yield RunningIonbeam(
            url=f"grpc://localhost:{server.port}", server=server, event_bus=event_bus
        )
    finally:
        server.spawn(scheduler.stop()).result(timeout=5)
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
        schedules=[],
    ) as running:
        yield running


LIVE_SOURCE = "live_source"
LIVE_SCHEDULE = [
    SourceSchedule(
        source_name=LIVE_SOURCE,
        window_size=timedelta(hours=1),
        trigger_interval=timedelta(seconds=1),
        window_lag=timedelta(hours=2),
    )
]


@pytest.fixture
def scheduled_ionbeam(
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
        LIVE_SCHEDULE,
    ) as running:
        yield running


def _metadata() -> IngestionMetadata:
    return IngestionMetadata(
        name=DATASET,
        dataset_schema=DatasetSchema(
            time=TimeCoordinate(),
            coordinates=geographic_point_coordinates(),
            variables=[
                Variable(
                    name="temperature",
                    semantics=CfSemantics(standard_name="air_temperature"),
                    unit="deg_C",
                )
            ],
            tags=[Tag(name="station_id")],
        ),
    )


def _observation_frame() -> pd.DataFrame:
    n = len(TEMPERATURES)
    return pd.DataFrame(
        {
            "time": list(
                pd.date_range(WINDOW_START, periods=n, freq="6min", tz="UTC")
            ),
            "lat": [52.5] * n,
            "lon": [13.4] * n,
            "temperature": TEMPERATURES,
            "station_id": ["test_station"] * n,
        }
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
                "op": "dataset",
                "dataset": DATASET,
                "start": start.isoformat(),
                "end": end.isoformat(),
            }
        ).encode("utf-8")
    )


def _read_event(connection, event):
    reader = connection.do_get(
        flight.Ticket(
            json.dumps(
                {"op": "dataset", "locations": event.dataset_locations}
            ).encode()
        )
    )
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
    assert isinstance(event, DataSetAvailableEvent)
    assert event.metadata.name == DATASET
    assert event.start_time == WINDOW_START
    assert event.end_time == WINDOW_END
    assert event.dataset_locations

    df = pa.Table.from_batches(batches).to_pandas()
    assert len(df) == len(TEMPERATURES)
    assert {
        "time",
        "lat",
        "lon",
        CANONICAL_TEMP,
        "station_id",
    } <= set(df.columns)
    assert df["time"].is_monotonic_increasing
    assert df["time"].iloc[0] == WINDOW_START
    assert list(df[CANONICAL_TEMP]) == TEMPERATURES
    assert set(df["station_id"]) == {"test_station"}
    assert set(df["lat"]) == {52.5}
    assert set(df["lon"]) == {13.4}


async def test_dataset_is_built_and_exported_while_ingest_stream_is_still_open(ionbeam):
    """A continuous stream must yield datasets as windows settle — not only
    when the stream ends."""
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
    assert isinstance(event, DataSetAvailableEvent)
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

        assert info.total_records == len(TEMPERATURES)
        assert CANONICAL_TEMP in info.schema.names

        table = raw.do_get(info.endpoints[0].ticket).read_all()
        assert table.num_rows == len(TEMPERATURES)
        assert sorted(table.column(CANONICAL_TEMP).to_pylist()) == TEMPERATURES
    finally:
        raw.close()


async def test_flight_info_for_unbuilt_window_errors(ionbeam):
    raw = flight.connect(ionbeam.url)
    try:
        with pytest.raises(flight.FlightError, match="dataset window not built"):
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


def test_register_then_ingest_happy_path_raw_flight(ionbeam, tmp_path):
    metadata = _metadata()
    raw = flight.connect(ionbeam.url)
    try:
        results = list(
            raw.do_action(
                flight.Action(
                    "register_dataset",
                    metadata.model_dump_json().encode("utf-8"),
                )
            )
        )
        assert json.loads(results[0].body.to_pybytes())["schema_hash"] == metadata.schema_hash()

        # registration lands in the durable log, content-addressed by its hash
        log = (
            tmp_path / "datasets" / "registrations"
            / metadata.name / f"{metadata.schema_hash()}.json"
        )
        registered = RegisteredDatasetMetadata.model_validate_json(log.read_text())
        assert registered.metadata == metadata
        assert registered.registered_at is not None

        batch = _canonical_batch(metadata)
        writer, reader = raw.do_put(_ingest_descriptor(metadata), batch.schema)
        writer.write_batch(batch)
        writer.done_writing()
        response = json.loads(reader.read().to_pybytes().decode("utf-8"))
        assert response["rows"] == len(TEMPERATURES)
    finally:
        raw.close()


def test_drifted_stream_schema_rejected_with_column_named(ionbeam):
    metadata = _metadata()
    raw = flight.connect(ionbeam.url)
    try:
        list(
            raw.do_action(
                flight.Action(
                    "register_dataset",
                    metadata.model_dump_json().encode("utf-8"),
                )
            )
        )
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
    finally:
        raw.close()


def test_registration_rejects_uninterpretable_structural_coordinate_units(ionbeam):
    """Geographic x/y are structural — core interprets their values (GeoParquet
    geometry, exporter geolocation) — so a unit that cannot mean degrees is
    rejected at the edge, before anything ingests under it."""
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
    raw = flight.connect(ionbeam.url)
    try:
        with pytest.raises(Exception, match="convertible to degrees"):
            list(
                raw.do_action(
                    flight.Action(
                        "register_dataset", bad.model_dump_json().encode("utf-8")
                    )
                )
            )
    finally:
        raw.close()


def test_reregistration_changed_map_same_version_rejected(ionbeam):
    metadata = _metadata()
    changed = _metadata().model_copy(
        update={
            "dataset_schema": DatasetSchema(
                time=TimeCoordinate(),
                coordinates=geographic_point_coordinates(),
                variables=[Variable(name="temperature", unit="K")],
                tags=[Tag(name="station_id")],
            )
        }
    )
    raw = flight.connect(ionbeam.url)
    try:
        list(
            raw.do_action(
                flight.Action("register_dataset", metadata.model_dump_json().encode("utf-8"))
            )
        )
        with pytest.raises(Exception, match="different schema hash"):
            list(
                raw.do_action(
                    flight.Action(
                        "register_dataset",
                        changed.model_dump_json().encode("utf-8"),
                    )
                )
            )
    finally:
        raw.close()


def test_identical_reregistration_is_noop(ionbeam):
    metadata = _metadata()
    raw = flight.connect(ionbeam.url)
    try:
        first = list(
            raw.do_action(
                flight.Action("register_dataset", metadata.model_dump_json().encode("utf-8"))
            )
        )
        second = list(
            raw.do_action(
                flight.Action("register_dataset", metadata.model_dump_json().encode("utf-8"))
            )
        )
        assert json.loads(first[0].body.to_pybytes()) == json.loads(
            second[0].body.to_pybytes()
        )
    finally:
        raw.close()


async def test_scheduler_drives_the_full_loop_unattended(scheduled_ionbeam):
    """The system runs itself: the scheduler emits a lagged trigger window, the
    source fetches and ingests it, and the built dataset is pushed to the exporter
    — no manual ingest or trigger anywhere."""
    span = timedelta(hours=1)
    triggers = []
    received = []

    client = IonbeamClient(IonbeamClientConfig(flight_url=scheduled_ionbeam.url))

    async def fetch_on_trigger(start, end, trigger_id):
        triggers.append((start, end))
        if len(triggers) > 1:  # the schedule re-fires every second; deliver once
            return
        aligned_start = align_to_aggregation(start, span)
        aligned_end = align_to_aggregation(end, span)
        if aligned_end < end:
            aligned_end += span
        timestamps = pd.date_range(
            aligned_start, aligned_end, freq="6min", inclusive="left", tz="UTC"
        )
        frame = pd.DataFrame(
            {
                "time": list(timestamps),
                "lat": [52.5] * len(timestamps),
                "lon": [13.4] * len(timestamps),
                "temperature": [20.0 + i for i in range(len(timestamps))],
                "station_id": ["live_station"] * len(timestamps),
            }
        )
        metadata = _metadata()
        await client.ingest(
            canonical_record_batches([frame], metadata),
            metadata,
            aligned_start,
            aligned_end,
            ingestion_id=trigger_id,
        )

    def export_handler(connection, event):
        received.append((event, _read_event(connection, event)))

    client.register_trigger_handler(LIVE_SOURCE, fetch_on_trigger)
    client.register_export_handler(
        "live-exporter", export_handler, dataset_filter={DATASET}
    )

    async with client:
        # test-only sync: the export subscription must be registered on the bus
        # before the builder publishes, or the push is lost (triggers self-heal
        # because the schedule re-fires every second)
        await _poll(
            lambda: scheduled_ionbeam.event_bus._dataset_subs,
            message="export subscription",
        )
        await _poll(lambda: received, timeout=20.0, message="scheduler-driven push")

    start, end = triggers[0]
    assert end - start == timedelta(hours=1)  # window_size
    lag = datetime.now(timezone.utc) - end
    assert lag >= timedelta(hours=2) - timedelta(seconds=1)  # window_lag honored

    event, batches = received[0]
    assert event.metadata.name == DATASET
    assert event.end_time - event.start_time == span
    assert event.start_time >= align_to_aggregation(start, span)
    assert event.end_time <= align_to_aggregation(end, span) + span

    df = pa.Table.from_batches(batches).to_pandas()
    assert len(df) == 10  # one aligned hour at 6-minute cadence
    assert CANONICAL_TEMP in df.columns
    assert set(df["station_id"]) == {"live_station"}
