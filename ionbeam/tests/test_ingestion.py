# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Ingestion of a source's stream: the coverage claims it publishes, the row
provenance it stamps, and the arrival lateness it measures."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4, uuid5

import pandas as pd
import pyarrow as pa
import pytest
import structlog

from ionbeam_client.models import (
    Coordinate,
    DataAvailableEvent,
    DatasetSchema,
    IngestionMetadata,
    Variable,
)
from ionbeam_client.canonical_stream import canonical_arrow_schema
from ionbeam.datasets import DatasetBuildConfig, DatasetRegistry
from ionbeam.handlers.ingestion import Ingestion
from ionbeam.observability import IngestionMetrics
from ionbeam.storage.lateness_histogram import bucket_for

from conftest import observation_frame

DECLARED_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
DECLARED_END = datetime(2024, 1, 1, 6, tzinfo=timezone.utc)
HOUR = timedelta(hours=1)


@pytest.fixture
def make_ingestion(
    timeseries_db,
    ingestion_metrics: IngestionMetrics,
    coordination_store,
):
    """Build a handler whose registry aggregates ``test_dataset`` at ``span``.
    The aggregation span is server-side production config."""

    def _make(
        span: timedelta = timedelta(days=1),
        retention: timedelta = timedelta(days=7),
        dedup_ingestion: bool = False,
    ) -> Ingestion:
        registry = DatasetRegistry(
            {
                "test_dataset": DatasetBuildConfig(
                    aggregation_span=span,
                    dedup_ingestion=dedup_ingestion,
                )
            }
        )
        return Ingestion(
            timeseries_db,
            ingestion_metrics,
            coordination_store,
            registry,
            retention=retention,
        )

    return _make


@pytest.fixture
def ingestion(make_ingestion):
    return make_ingestion()


async def _ingest(
    handler: Ingestion,
    metadata: IngestionMetadata,
    *batches: pa.RecordBatch,
    ingestion_id=None,
    end_time: datetime = DECLARED_END,
) -> list[DataAvailableEvent]:
    """Run one ingestion command to completion, returning every claim it
    published, the final one last."""
    events: list[DataAvailableEvent] = []

    async def record(event: DataAvailableEvent) -> None:
        events.append(event)

    async def stream():
        for batch in batches:
            yield batch

    await handler.ingest(
        ingestion_id=ingestion_id or uuid4(),
        metadata=metadata,
        start_time=DECLARED_START,
        end_time=end_time,
        batches=stream(),
        on_data_available=record,
    )
    return events


def _minutes(count: int, start: datetime = DECLARED_START) -> list[datetime]:
    return list(pd.date_range(start, periods=count, freq="1min", tz="UTC"))


def _at(ticks: float) -> datetime:
    """Six minutes per tick: ten ticks span one PT1H aggregation window."""
    return DECLARED_START + timedelta(minutes=6 * ticks)


def _ago(**delta) -> datetime:
    return pd.Timestamp.now(tz="UTC") - pd.Timedelta(**delta)


async def test_stream_within_one_window_publishes_only_the_final_event(
    ingestion, metadata, canonical_batch
):
    """Default day-long aggregation: an hour of data crosses no boundary."""
    ingestion_id = uuid4()

    events = await _ingest(
        ingestion,
        metadata,
        canonical_batch(_minutes(10)),
        ingestion_id=ingestion_id,
    )

    assert [event.id for event in events] == [uuid5(ingestion_id, "claim-0")]
    assert (events[0].start_time, events[0].end_time) == (
        DECLARED_START,
        DECLARED_END,
    )


async def test_checkpoints_publish_on_boundary_crossings_and_chain_contiguously(
    make_ingestion, metadata, canonical_batch
):
    """A checkpoint fires as the watermark leaves a window, and each one starts
    where the last ended. A batch jumping several windows still checkpoints once."""
    handler = make_ingestion(HOUR)
    ingestion_id = uuid4()
    events: list[DataAvailableEvent] = []
    events_seen_after_batch: list[int] = []

    async def record(event: DataAvailableEvent) -> None:
        events.append(event)

    async def batches():
        yield canonical_batch([_at(0), _at(9)])
        events_seen_after_batch.append(len(events))
        yield canonical_batch([_at(10), _at(12)])
        events_seen_after_batch.append(len(events))
        # one batch jumping three windows still yields a single checkpoint
        yield canonical_batch([_at(15), _at(43)])
        events_seen_after_batch.append(len(events))

    with structlog.testing.capture_logs() as logs:
        await handler.ingest(
            ingestion_id=ingestion_id,
            metadata=metadata,
            start_time=DECLARED_START,
            end_time=DECLARED_END,
            batches=batches(),
            on_data_available=record,
        )

    assert events_seen_after_batch == [0, 1, 2]
    assert [event.id for event in events] == [
        uuid5(ingestion_id, f"claim-{index}") for index in range(3)
    ]
    assert [(event.start_time, event.end_time) for event in events] == [
        (DECLARED_START, _at(12)),
        (_at(12), _at(43)),
        (_at(43), DECLARED_END),
    ]

    assert not any(log["log_level"] == "warning" for log in logs)


async def test_late_data_widens_the_next_checkpoint_to_cover_its_window(
    make_ingestion, metadata, canonical_batch
):
    """Data older than what is already checkpointed must be re-checkpointed under a fresh
    record id, so windows already built from earlier checkpoints get rebuilt."""
    events = await _ingest(
        make_ingestion(HOUR),
        metadata,
        canonical_batch([_at(0), _at(12)]),
        canonical_batch([_at(5), _at(21)]),  # _at(5) is late
    )

    late_checkpoint = events[1]
    assert (late_checkpoint.start_time, late_checkpoint.end_time) == (
        _at(5),
        _at(21),
    )
    assert late_checkpoint.id != events[0].id


async def test_replaying_the_same_command_yields_the_same_record_ids(
    make_ingestion, metadata, canonical_batch
):
    """A retried command produces identical checkpoint ids for identical
    stream and ingestion id, so no spurious window rebuilds."""
    ingestion_id = uuid4()

    async def run() -> list[DataAvailableEvent]:
        return await _ingest(
            make_ingestion(HOUR),
            metadata,
            canonical_batch([_at(0), _at(11)]),
            canonical_batch([_at(12), _at(25)]),
            ingestion_id=ingestion_id,
        )

    assert [e.id for e in await run()] == [e.id for e in await run()]


async def test_rows_are_tagged_with_the_record_that_delivers_them(
    make_ingestion, metadata, canonical_batch, timeseries_db
):
    """Rows written before a checkpoint fires belong to that checkpoint; rows after
    it belong to the next checkpoint (the final record)."""
    events = await _ingest(
        make_ingestion(HOUR),
        metadata,
        canonical_batch([_at(0), _at(12)], [20.0, 21.0], ["st1", "st1"]),
        canonical_batch([_at(13)], [22.0], ["st1"]),
    )

    assert len(events) == 2  # one checkpoint + final
    stored = timeseries_db.stored("test_dataset").to_pandas()
    delivered = {
        record_id: sorted(rows["time"])
        for record_id, rows in stored.groupby("ib_record_id")
    }
    assert delivered == {
        str(events[0].records[0].id): [_at(0)],
        str(events[0].records[1].id): [_at(12)],
        str(events[1].records[0].id): [_at(13)],
    }
    assert {r.id for r in events[0].records}.isdisjoint(
        r.id for r in events[1].records
    )


async def test_dedup_ingestion_writes_only_novel_content(
    make_ingestion, metadata, canonical_batch, timeseries_db
):
    """A sweep source re-fetching history re-delivers identical rows under
    fresh record ids; a delta dataset stores only changed content — and the
    change itself (a QC correction) is kept."""
    handler = make_ingestion(HOUR, dedup_ingestion=True)
    times = [_at(0), _at(2), _at(4)]

    for temperatures in ([20.0, 21.0, 22.0], [20.0, 21.0, 22.0], [20.0, 18.5, 22.0]):
        await _ingest(
            handler, metadata, canonical_batch(times, temperatures, ["st1"] * 3)
        )

    stored = timeseries_db.stored("test_dataset").column("temperature").to_pylist()
    assert sorted(stored) == [18.5, 20.0, 21.0, 22.0]


async def test_a_failed_write_marks_nothing_stored(
    make_ingestion, metadata, canonical_batch, timeseries_db
):
    """Stored-content marks land only after a successful write, so a retry
    after a failed write persists every row."""
    handler = make_ingestion(HOUR, dedup_ingestion=True)
    times = [_at(0), _at(2), _at(4)]
    healthy_write = timeseries_db.write
    outage = {"remaining": 1}

    async def flaky_write(**kwargs):
        if outage["remaining"]:
            outage["remaining"] -= 1
            raise ConnectionError("influx unavailable")
        return await healthy_write(**kwargs)

    timeseries_db.write = flaky_write

    with pytest.raises(ConnectionError):
        await _ingest(handler, metadata, canonical_batch(times))
    await _ingest(handler, metadata, canonical_batch(times))

    stored = timeseries_db.stored("test_dataset")
    assert stored.num_rows == 3
    assert stored.column("temperature").to_pylist() == [20.0, 20.0, 20.0]


async def test_a_raw_stream_is_stored_canonicalized(
    ingestion, metadata, canonical_batch, timeseries_db
):
    """Streamed batches are canonicalized (renames, tz, sort, tags) and written."""
    unsorted = list(reversed(_minutes(10)))

    events = await _ingest(ingestion, metadata, canonical_batch(unsorted))

    assert events[-1].metadata == metadata
    assert (events[-1].start_time, events[-1].end_time) == (
        DECLARED_START,
        DECLARED_END,
    )

    assert timeseries_db.last_write == (
        "test_dataset",
        ["station_id", "ib_record_id"],
        "time",
    )

    stored = timeseries_db.stored("test_dataset").to_pandas()
    assert set(stored.columns) == {
        "time",
        "lat",
        "lon",
        "temperature",
        "station_id",
        "ib_record_id",
    }
    assert str(stored["time"].dt.tz) == "UTC"
    pd.testing.assert_frame_equal(
        stored[["time", "lat", "lon", "temperature", "station_id"]],
        observation_frame(_minutes(10)),
        check_dtype=False,
    )


async def test_lateness_counts_each_novel_datum_once(
    make_ingestion, metadata, canonical_batch, coordination_store
):
    """Every datum's lateness (now minus its own observation time) lands in the
    histogram, bucketed per row. A dedup source re-fetching an overlapping span
    re-delivers rows the write suppresses, in whatever window they fall, so they
    do not re-count; a QC pass that genuinely changes a value does count."""
    handler = make_ingestion(HOUR, dedup_ingestion=True)
    # spread across three windows so each row dedups against its own
    times = [_ago(hours=1)] * 3 + [_ago(hours=6)] * 2
    stations = ["a", "b", "c", "d", "e"]

    await _ingest(
        handler, metadata, canonical_batch(times, [20.0] * 5, stations)
    )
    assert coordination_store.lateness("test_dataset") == {
        bucket_for(3600): 3,
        bucket_for(6 * 3600): 2,
    }

    await _ingest(
        handler, metadata, canonical_batch(times, [20.0] * 5, stations)
    )
    assert sum(coordination_store.lateness("test_dataset").values()) == 5

    # netatmo's QC pass rewrites one value: same station and time, new content
    await _ingest(
        handler,
        metadata,
        canonical_batch(times, [20.0, 18.5, 20.0, 20.0, 20.0], stations),
    )
    assert sum(coordination_store.lateness("test_dataset").values()) == 6


async def test_redelivery_counts_again_without_dedup_ingestion(
    ingestion, metadata, canonical_batch, coordination_store
):
    """The histogram counts what the store writes: a push feed without
    ``dedup_ingestion`` records its rare redeliveries again."""
    batch = canonical_batch([_ago(minutes=m) for m in range(60, 65)])

    await _ingest(ingestion, metadata, batch)
    await _ingest(ingestion, metadata, batch)

    assert sum(coordination_store.lateness("test_dataset").values()) == 10


async def test_lateness_ignores_rows_for_sealed_windows(
    make_ingestion, metadata, canonical_batch, coordination_store
):
    """A row whose window is already final cannot affect any build; letting it
    into the histogram would push the settle gate past the point where waiting
    can help."""
    handler = make_ingestion(HOUR, retention=timedelta(hours=48))

    await _ingest(
        handler, metadata, canonical_batch([_ago(hours=72), _ago(hours=96)])
    )

    assert coordination_store.lateness("test_dataset") == {}


async def test_a_non_geographic_dataset_stores_its_declared_columns(
    make_ingestion, timeseries_db
):
    """Coordinates need not be geographic: a spherical-harmonic feed declares
    integer axes and no tags, and is stored under its own column names."""
    metadata = IngestionMetadata(
        name="test_dataset",
        dataset_schema=DatasetSchema(
            coordinates=[
                Coordinate(name="degree", dtype="int64"),
                Coordinate(name="order", dtype="int64"),
            ],
            variables=[Variable(name="power", dtype="float64")],
        ),
    )
    batch = pa.RecordBatch.from_pydict(
        {"time": [DECLARED_START], "degree": [2], "order": [1], "power": [42.0]},
        schema=canonical_arrow_schema(metadata),
    )

    await _ingest(make_ingestion(), metadata, batch)

    assert timeseries_db.last_write == ("test_dataset", ["ib_record_id"], "time")
    stored = timeseries_db.stored("test_dataset")
    assert stored.column_names == ["time", "degree", "order", "power", "ib_record_id"]


async def test_a_stream_missing_a_declared_column_is_rejected_by_name(
    ingestion, metadata
):
    """A stream that omits a declared column names the column it dropped."""
    partial = pa.RecordBatch.from_pydict(
        {
            "time": [DECLARED_START],
            "lat": [52.5],
            "temperature": [20.0],
            "station_id": ["test_station"],
        },
        schema=canonical_arrow_schema(metadata).remove(
            canonical_arrow_schema(metadata).get_field_index("lon")
        ),
    )

    with pytest.raises(ValueError, match="lon"):
        await _ingest(ingestion, metadata, partial)


async def test_rows_carrying_only_ancillary_values_are_dropped(
    make_ingestion, timeseries_db
):
    """A row whose primary variables are all null carries no observation, whatever
    its ancillary columns hold; a row with a value is kept."""
    metadata = IngestionMetadata(
        name="test_dataset",
        dataset_schema=DatasetSchema(
            variables=[
                Variable(name="temperature"),
                Variable(name="qc", dtype="int64", ancillary_of=["temperature"]),
            ]
        ),
    )
    batch = pa.RecordBatch.from_pydict(
        {
            "time": [DECLARED_START, DECLARED_START + timedelta(minutes=1)],
            "temperature": [None, 20.0],
            "qc": [1, None],
        },
        schema=canonical_arrow_schema(metadata),
    )

    await _ingest(make_ingestion(), metadata, batch)

    stored = timeseries_db.stored("test_dataset")
    assert stored.column("temperature").to_pylist() == [20.0]


async def test_stream_with_undeclared_column_types_is_rejected(
    ingestion, metadata, canonical_batch
):
    """A correctly-named, correctly-stamped stream still fails the contract
    if a column's Arrow type differs from the declared dtype."""
    declared = canonical_arrow_schema(metadata)
    drifted = declared.set(
        declared.get_field_index("temperature"),
        pa.field("temperature", pa.string()),
    )

    with pytest.raises(ValueError, match="'temperature' type mismatch"):
        await _ingest(
            ingestion,
            metadata,
            canonical_batch([DECLARED_START], ["20.0"], schema=drifted),
        )


async def test_record_bounds_widen_only_past_the_declared_end(
    ingestion, metadata, canonical_batch
):
    """Data past the declared end widens the record's end and warns; the declared
    start is kept, and data falling inside the declared window changes nothing."""
    times = _minutes(10)

    with structlog.testing.capture_logs() as inside_logs:
        inside = await _ingest(ingestion, metadata, canonical_batch(times))
    with structlog.testing.capture_logs() as overrun_logs:
        overrun = await _ingest(
            ingestion,
            metadata,
            canonical_batch(times),
            end_time=DECLARED_START + timedelta(minutes=5),
        )

    assert (inside[-1].start_time, inside[-1].end_time) == (
        DECLARED_START,
        DECLARED_END,
    )
    assert not any(log["log_level"] == "warning" for log in inside_logs)

    assert (overrun[-1].start_time, overrun[-1].end_time) == (
        DECLARED_START,
        DECLARED_START + timedelta(minutes=9),
    )
    assert any(log["log_level"] == "warning" for log in overrun_logs)


async def test_a_stream_with_no_data_still_claims_its_declared_coverage(
    ingestion, metadata, canonical_batch, timeseries_db
):
    """A window the source covered but that held no data still yields coverage."""
    ingestion_id = uuid4()

    events = await _ingest(
        ingestion,
        metadata,
        canonical_batch([DECLARED_START], [None]),
        ingestion_id=ingestion_id,
    )

    assert events[-1].id == uuid5(ingestion_id, "claim-0")
    assert (events[-1].start_time, events[-1].end_time) == (
        DECLARED_START,
        DECLARED_END,
    )
    assert events[-1].records == []
    assert timeseries_db.stored("test_dataset") is None
