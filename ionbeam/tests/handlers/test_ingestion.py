# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timedelta, timezone
from uuid import uuid4, uuid5

import pandas as pd
import pyarrow as pa
import pytest
import structlog

from ionbeam_client.models import (
    CfSemantics,
    DataAvailableEvent,
    DatasetSchema,
    IngestionMetadata,
    Tag,
    TimeCoordinate,
    Variable,
    geographic_point_coordinates,
)
from ionbeam_client.canonical_stream import canonical_arrow_schema
from ionbeam_client.schema_metadata import SCHEMA_HASH
from ionbeam.datasets import DatasetBuildConfig, DatasetRegistry
from ionbeam.handlers.ingestion import Ingestion
from ionbeam.observability import IngestionMetrics
from ionbeam.storage.lateness_histogram import bucket_for


@pytest.fixture
def make_ingestion(
    timeseries_db,
    ingestion_metrics: IngestionMetrics,
    coordination_store,
):
    """Build a handler whose registry aggregates ``test_dataset`` at ``span``.
    The aggregation span is server-side production config, fixed here rather
    than in metadata."""

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


@pytest.fixture
def sample_metadata():
    return _metadata()


def _with_schema_hash(batch: pa.RecordBatch, metadata: IngestionMetadata) -> pa.RecordBatch:
    schema = batch.schema.with_metadata(
        {**(batch.schema.metadata or {}), SCHEMA_HASH.encode(): metadata.schema_hash().encode()}
    )
    return pa.RecordBatch.from_arrays(
        [batch.column(i) for i in range(batch.num_columns)], schema=schema
    )


def _canonical_batch(metadata: IngestionMetadata, columns: dict) -> pa.RecordBatch:
    """A batch in the shape the contract demands: canonical schema, hash stamped."""
    return pa.RecordBatch.from_pydict(columns, schema=canonical_arrow_schema(metadata))


def _stream_batch(timestamps, metadata: IngestionMetadata) -> pa.RecordBatch:
    n = len(timestamps)
    return _canonical_batch(
        metadata,
        {
            "time": timestamps,
            "lat": [52.5] * n,
            "lon": [13.4] * n,
            "temperature": [20.0] * n,
            "station_id": ["test_station"] * n,
        },
    )


async def _stream(*batches):
    for batch in batches:
        yield batch


async def _discard(event: DataAvailableEvent) -> None:
    pass


DECLARED_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
DECLARED_END = datetime(2024, 1, 1, 6, tzinfo=timezone.utc)


def _metadata() -> IngestionMetadata:
    return IngestionMetadata(
        name="test_dataset",
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


def _at(ticks: float) -> datetime:
    """Six minutes per tick: ten ticks span one PT1H aggregation window."""
    return DECLARED_START + timedelta(minutes=6 * ticks)


class TestCoverageCheckpoints:
    """A long-running stream must publish coverage checkpoints while it is still open,
    so downstream windows can build without waiting for the stream to end."""

    SPAN = timedelta(hours=1)

    async def _run(
        self,
        ingestion,
        metadata,
        batches,
        events,
        ingestion_id=None,
        end_time=DECLARED_END,
    ):
        async def record(event: DataAvailableEvent) -> None:
            events.append(event)

        return await ingestion.ingest(
            ingestion_id=ingestion_id or uuid4(),
            metadata=metadata,
            start_time=DECLARED_START,
            end_time=end_time,
            batches=batches,
            on_data_available=record,
        )

    async def test_stream_within_one_window_publishes_only_the_final_event(
        self, make_ingestion, sample_metadata
    ):
        """Default day-long aggregation: an hour of data crosses no boundary."""
        ingestion_id = uuid4()
        events: list[DataAvailableEvent] = []
        timestamps = list(
            pd.date_range(DECLARED_START, periods=10, freq="1min", tz="UTC")
        )

        result = await self._run(
            make_ingestion(),
            sample_metadata,
            _stream(_stream_batch(timestamps, sample_metadata)),
            events,
            ingestion_id=ingestion_id,
        )

        assert events == [result]
        # the final claim is the stream's only checkpoint, deterministically
        # derived so a replay re-checkpoints under the same id
        assert result.id == uuid5(ingestion_id, "claim-0")
        assert result.start_time == DECLARED_START
        assert result.end_time == DECLARED_END

    async def test_checkpoint_publishes_when_watermark_crosses_window_boundary(
        self, make_ingestion
    ):
        ingestion = make_ingestion(self.SPAN)
        metadata = _metadata()
        ingestion_id = uuid4()
        events: list[DataAvailableEvent] = []
        events_seen_after_batch: list[int] = []

        async def batches():
            yield _stream_batch([_at(0), _at(9)], metadata)
            events_seen_after_batch.append(len(events))
            yield _stream_batch([_at(10), _at(12)], metadata)
            events_seen_after_batch.append(len(events))
            yield _stream_batch([_at(13), _at(14)], metadata)
            events_seen_after_batch.append(len(events))

        with structlog.testing.capture_logs() as logs:
            result = await self._run(
                ingestion, metadata, batches(), events, ingestion_id=ingestion_id
            )

        # nothing inside the first window; one checkpoint on crossing; none without
        assert events_seen_after_batch == [0, 1, 1]

        checkpoint = events[0]
        assert checkpoint.id == uuid5(ingestion_id, "claim-0")
        assert checkpoint.start_time == DECLARED_START
        assert checkpoint.end_time == _at(12)

        assert result.id == uuid5(ingestion_id, "claim-1")
        assert result.start_time == _at(12)
        assert result.end_time == DECLARED_END
        assert events == [checkpoint, result]

        # chained checkpoints are expected behavior
        assert not any(log["log_level"] == "warning" for log in logs)

    async def test_checkpoints_chain_contiguously_and_batch_spanning_windows_checkpoint_once(
        self, make_ingestion
    ):
        ingestion = make_ingestion(self.SPAN)
        metadata = _metadata()
        events: list[DataAvailableEvent] = []

        result = await self._run(
            ingestion,
            metadata,
            _stream(
                _stream_batch([_at(0), _at(11)], metadata),
                # one batch jumping three windows still yields a single checkpoint
                _stream_batch([_at(15), _at(43)], metadata),
            ),
            events,
        )

        assert len(events) == 3  # two checkpoints + final
        first, second = events[0], events[1]
        assert first.start_time == DECLARED_START
        assert first.end_time == _at(11)
        assert second.start_time == _at(11)
        assert second.end_time == _at(43)
        assert result.start_time == _at(43)
        assert result.end_time == DECLARED_END

    async def test_late_data_widens_the_next_checkpoint_to_cover_its_window(
        self, make_ingestion
    ):
        """Data older than what is already checkpointed must be re-checkpointed under a fresh
        record id, so windows already built from earlier checkpoints get rebuilt."""
        ingestion = make_ingestion(self.SPAN)
        metadata = _metadata()
        events: list[DataAvailableEvent] = []

        await self._run(
            ingestion,
            metadata,
            _stream(
                _stream_batch([_at(0), _at(12)], metadata),
                _stream_batch([_at(5), _at(21)], metadata),  # _at(5) is late
            ),
            events,
        )

        late_checkpoint = events[1]
        assert late_checkpoint.start_time == _at(5)
        assert late_checkpoint.end_time == _at(21)
        assert late_checkpoint.id != events[0].id

    async def test_replaying_the_same_command_yields_the_same_record_ids(
        self, make_ingestion
    ):
        """A retried command produces identical checkpoint ids for identical
        stream and ingestion id, so no spurious window rebuilds."""
        metadata = _metadata()
        ingestion_id = uuid4()

        async def run() -> list[DataAvailableEvent]:
            handler = make_ingestion(self.SPAN)
            events: list[DataAvailableEvent] = []
            await self._run(
                handler,
                metadata,
                _stream(
                    _stream_batch([_at(0), _at(11)], metadata),
                    _stream_batch([_at(12), _at(25)], metadata),
                ),
                events,
                ingestion_id=ingestion_id,
            )
            return events

        assert [e.id for e in await run()] == [e.id for e in await run()]


class TestRowProvenance:
    """Every stored row carries the id of the ingestion record that delivered it,
    so a build can select exactly its desired record set at query time."""

    SPAN = timedelta(hours=1)

    def _batch(self, metadata, minutes, temperatures, station="st1"):
        return _canonical_batch(
            metadata,
            {
                "time": [_at(m) for m in minutes],
                "lat": [52.5] * len(minutes),
                "lon": [13.4] * len(minutes),
                "temperature": temperatures,
                "station_id": [station] * len(minutes),
            },
        )

    async def _ingest(self, handler, metadata, batches, ingestion_id=None):
        events = []

        async def record(event: DataAvailableEvent) -> None:
            events.append(event)

        await handler.ingest(
            ingestion_id=ingestion_id or uuid4(),
            metadata=metadata,
            start_time=DECLARED_START,
            end_time=DECLARED_END,
            batches=_stream(*batches),
            on_data_available=record,
        )
        return events

    async def test_rows_are_tagged_with_the_record_that_delivers_them(
        self, make_ingestion, timeseries_db
    ):
        """Rows written before a checkpoint fires belong to that checkpoint; rows after
        it belong to the next checkpoint (the final record)."""
        handler = make_ingestion(self.SPAN)
        metadata = _metadata()

        events = await self._ingest(
            handler,
            metadata,
            [
                self._batch(metadata, [0, 12], [20.0, 21.0]),
                self._batch(metadata, [13], [22.0]),
            ],
        )

        assert len(events) == 2  # one checkpoint + final
        # rows are tagged with their window's record id, and each claim
        # publishes exactly the records whose rows it wrote
        tagged = [
            set(call["table"].column("ib_record_id").to_pylist())
            for call in timeseries_db.write_calls
        ]
        assert tagged == [
            {str(record.id) for record in events[0].records},
            {str(record.id) for record in events[1].records},
        ]
        # the first batch crossed a window boundary: two windows, two records
        assert len(events[0].records) == 2
        assert len(events[1].records) == 1
        # record ids never repeat across claims
        assert {r.id for r in events[0].records}.isdisjoint(
            r.id for r in events[1].records
        )

    async def test_dedup_ingestion_write_only_novel_content(
        self, make_ingestion, timeseries_db
    ):
        """A sweep source re-fetching history re-delivers identical rows under
        fresh record ids; a delta dataset stores only changed content — and the
        change itself (a QC correction) is kept."""
        metadata = _metadata()

        def sweep(temperatures):
            return [self._batch(metadata, [0, 2, 4], temperatures)]

        handler = make_ingestion(self.SPAN, dedup_ingestion=True)
        await self._ingest(handler, metadata, sweep([20.0, 21.0, 22.0]))
        await self._ingest(handler, metadata, sweep([20.0, 21.0, 22.0]))
        await self._ingest(handler, metadata, sweep([20.0, 18.5, 22.0]))

        written = [
            call["table"].column("temperature").to_pylist()
            for call in timeseries_db.write_calls
        ]
        # first sweep stores all rows; the identical sweep stores nothing
        # (no write at all); the correcting sweep stores just the changed row
        assert written == [[20.0, 21.0, 22.0], [18.5]]

    async def test_delta_marks_nothing_stored_when_the_write_fails(
        self, make_ingestion, timeseries_db
    ):
        """Stored-content sets are marked only after the write succeeds: a
        failed write followed by a retry must persist every row — the failure
        direction is a duplicate, never a lost row."""
        metadata = _metadata()
        ingestion_id = uuid4()

        def batches():
            return [self._batch(metadata, [0, 2, 4], [20.0, 21.0, 22.0])]

        handler = make_ingestion(self.SPAN, dedup_ingestion=True)
        healthy_write = timeseries_db.write
        outage = {"remaining": 1}

        async def flaky_write(**kwargs):
            if outage["remaining"]:
                outage["remaining"] -= 1
                raise ConnectionError("influx unavailable")
            return await healthy_write(**kwargs)

        timeseries_db.write = flaky_write

        with pytest.raises(ConnectionError):
            await self._ingest(handler, metadata, batches(), ingestion_id=ingestion_id)

        await self._ingest(handler, metadata, batches(), ingestion_id=ingestion_id)
        assert len(timeseries_db.write_calls) == 1
        assert timeseries_db.write_calls[0]["table"].num_rows == 3


class TestIngestion:
    async def test_ingestion_canonicalizes_and_writes_to_timeseries(
        self, ingestion, sample_metadata, timeseries_db
    ):
        """Streamed batches are canonicalized (renames, tz, sort, tags) and written."""
        timestamps = list(
            pd.date_range("2024-01-01", periods=10, freq="1min", tz="UTC")
        )
        batch = _stream_batch(list(reversed(timestamps)), sample_metadata)  # unsorted on purpose

        result = await ingestion.ingest(
            ingestion_id=uuid4(),
            metadata=sample_metadata,
            start_time=DECLARED_START,
            end_time=DECLARED_END,
            batches=_stream(batch),
            on_data_available=_discard,
        )

        assert isinstance(result, DataAvailableEvent)
        assert result.metadata == sample_metadata
        assert result.start_time == DECLARED_START
        assert result.end_time == DECLARED_END

        assert len(timeseries_db.write_calls) == 1
        write_call = timeseries_db.write_calls[0]

        assert write_call["measurement"] == "test_dataset"
        assert write_call["timestamp_column"] == "time"
        assert write_call["tag_columns"] == ["station_id", "ib_record_id"]

        written_df = write_call["table"].to_pandas()

        expected_columns = {"time", "lat", "lon", "temperature", "station_id", "ib_record_id"}
        assert set(written_df.columns) == expected_columns

        assert len(written_df) == 10
        assert written_df["lat"].iloc[0] == 52.5
        assert written_df["lon"].iloc[0] == 13.4
        assert written_df["temperature"].iloc[0] == 20.0
        assert written_df["station_id"].iloc[0] == "test_station"

        assert pd.api.types.is_datetime64_any_dtype(written_df["time"])
        assert str(written_df["time"].dt.tz) == "UTC"
        assert written_df["time"].is_monotonic_increasing

    async def test_ingestion_records_per_datum_arrival_lateness(
        self, ingestion, sample_metadata, coordination_store
    ):
        """Every datum's lateness (now minus its own observation time) lands in
        the histogram: a batch carrying late rows records the late tail. Distinct
        stations keep each row a distinct content set."""
        now = pd.Timestamp.now(tz="UTC")
        timestamps = [now - pd.Timedelta(hours=1)] * 3 + [now - pd.Timedelta(hours=6)] * 2
        batch = _canonical_batch(
            sample_metadata,
            {
                "time": timestamps,
                "lat": [52.5] * 5,
                "lon": [13.4] * 5,
                "temperature": [20.0] * 5,
                "station_id": ["a", "b", "c", "d", "e"],
            },
        )

        await ingestion.ingest(
            ingestion_id=uuid4(),
            metadata=sample_metadata,
            start_time=DECLARED_START,
            end_time=DECLARED_END,
            batches=_stream(batch),
            on_data_available=_discard,
        )

        histogram = coordination_store._lateness["test_dataset"]
        assert sum(histogram.values()) == 5
        assert histogram == {
            bucket_for(3600): 3,
            bucket_for(6 * 3600): 2,
        }

    async def _ingest(self, handler, metadata, batch):
        await handler.ingest(
            ingestion_id=uuid4(),
            metadata=metadata,
            start_time=DECLARED_START,
            end_time=DECLARED_END,
            batches=_stream(batch),
            on_data_available=_discard,
        )

    async def test_identical_redelivery_records_lateness_only_once(
        self, make_ingestion, sample_metadata, coordination_store
    ):
        """A ``dedup_ingestion`` source re-fetching an overlapping span re-delivers
        the same rows; the write suppresses them, so they do not re-count as late
        arrivals."""
        handler = make_ingestion(dedup_ingestion=True)
        now = pd.Timestamp.now(tz="UTC")
        timestamps = [now - pd.Timedelta(minutes=m) for m in range(60, 65)]
        batch = _stream_batch(timestamps, sample_metadata)

        await self._ingest(handler, sample_metadata, batch)
        await self._ingest(handler, sample_metadata, batch)

        histogram = coordination_store._lateness["test_dataset"]
        assert sum(histogram.values()) == 5

    async def test_qc_update_changing_a_value_is_still_recorded(
        self, make_ingestion, sample_metadata, coordination_store
    ):
        """netatmo's QC pass rewrites a value ~2h later: same station and time,
        different content — that genuinely late change must land in the histogram."""
        handler = make_ingestion(dedup_ingestion=True)
        timestamps = [pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=1)] * 3

        def batch(temperatures):
            return _canonical_batch(
                sample_metadata,
                {
                    "time": timestamps,
                    "lat": [52.5] * 3,
                    "lon": [13.4] * 3,
                    "temperature": temperatures,
                    "station_id": ["a", "b", "c"],
                },
            )

        await self._ingest(handler, sample_metadata, batch([20.0, 21.0, 22.0]))
        await self._ingest(handler, sample_metadata, batch([20.0, 18.5, 22.0]))

        histogram = coordination_store._lateness["test_dataset"]
        assert sum(histogram.values()) == 4

    async def test_redelivery_spanning_windows_dedups_each_window(
        self, make_ingestion, sample_metadata, coordination_store
    ):
        """One batch can straddle aggregation windows (a multi-hour re-fetch);
        every row must dedup against its own window's stored-content set."""
        handler = make_ingestion(span=timedelta(hours=1), dedup_ingestion=True)
        now = pd.Timestamp.now(tz="UTC")
        timestamps = [now - pd.Timedelta(minutes=m) for m in (30, 90, 150)]
        batch = _stream_batch(timestamps, sample_metadata)

        await self._ingest(handler, sample_metadata, batch)
        await self._ingest(handler, sample_metadata, batch)

        histogram = coordination_store._lateness["test_dataset"]
        assert sum(histogram.values()) == 3

    async def test_redelivery_without_dedup_ingestion_counts_again(
        self, make_ingestion, sample_metadata, coordination_store
    ):
        """The histogram counts what the store writes: a push feed without
        ``dedup_ingestion`` records its rare redeliveries again — noise a p95
        tolerates, in exchange for no second dedup machinery."""
        handler = make_ingestion()
        now = pd.Timestamp.now(tz="UTC")
        timestamps = [now - pd.Timedelta(minutes=m) for m in range(60, 65)]
        batch = _stream_batch(timestamps, sample_metadata)

        await self._ingest(handler, sample_metadata, batch)
        await self._ingest(handler, sample_metadata, batch)

        histogram = coordination_store._lateness["test_dataset"]
        assert sum(histogram.values()) == 10

    async def test_rows_for_a_sealed_window_are_not_recorded(
        self, make_ingestion, sample_metadata, coordination_store
    ):
        """A row whose window is already final cannot affect any build; letting it
        into the histogram would push the settle gate past the point where waiting
        can help."""
        handler = make_ingestion(
            span=timedelta(hours=1), retention=timedelta(hours=48)
        )
        now = pd.Timestamp.now(tz="UTC")
        timestamps = [now - pd.Timedelta(hours=h) for h in (72, 96)]
        batch = _stream_batch(timestamps, sample_metadata)

        await self._ingest(handler, sample_metadata, batch)

        histogram = coordination_store._lateness.get("test_dataset", {})
        assert sum(histogram.values()) == 0

    async def test_stream_with_undeclared_column_types_is_rejected(
        self, ingestion, sample_metadata
    ):
        """A correctly-named, correctly-stamped stream still fails the contract
        if a column's Arrow type differs from the declared dtype."""
        drifted_schema = pa.schema(
            [
                pa.field("time", pa.timestamp("ns", tz="UTC")),
                pa.field("lat", pa.float64()),
                pa.field("lon", pa.float64()),
                pa.field("temperature", pa.string()),
                pa.field("station_id", pa.string()),
            ]
        )
        batch = pa.RecordBatch.from_pydict(
            {
                "time": [DECLARED_START],
                "lat": [52.5],
                "lon": [13.4],
                "temperature": ["20.0"],
                "station_id": ["test_station"],
            },
            schema=drifted_schema,
        )

        with pytest.raises(ValueError, match="'temperature' type mismatch"):
            await ingestion.ingest(
                ingestion_id=uuid4(),
                metadata=sample_metadata,
                start_time=DECLARED_START,
                end_time=DECLARED_END,
                batches=_stream(_with_schema_hash(batch, sample_metadata)),
                on_data_available=_discard,
            )

    async def test_ingestion_widens_end_to_actual_data_end(
        self, ingestion, sample_metadata
    ):
        """Data past the declared end widens the record's end; the declared start is kept."""
        declared_end = DECLARED_START + timedelta(minutes=5)
        actual_end = DECLARED_START + timedelta(minutes=9)
        timestamps = list(
            pd.date_range(DECLARED_START, periods=10, freq="1min", tz="UTC")
        )

        with structlog.testing.capture_logs() as logs:
            result = await ingestion.ingest(
                ingestion_id=uuid4(),
                metadata=sample_metadata,
                start_time=DECLARED_START,
                end_time=declared_end,
                batches=_stream(_stream_batch(timestamps, sample_metadata)),
                on_data_available=_discard,
            )

        assert result.start_time == DECLARED_START
        assert result.end_time == actual_end
        assert any(log["log_level"] == "warning" for log in logs)

    async def test_ingestion_keeps_declared_bounds_when_data_inside_window(
        self, ingestion, sample_metadata
    ):
        timestamps = list(
            pd.date_range(DECLARED_START, periods=10, freq="1min", tz="UTC")
        )

        with structlog.testing.capture_logs() as logs:
            result = await ingestion.ingest(
                ingestion_id=uuid4(),
                metadata=sample_metadata,
                start_time=DECLARED_START,
                end_time=DECLARED_END,
                batches=_stream(_stream_batch(timestamps, sample_metadata)),
                on_data_available=_discard,
            )

        assert result.start_time == DECLARED_START
        assert result.end_time == DECLARED_END
        assert not any(log["log_level"] == "warning" for log in logs)

    async def test_zero_canonical_rows_still_returns_event_with_declared_bounds(
        self, ingestion, sample_metadata, timeseries_db
    ):
        """A window the source covered but that held no data still yields coverage."""
        batch = _canonical_batch(
            sample_metadata,
            {
                "time": [DECLARED_START],
                "lat": [52.5],
                "lon": [13.4],
                "temperature": [None],
                "station_id": ["test_station"],
            },
        )

        ingestion_id = uuid4()
        result = await ingestion.ingest(
            ingestion_id=ingestion_id,
            metadata=sample_metadata,
            start_time=DECLARED_START,
            end_time=DECLARED_END,
            batches=_stream(batch),
            on_data_available=_discard,
        )

        assert result.id == uuid5(ingestion_id, "claim-0")
        assert result.start_time == DECLARED_START
        assert result.end_time == DECLARED_END
        assert result.records == []  # coverage without rows claims no records
        assert timeseries_db.write_calls == []
