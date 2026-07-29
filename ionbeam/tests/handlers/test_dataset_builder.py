# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Builder behaviours the e2e flow cannot reach: failure requeue, the
concurrent worker loop, and record-scoped composition. The happy path is
covered end-to-end in tests/flight/test_flight_e2e.py."""

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from ionbeam.builds import manifest_key
from ionbeam.datasets import DatasetProductionConfig, DatasetRegistry
from ionbeam.handlers.dataset_builder import (
    DatasetBuilderConfig,
    DatasetBuilder,
)
from ionbeam.provenance import (
    IngestionRecord,
    ManifestBuild,
    RecordSet,
    Window,
    WindowManifest,
)
from ionbeam.storage.memory_timeseries import InMemoryTimeSeriesDatabase
from ionbeam_client.schema_metadata import BUILD
from ionbeam_client.models import (
    CfSemantics,
    DatasetSchema,
    IngestionMetadata,
    Tag,
    TimeCoordinate,
    Variable,
    geographic_point_coordinates,
)

REGISTRY = DatasetRegistry(
    {"test_dataset": DatasetProductionConfig(aggregation_span=timedelta(hours=1))}
)

METADATA = IngestionMetadata(
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


async def _seed_window(store, start: datetime) -> Window:
    """One fully covered window with its desired record set in place."""
    record = IngestionRecord(
        id=uuid4(),
        metadata=METADATA,
        start_time=start,
        end_time=start + timedelta(hours=1),
        arrived_at=start + timedelta(hours=1),
    )
    await store.save_ingestion_record(record)
    window = Window("test_dataset", start, timedelta(hours=1))
    await store.add_desired_record_ids(window, [str(record.id)])
    return window


def _builder(store, queue, timeseries_db, metrics, arrow_store, published, **config):
    async def publisher(event) -> None:
        published.append(event)

    return DatasetBuilder(
        DatasetBuilderConfig(poll_interval_seconds=0.1, **config),
        store,
        queue,
        timeseries_db,
        metrics,
        arrow_store,
        event_publisher=publisher,
        registry=REGISTRY,
    )


async def test_failed_build_reschedules_the_window(
    coordination_store,
    build_queue,
    failing_timeseries_db,
    arrow_store,
    builder_metrics,
):
    window = await _seed_window(
        coordination_store, datetime(2024, 1, 1, 10, tzinfo=timezone.utc)
    )
    published = []
    builder = _builder(
        coordination_store,
        build_queue,
        failing_timeseries_db,
        builder_metrics,
        arrow_store,
        published,
    )

    await builder.build_window(window)

    assert list(build_queue.get_queue_dict()) == [window.dataset_key]
    assert arrow_store.stored_keys() == []
    assert published == []


async def test_worker_loop_drains_the_queue_with_concurrent_builds(
    coordination_store,
    build_queue,
    timeseries_db,
    arrow_store,
    builder_metrics,
):
    windows = []
    for hour in range(10, 15):
        window = await _seed_window(
            coordination_store,
            datetime(2024, 1, 1, hour, tzinfo=timezone.utc),
        )
        await build_queue.schedule(window, window.end)
        windows.append(window)

    published = []
    builder = _builder(
        coordination_store,
        build_queue,
        timeseries_db,
        builder_metrics,
        arrow_store,
        published,
        concurrency=2,
    )

    async with builder:
        deadline = asyncio.get_event_loop().time() + 10.0
        while len(arrow_store.stored_keys()) < len(windows):
            assert asyncio.get_event_loop().time() < deadline, "builds did not finish"
            await asyncio.sleep(0.05)

    assert len(arrow_store.stored_keys()) == len(windows)
    assert build_queue.get_queue_dict() == {}
    assert {(event.start_time, event.end_time) for event in published} == {
        (window.start, window.end) for window in windows
    }


WINDOW_START = datetime(2024, 1, 1, 10, tzinfo=timezone.utc)
WINDOW = Window("test_dataset", WINDOW_START, timedelta(hours=1))


def _arrived(minutes: int) -> datetime:
    return WINDOW_START + timedelta(hours=1, minutes=minutes)


async def _seed_rows(
    db, record_id: str, minutes: list[int], temperatures: list[float]
) -> None:
    n = len(minutes)
    table = pa.table(
        {
            "time": pa.array(
                [WINDOW_START + timedelta(minutes=m) for m in minutes],
                type=pa.timestamp("ns", tz="UTC"),
            ),
            "lat": [52.5] * n,
            "lon": [13.4] * n,
            "temperature": temperatures,
            "station_id": ["st1"] * n,
            "ib_record_id": [record_id] * n,
        }
    )
    await db.write(table, "test_dataset", ["station_id", "ib_record_id"], "time")


async def _seed_record(store, arrived_at: datetime, id=None) -> str:
    record = IngestionRecord(
        id=id or uuid4(),
        metadata=METADATA,
        start_time=WINDOW.start,
        end_time=WINDOW.end,
        arrived_at=arrived_at,
    )
    await store.save_ingestion_record(record)
    await store.add_desired_record_ids(WINDOW, [str(record.id)])
    return str(record.id)


class TestRecordScopedBuilds:
    """A build composes exactly its desired records' rows — never whatever the
    time range happens to hold at query time — so the stamped record-set hash
    names the artifact's true contents."""

    async def _build(
        self, store, queue, db, metrics, arrow_store
    ) -> pa.Table:
        published = []
        builder = _builder(store, queue, db, metrics, arrow_store, published)
        await builder.build_window(WINDOW)
        assert len(published) == 1
        return pa.concat_tables(
            pq.read_table(arrow_store._get_path(location))
            for location in published[0].dataset_locations
        )

    async def test_build_composes_exactly_the_desired_records(
        self,
        coordination_store,
        build_queue,
        arrow_store,
        builder_metrics,
    ):
        """Rows delivered by a record outside the desired set — arrived after
        the build was decided, or never recorded — stay out of the artifact,
        even though they sit in the same time range."""
        db = InMemoryTimeSeriesDatabase()
        desired = await _seed_record(
            coordination_store, arrived_at=_arrived(1)
        )
        await _seed_rows(db, desired, [0, 6], [20.0, 21.0])
        await _seed_rows(db, str(uuid4()), [12], [99.0])  # not desired

        table = await self._build(
            coordination_store,
            build_queue,
            db,
            builder_metrics,
            arrow_store,
        )
        assert sorted(table.column("temperature").to_pylist()) == [20.0, 21.0]

    async def test_correction_from_the_later_arrived_record_wins(
        self,
        coordination_store,
        build_queue,
        arrow_store,
        builder_metrics,
    ):
        """Same station and observation time in two desired records: the
        later-arrived record's row replaces the earlier one — a QC correction
        propagates. The correction gets the lexicographically smaller id, so
        passing proves the collapse orders by arrival, not by record id."""
        db = InMemoryTimeSeriesDatabase()
        original = await _seed_record(
            coordination_store,
            arrived_at=_arrived(1),
            id=UUID("ffffffff-0000-0000-0000-000000000001"),
        )
        correction = await _seed_record(
            coordination_store,
            arrived_at=_arrived(30),
            id=UUID("00000000-0000-0000-0000-000000000002"),
        )
        await _seed_rows(db, original, [0], [20.0])
        await _seed_rows(db, correction, [0], [18.5])

        table = await self._build(
            coordination_store,
            build_queue,
            db,
            builder_metrics,
            arrow_store,
        )
        assert table.column("temperature").to_pylist() == [18.5]

    async def test_unreachable_record_rows_defer_the_build(
        self,
        coordination_store,
        build_queue,
        arrow_store,
        builder_metrics,
    ):
        """A desired record whose rows the record filter cannot reach means the
        hot store lost or expired them. Publishing a partial record set would
        be silent data loss; publishing the raw time range would be unfolded
        duplicates. The build defers instead — nothing is published."""
        db = InMemoryTimeSeriesDatabase()
        reachable = await _seed_record(
            coordination_store, arrived_at=_arrived(1)
        )
        await _seed_record(coordination_store, arrived_at=_arrived(2))
        await _seed_rows(db, reachable, [0], [20.0])

        published = []
        builder = _builder(
            coordination_store,
            build_queue,
            db,
            builder_metrics,
            arrow_store,
            published,
            retry_backoff_base_seconds=0.01,
        )
        await builder.build_window(WINDOW)

        assert published == []
        assert arrow_store.stored_keys() == []
        assert list(build_queue.get_queue_dict()) == [WINDOW.dataset_key]


class TestWindowManifests:
    """Every artifact write appends a build entry to a manifest stored beside
    the file — the durable record of which ingestion records, under which
    schema, produced it — so provenance survives the coordination cache."""

    async def _build(
        self, store, queue, db, metrics, arrow_store
    ) -> WindowManifest:
        published = []
        builder = _builder(store, queue, db, metrics, arrow_store, published)
        await builder.build_window(WINDOW)
        document = await arrow_store.read_json(manifest_key(WINDOW))
        assert document is not None
        return WindowManifest.model_validate_json(document)

    async def test_manifest_names_the_records_and_schema_of_the_build(
        self,
        coordination_store,
        build_queue,
        arrow_store,
        builder_metrics,
    ):
        db = InMemoryTimeSeriesDatabase()
        record_id = await _seed_record(
            coordination_store, arrived_at=_arrived(1)
        )
        await _seed_rows(db, record_id, [0, 6], [20.0, 21.0])

        manifest = await self._build(
            coordination_store,
            build_queue,
            db,
            builder_metrics,
            arrow_store,
        )

        assert manifest.dataset == "test_dataset"
        assert manifest.window_start == WINDOW.start
        assert manifest.window_end == WINDOW.end
        assert manifest.ingestion_metadata == METADATA
        (build,) = manifest.builds
        assert build.version == 1
        assert build.total_rows == 2
        assert build.schema_hash == METADATA.schema_hash()
        assert build.record_ids_hash == RecordSet.from_list([record_id]).hash
        (record,) = build.records
        assert record.id == record_id
        assert record.start_time == WINDOW.start
        assert record.arrived_at == _arrived(1)

    async def test_rebuilds_append_to_the_build_history(
        self,
        coordination_store,
        build_queue,
        arrow_store,
        builder_metrics,
    ):
        db = InMemoryTimeSeriesDatabase()
        first = await _seed_record(coordination_store, arrived_at=_arrived(1))
        await _seed_rows(db, first, [0], [20.0])
        await self._build(
            coordination_store,
            build_queue,
            db,
            builder_metrics,
            arrow_store,
        )

        second = await _seed_record(
            coordination_store, arrived_at=_arrived(30)
        )
        await _seed_rows(db, second, [6], [21.0])
        manifest = await self._build(
            coordination_store,
            build_queue,
            db,
            builder_metrics,
            arrow_store,
        )

        assert [build.version for build in manifest.builds] == [1, 2]
        assert {record.id for record in manifest.builds[0].records} == {first}
        assert {record.id for record in manifest.builds[1].records} == {first, second}
        assert (
            manifest.builds[0].record_ids_hash != manifest.builds[1].record_ids_hash
        )

        # each build is its own immutable object: the rebuild never rewrote v1,
        # so a reader holding the v1 location is undisturbed until v1 expires
        (v1,) = manifest.builds[0].locations
        (v2,) = manifest.builds[1].locations
        assert arrow_store.stored_keys() == sorted([v1, v2])
        assert arrow_store.get_total_rows(v1) == 1
        assert arrow_store.get_total_rows(v2) == 2


async def test_repeated_failures_park_the_window_until_a_build_succeeds(
    coordination_store,
    build_queue,
    arrow_store,
    builder_metrics,
):
    """Past the retry budget a window parks on the dead-letter set — visible,
    out of the retry loop — and a later successful build clears it."""
    db = InMemoryTimeSeriesDatabase()
    record_id = await _seed_record(coordination_store, arrived_at=_arrived(1))

    published = []
    builder = _builder(
        coordination_store,
        build_queue,
        db,
        builder_metrics,
        arrow_store,
        published,
        max_build_attempts=2,
        retry_backoff_base_seconds=0.01,
    )

    await builder.build_window(WINDOW)  # no rows for the record: defers
    await builder.build_window(WINDOW)  # budget exhausted: parks
    assert [w.dataset_key for w in await build_queue.dead_lettered()] == [
        WINDOW.dataset_key
    ]
    assert published == []

    await _seed_rows(db, record_id, [0], [20.0])
    await builder.build_window(WINDOW)
    assert [event.start_time for event in published] == [WINDOW.start]
    assert await build_queue.dead_lettered() == []


async def test_a_deferring_window_frees_its_slot_for_other_builds(
    coordination_store,
    build_queue,
    arrow_store,
    builder_metrics,
):
    """A window that cannot build defers into the queue — its backoff must not
    hold the worker slot, so the next due window builds immediately even with
    one worker."""
    db = InMemoryTimeSeriesDatabase()
    await _seed_record(coordination_store, arrived_at=_arrived(1))  # no rows

    healthy = Window("test_dataset", WINDOW_START + timedelta(hours=1), timedelta(hours=1))
    healthy_record = IngestionRecord(
        id=uuid4(),
        metadata=METADATA,
        start_time=healthy.start,
        end_time=healthy.end,
        arrived_at=healthy.end,
    )
    await coordination_store.save_ingestion_record(healthy_record)
    await coordination_store.add_desired_record_ids(
        healthy, [str(healthy_record.id)]
    )
    await _seed_rows(db, str(healthy_record.id), [65], [21.0])

    await build_queue.schedule(WINDOW, WINDOW.end)  # claimed first, defers
    await build_queue.schedule(healthy, healthy.end)

    published = []
    builder = _builder(
        coordination_store,
        build_queue,
        db,
        builder_metrics,
        arrow_store,
        published,
        concurrency=1,
        retry_backoff_base_seconds=300.0,
    )
    async with builder:
        deadline = asyncio.get_event_loop().time() + 10.0
        while not published:
            assert asyncio.get_event_loop().time() < deadline, "healthy window never built"
            await asyncio.sleep(0.05)

    assert [event.start_time for event in published] == [healthy.start]
    assert build_queue.get_queue_dict()[WINDOW.dataset_key] > datetime.now(timezone.utc)


async def test_worker_loop_survives_a_queue_outage(
    coordination_store,
    build_queue,
    timeseries_db,
    arrow_store,
    builder_metrics,
):
    """A transient coordination outage (Valkey down, DNS blip at startup) must
    idle the worker loop, not kill it: once the queue answers again, queued
    windows still build."""
    window = await _seed_window(
        coordination_store, datetime(2024, 1, 1, 10, tzinfo=timezone.utc)
    )
    await build_queue.schedule(window, window.end)

    outage = 3
    healthy_claim = build_queue.claim_due

    async def flaky_claim():
        nonlocal outage
        if outage:
            outage -= 1
            raise ConnectionError("valkey unreachable")
        return await healthy_claim()

    build_queue.claim_due = flaky_claim

    published = []
    builder = _builder(
        coordination_store,
        build_queue,
        timeseries_db,
        builder_metrics,
        arrow_store,
        published,
        retry_backoff_base_seconds=0.01,
    )

    async with builder:
        deadline = asyncio.get_event_loop().time() + 10.0
        while not arrow_store.stored_keys():
            assert asyncio.get_event_loop().time() < deadline, "build never ran"
            await asyncio.sleep(0.05)

    assert outage == 0
    assert [(event.start_time, event.end_time) for event in published] == [
        (window.start, window.end)
    ]


async def test_build_file_footer_carries_its_own_provenance(
    coordination_store,
    build_queue,
    arrow_store,
    builder_metrics,
):
    """Every build file is self-describing: the parquet footer carries the
    build's own manifest entry, so a copied file keeps its provenance."""
    db = InMemoryTimeSeriesDatabase()
    record = await _seed_record(coordination_store, arrived_at=_arrived(1))
    await _seed_rows(db, record, [0, 6], [20.0, 21.0])
    published = []
    builder = _builder(
        coordination_store,
        build_queue,
        db,
        builder_metrics,
        arrow_store,
        published,
    )
    await builder.build_window(WINDOW)

    schema = pq.read_schema(
        arrow_store._get_path(published[0].dataset_locations[0])
    )
    build = ManifestBuild.model_validate_json(schema.metadata[BUILD.encode()])
    assert build.version == 1
    assert build.total_rows == 2
    assert build.record_ids_hash == RecordSet.from_list([record]).hash
    assert [r.id for r in build.records] == [record]
