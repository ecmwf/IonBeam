import json
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from isodate import duration_isoformat

from ionbeam.models.models import (
    CanonicalVariable,
    DataAvailableEvent,
    DataIngestionMap,
    DatasetMetadata,
    IngestionMetadata,
    LatitudeAxis,
    LongitudeAxis,
    MetadataVariable,
    TimeAxis,
)
from ionbeam.observability.metrics import IonbeamMetricsProtocol
from ionbeam.services.dataset_coordinator import (
    DatasetCoordinatorConfig,
    DatasetCoordinatorService,
)
from ionbeam.services.models import IngestionRecord
from tests.conftest import MockEventStore


@pytest.fixture
def coordinator_service(mock_event_store: MockEventStore, mock_metrics: IonbeamMetricsProtocol) -> DatasetCoordinatorService:
    config = DatasetCoordinatorConfig(only_process_spanned_windows=True)
    return DatasetCoordinatorService(config, mock_event_store, mock_metrics)


def _create_metadata(aggregation_span: timedelta, stc_window: timedelta) -> IngestionMetadata:
    return IngestionMetadata(
        dataset=DatasetMetadata(
            name="test_dataset",
            description="Test dataset",
            aggregation_span=aggregation_span,
            source_links=[],
            keywords=[],
            subject_to_change_window=stc_window,
        ),
        ingestion_map=DataIngestionMap(
            datetime=TimeAxis(from_col="timestamp"),
            lat=LatitudeAxis(from_col="latitude", standard_name="latitude", cf_unit="degrees_north"),
            lon=LongitudeAxis(from_col="longitude", standard_name="longitude", cf_unit="degrees_east"),
            canonical_variables=[
                CanonicalVariable(column="temperature", standard_name="air_temperature", cf_unit="deg_C")
            ],
            metadata_variables=[MetadataVariable(column="station_id")],
        ),
    )


def _create_data_available_event(
    event_id: UUID, metadata: IngestionMetadata, start_time: datetime, end_time: datetime
) -> DataAvailableEvent:
    return DataAvailableEvent(id=event_id, metadata=metadata, start_time=start_time, end_time=end_time)


def _align_to_aggregation(ts: datetime, aggregation: timedelta) -> datetime:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = ts - epoch
    aligned_seconds = (delta.total_seconds() // aggregation.total_seconds()) * aggregation.total_seconds()
    return epoch + timedelta(seconds=aligned_seconds)


def _window_id(window_start: datetime, aggregation: timedelta) -> str:
    return f"{window_start.isoformat()}_{duration_isoformat(aggregation)}"


def _get_queue_map(store: MockEventStore, key: str = "dataset_queue") -> dict:
    raw = store.events.get(key)
    return json.loads(raw) if raw else {}


class TestDatasetCoordinator:
    """Test suite for DatasetCoordinator service - focuses on coordinator responsibilities."""

    @pytest.mark.asyncio
    async def test_coordinator_stores_ingestion_record(
        self,
        coordinator_service: DatasetCoordinatorService,
        mock_event_store: MockEventStore,
    ) -> None:
        """Coordinator: Stores ingestion record when receiving DataAvailableEvent."""
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))

        event_start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        event_end = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        event = _create_data_available_event(uuid4(), metadata, event_start, event_end)

        await coordinator_service.handle(event)

        # Event persisted
        stored = await mock_event_store.get_event(f"ingestion_events:{metadata.dataset.name}:{event.id}")
        assert stored is not None
        record = IngestionRecord.model_validate_json(stored)
        assert record.id == event.id
        assert record.start_time == event_start
        assert record.end_time == event_end

    @pytest.mark.asyncio
    async def test_coordinator_updates_window_event_ids(
        self,
        coordinator_service: DatasetCoordinatorService,
        mock_event_store: MockEventStore,
    ) -> None:
        """Coordinator: Updates desired state (event_ids) for affected windows."""
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))

        event_start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        event_end = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        event = _create_data_available_event(uuid4(), metadata, event_start, event_end)

        await coordinator_service.handle(event)

        # Window event_ids updated
        ws = _align_to_aggregation(event_start, metadata.dataset.aggregation_span)
        wid = _window_id(ws, metadata.dataset.aggregation_span)
        ev_ids_raw = await mock_event_store.get_event(f"{metadata.dataset.name}:{wid}:event_ids")
        assert ev_ids_raw is not None
        ids = json.loads(ev_ids_raw)
        assert str(event.id) in ids

    @pytest.mark.asyncio
    async def test_coordinator_enqueues_complete_window(
        self,
        coordinator_service: DatasetCoordinatorService,
        mock_event_store: MockEventStore,
    ) -> None:
        """Coordinator: Enqueues window when coverage is complete."""
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))

        event_start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        event_end = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        event = _create_data_available_event(uuid4(), metadata, event_start, event_end)

        await coordinator_service.handle(event)

        # Queue has exactly one member with expected score
        queue = _get_queue_map(mock_event_store)
        assert len(queue) == 1
        ws = _align_to_aggregation(event_start, metadata.dataset.aggregation_span)
        wid = _window_id(ws, metadata.dataset.aggregation_span)
        member = f"{metadata.dataset.name}:{wid}"
        assert member in queue
        expected_score = -int((ws + metadata.dataset.aggregation_span).timestamp())
        assert queue[member] == expected_score

    @pytest.mark.asyncio
    async def test_coordinator_aggregates_multiple_events_into_single_window(
        self,
        mock_event_store: MockEventStore,
        mock_metrics: IonbeamMetricsProtocol
    ) -> None:
        """Coordinator: Aggregates 24 hourly events into one daily window."""
        metadata = _create_metadata(aggregation_span=timedelta(days=1), stc_window=timedelta(hours=1))
        service = DatasetCoordinatorService(DatasetCoordinatorConfig(), mock_event_store, mock_metrics)

        base = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        events = []
        for h in range(24):
            start = base + timedelta(hours=h)
            end = start + timedelta(hours=1)
            events.append(_create_data_available_event(uuid4(), metadata, start, end))

        for ev in events:
            await service.handle(ev)

        ws = _align_to_aggregation(base, metadata.dataset.aggregation_span)
        wid = _window_id(ws, metadata.dataset.aggregation_span)
        
        # Verify all event IDs are in desired state
        ev_ids_raw = await mock_event_store.get_event(f"{metadata.dataset.name}:{wid}:event_ids")
        assert ev_ids_raw is not None
        ids = json.loads(ev_ids_raw)
        assert len(ids) == 24
        for ev in events:
            assert str(ev.id) in ids
        
        # Verify window is enqueued
        queue = _get_queue_map(mock_event_store)
        assert len(queue) == 1
        assert f"{metadata.dataset.name}:{wid}" in queue

    @pytest.mark.asyncio
    async def test_coordinator_splits_large_event_into_multiple_windows(
        self,
        mock_event_store: MockEventStore,
        mock_metrics: IonbeamMetricsProtocol
    ) -> None:
        """Coordinator: Single 3-hour event creates 3 hourly window entries."""
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))
        service = DatasetCoordinatorService(DatasetCoordinatorConfig(), mock_event_store, mock_metrics)

        start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
        ev = _create_data_available_event(uuid4(), metadata, start, end)
        await service.handle(ev)

        queue = _get_queue_map(mock_event_store)
        assert len(queue) == 3

        # Verify all 3 hourly windows are present
        expected_starts = [datetime(2024, 1, 1, h, 0, 0, tzinfo=timezone.utc) for h in (10, 11, 12)]
        for ws in expected_starts:
            wid = _window_id(ws, timedelta(hours=1))
            assert f"{metadata.dataset.name}:{wid}" in queue
            
            # Verify event_ids updated for each window
            ev_ids_raw = await mock_event_store.get_event(f"{metadata.dataset.name}:{wid}:event_ids")
            assert ev_ids_raw is not None
            ids = json.loads(ev_ids_raw)
            assert str(ev.id) in ids

    @pytest.mark.asyncio
    async def test_coordinator_detects_gaps_and_skips_incomplete_window(
        self,
        mock_event_store: MockEventStore,
        mock_metrics: IonbeamMetricsProtocol
    ) -> None:
        """Coordinator: Gaps in coverage prevent window from being enqueued."""
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))
        service = DatasetCoordinatorService(DatasetCoordinatorConfig(), mock_event_store, mock_metrics)

        base = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

        # 10:00-10:30
        e1 = _create_data_available_event(uuid4(), metadata, base, base + timedelta(minutes=30))
        # 10:45-11:00 (gap 10:30-10:45)
        e2 = _create_data_available_event(uuid4(), metadata, base + timedelta(minutes=45), base + timedelta(hours=1))
        # 11:00-12:00 full coverage next window
        e3 = _create_data_available_event(uuid4(), metadata, base + timedelta(hours=1), base + timedelta(hours=2))

        for ev in (e1, e2, e3):
            await service.handle(ev)

        queue = _get_queue_map(mock_event_store)
        # Only 11:00-12:00 expected
        assert len(queue) == 1
        wid = _window_id(datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc), timedelta(hours=1))
        assert f"{metadata.dataset.name}:{wid}" in queue
        
        # Verify 10:00-11:00 window has event_ids but is NOT enqueued
        wid_incomplete = _window_id(base, timedelta(hours=1))
        ev_ids_raw = await mock_event_store.get_event(f"{metadata.dataset.name}:{wid_incomplete}:event_ids")
        assert ev_ids_raw is not None  # Event IDs are tracked
        assert f"{metadata.dataset.name}:{wid_incomplete}" not in queue  # But not enqueued

    @pytest.mark.asyncio
    async def test_coordinator_respects_stc_window(
        self,
        mock_event_store: MockEventStore,
        mock_metrics: IonbeamMetricsProtocol
    ) -> None:
        """Coordinator: Events within STC cutoff should not enqueue windows."""
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=2))
        service = DatasetCoordinatorService(DatasetCoordinatorConfig(), mock_event_store, mock_metrics)

        now = datetime.now(timezone.utc)
        event_start = now - timedelta(minutes=90)
        event_end = event_start + timedelta(hours=1)

        ev = _create_data_available_event(uuid4(), metadata, event_start, event_end)
        await service.handle(ev)

        # Event should be stored
        stored = await mock_event_store.get_event(f"test_dataset:{ev.id}")
        
        # But window should not be enqueued (within STC)
        queue = _get_queue_map(mock_event_store)
        assert len(queue) == 0

    @pytest.mark.asyncio
    async def test_coordinator_skips_enqueue_when_observed_matches_desired(
        self,
        mock_event_store: MockEventStore,
        mock_metrics: IonbeamMetricsProtocol
    ) -> None:
        """Coordinator: If observed state hash matches desired, coordinator should not enqueue."""
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))
        service = DatasetCoordinatorService(DatasetCoordinatorConfig(), mock_event_store, mock_metrics)

        ws = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        we = ws + timedelta(hours=1)

        ev = _create_data_available_event(uuid4(), metadata, ws, we)

        # Pre-seed observed state matching this single event
        wid = _window_id(ws, metadata.dataset.aggregation_span)
        desired_ids = [str(ev.id)]
        desired_hash = __import__("hashlib").sha256(",".join(sorted(desired_ids)).encode("utf-8")).hexdigest()
        await mock_event_store.store_event(
            f"{metadata.dataset.name}:{wid}:state",
            json.dumps({"event_ids_hash": desired_hash, "version": 1, "timestamp": ws.isoformat()}),
        )

        await service.handle(ev)

        # Event should be stored and event_ids updated
        stored = await mock_event_store.get_event(f"ingestion_events:{metadata.dataset.name}:{ev.id}")
        assert stored is not None
        
        ev_ids_raw = await mock_event_store.get_event(f"{metadata.dataset.name}:{wid}:event_ids")
        assert ev_ids_raw is not None

        # But queue should be empty (observed matches desired)
        queue = _get_queue_map(mock_event_store)
        assert len(queue) == 0

    @pytest.mark.asyncio
    async def test_coordinator_only_processes_spanned_windows(
        self,
        mock_event_store: MockEventStore,
        mock_metrics: IonbeamMetricsProtocol
    ) -> None:
        """Coordinator: Earlier unrelated window is not processed when only_process_spanned_windows is True."""
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))
        service = DatasetCoordinatorService(DatasetCoordinatorConfig(only_process_spanned_windows=True), mock_event_store, mock_metrics)

        # Pre-populate unrelated earlier window events (08:00-09:00) with full coverage
        early_start = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
        rec1 = IngestionRecord(id=uuid4(), metadata=metadata, start_time=early_start, end_time=early_start + timedelta(minutes=30))
        rec2 = IngestionRecord(id=uuid4(), metadata=metadata, start_time=early_start + timedelta(minutes=30), end_time=early_start + timedelta(hours=1))
        await mock_event_store.store_event(f"ingestion_events:{metadata.dataset.name}:{rec1.id}", rec1.model_dump_json())
        await mock_event_store.store_event(f"ingestion_events:{metadata.dataset.name}:{rec2.id}", rec2.model_dump_json())

        # Current event for 10:00-11:00 should not process earlier window
        current = _create_data_available_event(
            uuid4(),
            metadata,
            datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
        )
        await service.handle(current)

        queue = _get_queue_map(mock_event_store)
        assert len(queue) == 1
        wid_current = _window_id(datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc), timedelta(hours=1))
        assert f"{metadata.dataset.name}:{wid_current}" in queue
        wid_early = _window_id(datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc), timedelta(hours=1))
        assert f"{metadata.dataset.name}:{wid_early}" not in queue

    @pytest.mark.asyncio
    async def test_coordinator_processes_all_windows_when_flag_disabled(
        self,
        mock_event_store: MockEventStore,
        mock_metrics: IonbeamMetricsProtocol
    ) -> None:
        """Coordinator: Processes all complete windows when only_process_spanned_windows is False."""
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))
        service = DatasetCoordinatorService(DatasetCoordinatorConfig(only_process_spanned_windows=False), mock_event_store, mock_metrics)

        # Pre-populate earlier window with full coverage (08:00-09:00)
        early_start = datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
        rec1 = IngestionRecord(id=uuid4(), metadata=metadata, start_time=early_start, end_time=early_start + timedelta(hours=1))
        await mock_event_store.store_event(f"ingestion_events:{metadata.dataset.name}:{rec1.id}", rec1.model_dump_json())

        # Current event for 10:00-11:00 should trigger processing of both windows
        current = _create_data_available_event(
            uuid4(),
            metadata,
            datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
        )
        await service.handle(current)

        queue = _get_queue_map(mock_event_store)
        assert len(queue) == 2
        wid_current = _window_id(datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc), timedelta(hours=1))
        wid_early = _window_id(datetime(2024, 1, 1, 8, 0, 0, tzinfo=timezone.utc), timedelta(hours=1))
        assert f"{metadata.dataset.name}:{wid_current}" in queue
        assert f"{metadata.dataset.name}:{wid_early}" in queue

    @pytest.mark.asyncio
    async def test_coordinator_assigns_priority_scores_correctly(
        self,
        mock_event_store: MockEventStore,
        mock_metrics: IonbeamMetricsProtocol
    ) -> None:
        """Coordinator: Older window should have a higher (less negative) score, thus higher priority."""
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))
        service = DatasetCoordinatorService(DatasetCoordinatorConfig(), mock_event_store, mock_metrics)

        # Two independent full windows
        ws1 = datetime(2024, 1, 1, 9, 0, 0, tzinfo=timezone.utc)
        we1 = ws1 + timedelta(hours=1)
        ws2 = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        we2 = ws2 + timedelta(hours=1)

        e1 = _create_data_available_event(uuid4(), metadata, ws1, we1)
        e2 = _create_data_available_event(uuid4(), metadata, ws2, we2)

        for ev in (e1, e2):
            await service.handle(ev)

        queue = _get_queue_map(mock_event_store)
        m1 = f"{metadata.dataset.name}:{_window_id(ws1, timedelta(hours=1))}"
        m2 = f"{metadata.dataset.name}:{_window_id(ws2, timedelta(hours=1))}"

        assert m1 in queue and m2 in queue
        # Older window (ws1) has earlier window_end -> higher score (less negative)
        assert queue[m1] > queue[m2]

    @pytest.mark.asyncio
    async def test_coordinator_handles_backfill_completing_window(
        self,
        mock_event_store: MockEventStore,
        mock_metrics: IonbeamMetricsProtocol
    ) -> None:
        """Coordinator: Late-arriving backfill data triggers enqueue when window becomes complete."""
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta())
        service = DatasetCoordinatorService(DatasetCoordinatorConfig(), mock_event_store, mock_metrics)

        base = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

        # Partial events that leave a gap
        event_b = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=base + timedelta(minutes=9),
            end_time=base + timedelta(minutes=30),
        )
        event_c = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=base + timedelta(minutes=30),
            end_time=base + timedelta(hours=1),
        )

        # Process partial events first - should not enqueue due to gap
        for ev in [event_b, event_c]:
            await service.handle(ev)

        queue = _get_queue_map(mock_event_store)
        assert len(queue) == 0, "Partial coverage should not enqueue window"

        # Later backfill that fills the gap
        event_a_full = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=base,
            end_time=base + timedelta(minutes=20),
        )

        await service.handle(event_a_full)

        # Now window should be enqueued
        queue = _get_queue_map(mock_event_store)
        assert len(queue) == 1, "Backfill completing coverage should enqueue window"
        
        wid = _window_id(base, timedelta(hours=1))
        assert f"{metadata.dataset.name}:{wid}" in queue
        
        # Verify all three events are in desired state
        ev_ids_raw = await mock_event_store.get_event(f"{metadata.dataset.name}:{wid}:event_ids")
        assert ev_ids_raw is not None
        ids = json.loads(ev_ids_raw)
        assert len(ids) == 3
        assert str(event_a_full.id) in ids
        assert str(event_b.id) in ids
        assert str(event_c.id) in ids

    @pytest.mark.asyncio
    async def test_coordinator_re_enqueues_when_new_event_changes_desired_state(
        self,
        mock_event_store: MockEventStore,
        mock_metrics: IonbeamMetricsProtocol
    ) -> None:
        """Coordinator: New event for already-built window triggers re-enqueue if desired state changes."""
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))
        service = DatasetCoordinatorService(DatasetCoordinatorConfig(), mock_event_store, mock_metrics)

        ws = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        we = ws + timedelta(hours=1)

        # First event completes window
        ev1 = _create_data_available_event(uuid4(), metadata, ws, we)
        await service.handle(ev1)

        queue = _get_queue_map(mock_event_store)
        assert len(queue) == 1

        wid = _window_id(ws, timedelta(hours=1))
        
        # Simulate builder completing the window
        ev_ids_raw = await mock_event_store.get_event(f"{metadata.dataset.name}:{wid}:event_ids")
        ids = json.loads(ev_ids_raw)
        observed_hash = __import__("hashlib").sha256(",".join(sorted(ids)).encode("utf-8")).hexdigest()
        await mock_event_store.store_event(
            f"{metadata.dataset.name}:{wid}:state",
            json.dumps({"event_ids_hash": observed_hash, "timestamp": ws.isoformat()})
        )
        
        # Clear queue (simulating builder processed it)
        await mock_event_store.store_event("dataset_queue", json.dumps({}))

        # New overlapping event arrives (e.g., backfill or correction)
        ev2 = _create_data_available_event(uuid4(), metadata, ws, we)
        await service.handle(ev2)

        # Window should be re-enqueued because desired state changed
        queue = _get_queue_map(mock_event_store)
        assert len(queue) == 1
        assert f"{metadata.dataset.name}:{wid}" in queue
        
        # Verify both events are in desired state
        ev_ids_raw = await mock_event_store.get_event(f"{metadata.dataset.name}:{wid}:event_ids")
        ids = json.loads(ev_ids_raw)
        assert len(ids) == 2
        assert str(ev1.id) in ids
        assert str(ev2.id) in ids
