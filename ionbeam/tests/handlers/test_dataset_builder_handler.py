import asyncio
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, List, Optional
from unittest.mock import AsyncMock
from uuid import uuid4

import pandas as pd
import pytest

from ionbeam_client.constants import LatitudeColumn, LongitudeColumn
from ionbeam_client.models import (
    CanonicalVariable,
    DataIngestionMap,
    DatasetMetadata,
    IngestionMetadata,
    LatitudeAxis,
    LongitudeAxis,
    MetadataVariable,
    TimeAxis,
)
from ionbeam.handlers.dataset_builder_handler import (
    DatasetBuilderHandler,
    DatasetBuilderConfig,
)
from ionbeam.models.service_models import IngestionRecord, Window, WindowBuildState
from ionbeam.observability.protocols import BuilderMetricsProtocol


@pytest.fixture
def builder_handler(
    mock_ingestion_record_store,
    mock_ordered_queue,
    mock_timeseries_db,
    mock_builder_metrics: BuilderMetricsProtocol,
    mock_arrow_store,
):
    config = DatasetBuilderConfig(
        queue_key="dataset_queue",
        poll_interval_seconds=0.1,
        delete_after_export=False,
        concurrency=1,
    )
    mock_publisher = AsyncMock()
    return DatasetBuilderHandler(
        config,
        mock_ingestion_record_store,
        mock_ordered_queue,
        mock_timeseries_db,
        mock_builder_metrics,
        mock_arrow_store,
        event_publisher=mock_publisher,
    )


def _create_metadata(
    aggregation_span: timedelta, stc_window: timedelta
) -> IngestionMetadata:
    """Helper to create metadata with configurable aggregation and STC windows."""
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
            lat=LatitudeAxis(
                from_col="latitude", standard_name="latitude", cf_unit="degrees_north"
            ),
            lon=LongitudeAxis(
                from_col="longitude", standard_name="longitude", cf_unit="degrees_east"
            ),
            canonical_variables=[
                CanonicalVariable(
                    column="temperature",
                    standard_name="air_temperature",
                    cf_unit="deg_C",
                )
            ],
            metadata_variables=[MetadataVariable(column="station_id")],
        ),
    )


async def _wait_for_condition(condition_fn, timeout: float = 5.0) -> bool:
    """Wait for a condition to become true with exponential backoff."""
    start = asyncio.get_event_loop().time()
    interval = 0.01

    while asyncio.get_event_loop().time() - start < timeout:
        if await condition_fn():
            return True
        await asyncio.sleep(interval)
        interval = min(interval * 1.5, 0.5)  # Cap at 500ms

    return False


class TestDatasetBuilderHandler:
    """Test suite for DatasetBuilderHandler."""

    @pytest.mark.asyncio
    async def test_builder_processes_queued_window_and_creates_parquet(
        self,
        builder_handler,
        mock_ingestion_record_store,
        mock_arrow_store,
    ) -> None:
        """Builder: Pops window from queue, fetches data, creates dataset artifact."""
        metadata = _create_metadata(
            aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1)
        )

        event_start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        event_end = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        event_id = uuid4()

        record = IngestionRecord(
            id=event_id, metadata=metadata, start_time=event_start, end_time=event_end
        )
        await mock_ingestion_record_store.save_ingestion_record(record)

        window_id = f"{event_start.isoformat()}_PT1H"
        dataset_key = f"test_dataset:{window_id}"
        window = Window.from_dataset_key(dataset_key)
        await mock_ingestion_record_store.set_desired_event_ids(window, [str(event_id)])

        priority = -int(window.end.timestamp())
        await builder_handler.build_window(window, priority)

        stored_keys = mock_arrow_store.list_keys()
        assert len(stored_keys) == 1
        stored_dataset_key = stored_keys[0]
        assert stored_dataset_key.startswith("test_dataset/20240101T100000")
        assert mock_arrow_store.get_total_rows(stored_dataset_key) > 0

        state = await mock_ingestion_record_store.get_window_state(window)
        assert state is not None
        assert state.event_ids_hash is not None

    @pytest.mark.asyncio
    async def test_builder_processes_multiple_queued_windows(
        self,
        builder_handler,
        mock_ingestion_record_store,
        mock_arrow_store,
    ) -> None:
        """Builder: Processes multiple queued windows from coordinator, creates multiple dataset artifacts."""
        metadata = _create_metadata(
            aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1)
        )

        event_start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        event_end = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
        event_id = uuid4()

        record = IngestionRecord(
            id=event_id, metadata=metadata, start_time=event_start, end_time=event_end
        )
        await mock_ingestion_record_store.save_ingestion_record(record)

        for hour in range(3):
            ws = event_start + timedelta(hours=hour)
            window_id = f"{ws.isoformat()}_PT1H"
            dataset_key = f"test_dataset:{window_id}"
            priority = -int((ws + timedelta(hours=1)).timestamp())

            window = Window.from_dataset_key(dataset_key)
            await mock_ingestion_record_store.set_desired_event_ids(
                window, [str(event_id)]
            )

            await builder_handler.build_window(window, priority)

        stored_keys = mock_arrow_store.list_keys()
        assert len(stored_keys) == 3

    @pytest.mark.asyncio
    async def test_builder_does_not_modify_existing_datasets(
        self,
        builder_handler,
        mock_ingestion_record_store,
        mock_arrow_store,
    ) -> None:
        """Builder: Processes new window without touching existing dataset artifacts."""
        metadata = _create_metadata(
            aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1)
        )

        event1_start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        event1_end = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        event1_id = uuid4()

        record1 = IngestionRecord(
            id=event1_id,
            metadata=metadata,
            start_time=event1_start,
            end_time=event1_end,
        )
        await mock_ingestion_record_store.save_ingestion_record(record1)

        window1_id = f"{event1_start.isoformat()}_PT1H"
        dataset1_key = f"test_dataset:{window1_id}"
        window1 = Window.from_dataset_key(dataset1_key)
        await mock_ingestion_record_store.set_desired_event_ids(
            window1, [str(event1_id)]
        )

        priority1 = -int(window1.end.timestamp())
        await builder_handler.build_window(window1, priority1)

        stored_keys = mock_arrow_store.list_keys()
        assert len(stored_keys) == 1
        first_dataset_key = stored_keys[0]

        event2_start = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        event2_end = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        event2_id = uuid4()

        record2 = IngestionRecord(
            id=event2_id,
            metadata=metadata,
            start_time=event2_start,
            end_time=event2_end,
        )
        await mock_ingestion_record_store.save_ingestion_record(record2)

        window2_id = f"{event2_start.isoformat()}_PT1H"
        dataset2_key = f"test_dataset:{window2_id}"
        window2 = Window.from_dataset_key(dataset2_key)
        await mock_ingestion_record_store.set_desired_event_ids(
            window2, [str(event2_id)]
        )

        priority2 = -int(window2.end.timestamp())
        await builder_handler.build_window(window2, priority2)

        stored_keys = mock_arrow_store.list_keys()
        assert len(stored_keys) == 2
        assert first_dataset_key in stored_keys

    @pytest.mark.asyncio
    async def test_builder_pops_highest_priority_from_queue(
        self,
        builder_handler,
        mock_ingestion_record_store,
        mock_ordered_queue,
        mock_arrow_store,
    ) -> None:
        """Verify builder picks highest score (oldest window) first."""
        metadata = _create_metadata(
            aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1)
        )

        for hour in [10, 11, 12]:
            event_start = datetime(2024, 1, 1, hour, 0, 0, tzinfo=timezone.utc)
            event_end = event_start + timedelta(hours=1)
            event_id = uuid4()

            record = IngestionRecord(
                id=event_id,
                metadata=metadata,
                start_time=event_start,
                end_time=event_end,
            )
            await mock_ingestion_record_store.save_ingestion_record(record)

            window_id = f"{event_start.isoformat()}_PT1H"
            dataset_key = f"test_dataset:{window_id}"
            window = Window.from_dataset_key(dataset_key)
            await mock_ingestion_record_store.set_desired_event_ids(
                window, [str(event_id)]
            )

            score = -int(window.end.timestamp())
            await mock_ordered_queue.enqueue(window, score)

        async with builder_handler:

            async def has_one_dataset():
                return len(mock_arrow_store.list_keys()) >= 1

            await _wait_for_condition(has_one_dataset, timeout=5.0)

        stored_keys = mock_arrow_store.list_keys()
        assert stored_keys
        first_key = stored_keys[0]
        assert "20240101T100000" in first_key

    @pytest.mark.asyncio
    async def test_builder_updates_observed_state_after_build(
        self,
        builder_handler,
        mock_ingestion_record_store,
        mock_arrow_store,
    ) -> None:
        """Verify state hash written after successful build."""
        metadata = _create_metadata(
            aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1)
        )

        event_start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        event_end = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        event_id = uuid4()

        record = IngestionRecord(
            id=event_id, metadata=metadata, start_time=event_start, end_time=event_end
        )
        await mock_ingestion_record_store.save_ingestion_record(record)

        window_id = f"{event_start.isoformat()}_PT1H"
        dataset_key = f"test_dataset:{window_id}"
        window = Window.from_dataset_key(dataset_key)

        await mock_ingestion_record_store.set_desired_event_ids(window, [str(event_id)])

        priority = -int(window.end.timestamp())
        await builder_handler.build_window(window, priority)

        state = await mock_ingestion_record_store.get_window_state(window)
        assert state is not None
        assert state.event_ids_hash is not None
        assert state.event_ids_hash != ""
        assert len(mock_arrow_store.list_keys()) == 1

    @pytest.mark.asyncio
    async def test_builder_skips_build_when_observed_matches_desired(
        self,
        builder_handler,
        mock_ingestion_record_store,
        mock_arrow_store,
    ) -> None:
        """Builder: Skips build when observed state hash already matches desired state."""
        metadata = _create_metadata(
            aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1)
        )

        event_start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        event_end = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        event_id = uuid4()

        record = IngestionRecord(
            id=event_id, metadata=metadata, start_time=event_start, end_time=event_end
        )
        await mock_ingestion_record_store.save_ingestion_record(record)

        window_id = f"{event_start.isoformat()}_PT1H"
        dataset_key = f"test_dataset:{window_id}"
        window = Window.from_dataset_key(dataset_key)

        desired_ids = [str(event_id)]
        await mock_ingestion_record_store.set_desired_event_ids(window, desired_ids)

        import hashlib

        desired_hash = hashlib.sha256(
            ",".join(sorted(desired_ids)).encode("utf-8")
        ).hexdigest()
        state = WindowBuildState(event_ids_hash=desired_hash, timestamp=event_start)
        await mock_ingestion_record_store.set_window_state(window, state)

        priority = -int(window.end.timestamp())
        await builder_handler.build_window(window, priority)

        assert len(mock_arrow_store.list_keys()) == 0

    @pytest.mark.asyncio
    async def test_builder_requeues_on_failure(
        self,
        mock_ingestion_record_store,
        mock_ordered_queue,
        failing_timeseries_db,
        mock_arrow_store,
        mock_builder_metrics: BuilderMetricsProtocol,
    ) -> None:
        """Verify failed builds return to queue with reason."""
        metadata = _create_metadata(
            aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1)
        )

        event_start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        event_end = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        event_id = uuid4()

        record = IngestionRecord(
            id=event_id, metadata=metadata, start_time=event_start, end_time=event_end
        )
        await mock_ingestion_record_store.save_ingestion_record(record)

        window_id = f"{event_start.isoformat()}_PT1H"
        dataset_key = f"test_dataset:{window_id}"
        window = Window.from_dataset_key(dataset_key)
        await mock_ingestion_record_store.set_desired_event_ids(window, [str(event_id)])

        from unittest.mock import AsyncMock

        config = DatasetBuilderConfig(
            queue_key="dataset_queue", poll_interval_seconds=0.1, concurrency=1
        )
        mock_publisher = AsyncMock()
        builder = DatasetBuilderHandler(
            config,
            mock_ingestion_record_store,
            mock_ordered_queue,
            failing_timeseries_db,
            mock_builder_metrics,
            mock_arrow_store,
            event_publisher=mock_publisher,
        )

        score = -int(window.end.timestamp())
        await builder.build_window(window, score)

        # Verify window was requeued
        queue_dict = mock_ordered_queue.get_queue_dict()
        assert window.dataset_key in queue_dict
        assert queue_dict[window.dataset_key] == score
        assert len(mock_arrow_store.list_keys()) == 0

    @pytest.mark.asyncio
    async def test_builder_publishes_dataset_available_event(
        self,
        mock_ingestion_record_store,
        mock_ordered_queue,
        mock_timeseries_db,
        mock_arrow_store,
        mock_builder_metrics: BuilderMetricsProtocol,
    ) -> None:
        """Verify DataSetAvailableEvent published via event publisher."""
        from unittest.mock import AsyncMock

        metadata = _create_metadata(
            aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1)
        )

        event_start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        event_end = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        event_id = uuid4()

        record = IngestionRecord(
            id=event_id, metadata=metadata, start_time=event_start, end_time=event_end
        )
        await mock_ingestion_record_store.save_ingestion_record(record)

        window_id = f"{event_start.isoformat()}_PT1H"
        dataset_key = f"test_dataset:{window_id}"
        window = Window.from_dataset_key(dataset_key)

        await mock_ingestion_record_store.set_desired_event_ids(window, [str(event_id)])

        mock_publisher = AsyncMock()

        config = DatasetBuilderConfig(
            queue_key="dataset_queue", poll_interval_seconds=0.1, concurrency=1
        )
        builder = DatasetBuilderHandler(
            config,
            mock_ingestion_record_store,
            mock_ordered_queue,
            mock_timeseries_db,
            mock_builder_metrics,
            mock_arrow_store,
            event_publisher=mock_publisher,
        )

        priority = -int(window.end.timestamp())
        await builder.build_window(window, priority)

        assert mock_publisher.called
        assert len(mock_arrow_store.list_keys()) == 1

    @pytest.mark.asyncio
    async def test_builder_respects_concurrency_limit(
        self,
        mock_ingestion_record_store,
        mock_ordered_queue,
        mock_timeseries_db,
        mock_arrow_store,
        mock_builder_metrics: BuilderMetricsProtocol,
    ) -> None:
        """Verify max concurrent builds honored."""
        metadata = _create_metadata(
            aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1)
        )

        for hour in range(10, 15):
            event_start = datetime(2024, 1, 1, hour, 0, 0, tzinfo=timezone.utc)
            event_end = event_start + timedelta(hours=1)
            event_id = uuid4()

            record = IngestionRecord(
                id=event_id,
                metadata=metadata,
                start_time=event_start,
                end_time=event_end,
            )
            await mock_ingestion_record_store.save_ingestion_record(record)

            window_id = f"{event_start.isoformat()}_PT1H"
            dataset_key = f"test_dataset:{window_id}"
            window = Window.from_dataset_key(dataset_key)
            await mock_ingestion_record_store.set_desired_event_ids(
                window, [str(event_id)]
            )

            score = -int(window.end.timestamp())
            await mock_ordered_queue.enqueue(window, score)

        config = DatasetBuilderConfig(
            queue_key="dataset_queue", poll_interval_seconds=0.1, concurrency=2
        )
        mock_publisher = AsyncMock()
        builder = DatasetBuilderHandler(
            config,
            mock_ingestion_record_store,
            mock_ordered_queue,
            mock_timeseries_db,
            mock_builder_metrics,
            mock_arrow_store,
            event_publisher=mock_publisher,
        )

        async with builder:

            async def has_inflight():
                return len(builder._inflight) > 0

            await _wait_for_condition(has_inflight, timeout=2.0)

            assert len(builder._inflight) <= 2

            async def all_complete():
                return len(mock_arrow_store.list_keys()) >= 5

            await _wait_for_condition(all_complete, timeout=10.0)

        assert len(mock_arrow_store.list_keys()) == 5
