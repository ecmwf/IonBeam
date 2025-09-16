import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import AsyncIterator, Generator, List, Optional
from uuid import UUID, uuid4

import pandas as pd
import pytest

from ionbeam.core.constants import LatitudeColumn, LongitudeColumn
from ionbeam.models.models import (
    CanonicalVariable,
    DataAvailableEvent,
    DataIngestionMap,
    DataSetAvailableEvent,
    DatasetMetadata,
    IngestionMetadata,
    LatitudeAxis,
    LongitudeAxis,
    MetadataVariable,
    TimeAxis,
)
from ionbeam.services.dataset_aggregation import DatasetAggregatorConfig, DatasetAggregatorService
from ionbeam.storage.event_store import EventStore
from ionbeam.storage.timeseries import TimeSeriesDatabase


class MockEventStore(EventStore):
    """Mock implementation for testing."""
    
    def __init__(self) -> None:
        self.events: dict[str, str] = {}
    
    async def get_event(self, key: str) -> Optional[str]:
        return self.events.get(key)
    
    async def store_event(self, key: str, event_json: str) -> None:
        self.events[key] = event_json
    
    async def get_events(self, pattern: str) -> List[str]:
        # Simple pattern matching for tests
        prefix = pattern.replace('*', '')
        return [v for k, v in self.events.items() if k.startswith(prefix)]


class MockTimeSeriesDatabase(TimeSeriesDatabase):
    """Mock implementation for testing."""
    
    def __init__(self) -> None:
        self.data: List[pd.DataFrame] = []
        self.delete_calls: List[tuple[str, datetime, datetime]] = []
    
    async def query_measurement_data(
        self, 
        measurement: str, 
        start_time: datetime, 
        end_time: datetime, 
        slice_duration: Optional[timedelta] = None
    ) -> AsyncIterator[pd.DataFrame]:
        # Return test data as async generator with static timestamps in InfluxDB long format
        time_range = pd.date_range(start_time, end_time, freq='1min', tz='UTC')[:-1]
        
        # Create records for each field (temperature, lat, lon) at each timestamp
        records = []
        for i, timestamp in enumerate(time_range):
            # Temperature field
            records.append({
                '_time': timestamp,
                '_measurement': measurement,
                '_field': 'air_temperature__deg_C______',
                '_value': 20.0 + i * 0.1,
                'station_id': 'test_station'
            })
            # Latitude field
            records.append({
                '_time': timestamp,
                '_measurement': measurement,
                '_field': LatitudeColumn,
                '_value': 52.5,
                'station_id': 'test_station'
            })
            # Longitude field
            records.append({
                '_time': timestamp,
                '_measurement': measurement,
                '_field': LongitudeColumn,
                '_value': 13.4,
                'station_id': 'test_station'
            })
        
        df = pd.DataFrame(records)
        yield df
    
    async def write_dataframe(
        self, 
        record: pd.DataFrame, 
        measurement_name: str, 
        tag_columns: List[str], 
        timestamp_column: str
    ) -> None:
        pass
    
    async def delete_measurement_data(
        self, 
        measurement: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> None:
        self.delete_calls.append((measurement, start_time, end_time))


@pytest.fixture
def mock_event_store() -> MockEventStore:
    return MockEventStore()


@pytest.fixture
def mock_timeseries_db() -> MockTimeSeriesDatabase:
    return MockTimeSeriesDatabase()


@pytest.fixture
def temp_data_path() -> Generator[Path, None, None]:
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def aggregation_service(
    mock_event_store: MockEventStore, 
    mock_timeseries_db: MockTimeSeriesDatabase, 
    temp_data_path: Path
) -> DatasetAggregatorService:
    config = DatasetAggregatorConfig(
        delete_after_export=False,
        data_path=temp_data_path,
        page_size=500
    )
    return DatasetAggregatorService(config, mock_event_store, mock_timeseries_db)


@pytest.fixture
def sample_metadata() -> IngestionMetadata:
    return _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))


def _create_metadata(aggregation_span: timedelta, stc_window: timedelta) -> IngestionMetadata:
    """Helper to create metadata with configurable aggregation and STC windows."""
    return IngestionMetadata(
        dataset=DatasetMetadata(
            name="test_dataset",
            description="Test dataset",
            aggregation_span=aggregation_span,
            source_links=[],
            keywords=[],
            subject_to_change_window=stc_window
        ),
        ingestion_map=DataIngestionMap(
            datetime=TimeAxis(from_col="timestamp"),
            lat=LatitudeAxis(from_col="latitude", standard_name="latitude", cf_unit="degrees_north"),
            lon=LongitudeAxis(from_col="longitude", standard_name="longitude", cf_unit="degrees_east"),
            canonical_variables=[
                CanonicalVariable(
                    column="temperature",
                    standard_name="air_temperature",
                    cf_unit="deg_C"
                )
            ],
            metadata_variables=[
                MetadataVariable(column="station_id")
            ]
        )
    )


def _create_data_available_event(event_id: UUID, metadata: IngestionMetadata, start_time: datetime, end_time: datetime) -> DataAvailableEvent:
    """Helper to create DataAvailableEvent."""
    return DataAvailableEvent(
        id=event_id,
        metadata=metadata,
        start_time=start_time,
        end_time=end_time
    )


class TestDatasetAggregatorService:
    """Test suite for DatasetAggregatorService."""
    
    @pytest.mark.asyncio
    async def test_single_event_creates_single_dataset(
        self, 
        aggregation_service: DatasetAggregatorService, 
        sample_metadata: IngestionMetadata, 
        mock_event_store: MockEventStore, 
        temp_data_path: Path
    ) -> None:
        """Test that a single event with complete window coverage creates a dataset."""
        # Create a data available event with static timestamps (outside STC window)
        event_start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        event_end = datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        event = DataAvailableEvent(
            id=uuid4(),
            metadata=sample_metadata,
            start_time=event_start,
            end_time=event_end
        )
        
        # Process aggregation
        datasets: List[DataSetAvailableEvent] = []
        async for dataset_event in await aggregation_service.handle(event):
            datasets.append(dataset_event)
        
        # Assert dataset was created
        assert len(datasets) == 1
        dataset_event = datasets[0]
        
        assert isinstance(dataset_event, DataSetAvailableEvent)
        assert dataset_event.metadata.name == "test_dataset"
        assert dataset_event.dataset_location.exists()
        assert dataset_event.dataset_location.suffix == '.parquet'
        assert dataset_event.start_time == event_start
        assert dataset_event.end_time == event_end
        
        # Assert event was stored
        event_key = f"ingestion_events:test_dataset:{event.id}"
        stored_event = await mock_event_store.get_event(event_key)
        assert stored_event is not None
        
        # Assert window events were recorded
        window_events = await mock_event_store.get_events("window_events:test_dataset:")
        assert len(window_events) > 0

    @pytest.mark.asyncio
    async def test_multiple_hourly_events_create_daily_dataset(
        self,
        mock_event_store: MockEventStore,
        mock_timeseries_db: MockTimeSeriesDatabase,
        temp_data_path: Path
    ) -> None:
        """Test that 24 hourly events can satisfy a daily aggregation dataset."""
        # Create metadata with daily aggregation (24 hours) and 1-hour STC window
        metadata = _create_metadata(aggregation_span=timedelta(days=1), stc_window=timedelta(hours=1))
        
        # Create aggregation service
        config = DatasetAggregatorConfig(
            delete_after_export=False,
            data_path=temp_data_path,
            page_size=500
        )
        aggregation_service = DatasetAggregatorService(config, mock_event_store, mock_timeseries_db)
        
        # Create 24 hourly events for a full day (well outside STC window)
        base_date = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        events = []
        
        for hour in range(24):
            event_start = base_date + timedelta(hours=hour)
            event_end = event_start + timedelta(hours=1)
            event = _create_data_available_event(
                event_id=uuid4(),
                metadata=metadata,
                start_time=event_start,
                end_time=event_end
            )
            events.append(event)
        
        # Process all events through aggregation service
        all_datasets: List[DataSetAvailableEvent] = []
        for event in events:
            async for dataset_event in await aggregation_service.handle(event):
                all_datasets.append(dataset_event)
        
        # Should create exactly one daily dataset after processing all 24 events
        assert len(all_datasets) == 1
        dataset_event = all_datasets[0]
        
        # Verify dataset properties
        assert isinstance(dataset_event, DataSetAvailableEvent)
        assert dataset_event.metadata.name == "test_dataset"
        assert dataset_event.dataset_location.exists()
        assert dataset_event.dataset_location.suffix == '.parquet'
        
        # Verify time span covers the full day
        assert dataset_event.start_time == base_date
        assert dataset_event.end_time == base_date + timedelta(days=1)
        
        # Verify all 24 events were stored
        stored_events = await mock_event_store.get_events("ingestion_events:test_dataset:")
        assert len(stored_events) == 24
        
        # Verify window events were recorded for the daily window
        window_events = await mock_event_store.get_events("window_events:test_dataset:")
        assert len(window_events) == 1

    @pytest.mark.asyncio
    async def test_single_large_event_creates_multiple_datasets(
        self,
        mock_event_store: MockEventStore,
        mock_timeseries_db: MockTimeSeriesDatabase,
        temp_data_path: Path
    ) -> None:
        """Test that a single large event spanning multiple windows creates multiple datasets."""
        # Create metadata with hourly aggregation and 1-hour STC window
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))
        
        # Create aggregation service
        config = DatasetAggregatorConfig(
            delete_after_export=False,
            data_path=temp_data_path,
            page_size=500
        )
        aggregation_service = DatasetAggregatorService(config, mock_event_store, mock_timeseries_db)
        
        # Create a single large event spanning 3 hours (well outside STC window)
        event_start = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        event_end = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)  # 3 hours span
        event = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=event_start,
            end_time=event_end
        )
        
        # Process the single large event
        datasets: List[DataSetAvailableEvent] = []
        async for dataset_event in await aggregation_service.handle(event):
            datasets.append(dataset_event)
        
        # Should create 3 hourly datasets from the single event
        assert len(datasets) == 3
        
        # Verify each dataset covers the correct hour window
        expected_windows = [
            (datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc), datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)),
            (datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc), datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)),
            (datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc), datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc))
        ]
        
        # Sort datasets by start time for consistent comparison
        datasets.sort(key=lambda d: d.start_time)
        
        for i, (expected_start, expected_end) in enumerate(expected_windows):
            dataset = datasets[i]
            assert isinstance(dataset, DataSetAvailableEvent)
            assert dataset.metadata.name == "test_dataset"
            assert dataset.dataset_location.exists()
            assert dataset.dataset_location.suffix == '.parquet'
            assert dataset.start_time == expected_start
            assert dataset.end_time == expected_end
        
        # Verify the single event was stored once
        stored_events = await mock_event_store.get_events("ingestion_events:test_dataset:")
        assert len(stored_events) == 1
        
        # Verify window events were recorded for all 3 windows
        window_events = await mock_event_store.get_events("window_events:test_dataset:")
        assert len(window_events) == 3

    @pytest.mark.asyncio
    async def test_gap_in_events_skips_window_dataset(
        self,
        mock_event_store: MockEventStore,
        mock_timeseries_db: MockTimeSeriesDatabase,
        temp_data_path: Path
    ) -> None:
        """Test that gaps in event coverage prevent dataset creation for affected windows."""
        # Create metadata with hourly aggregation and 1-hour STC window
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))
        
        # Create aggregation service
        config = DatasetAggregatorConfig(
            delete_after_export=False,
            data_path=temp_data_path,
            page_size=500
        )
        aggregation_service = DatasetAggregatorService(config, mock_event_store, mock_timeseries_db)
        
        # Create events with a gap: 10:00-10:30 and 10:45-11:00 (15 minute gap)
        # This should prevent the 10:00-11:00 window from being processed
        base_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        
        # First event: 10:00-10:30
        event1 = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=base_time,
            end_time=base_time + timedelta(minutes=30)
        )
        
        # Second event: 10:45-11:00 (15 minute gap from 10:30-10:45)
        event2 = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=base_time + timedelta(minutes=45),
            end_time=base_time + timedelta(hours=1)
        )
        
        # Third event: 11:00-12:00 (complete coverage for this window)
        event3 = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=base_time + timedelta(hours=1),
            end_time=base_time + timedelta(hours=2)
        )
        
        # Process all events
        all_datasets: List[DataSetAvailableEvent] = []
        for event in [event1, event2, event3]:
            async for dataset_event in await aggregation_service.handle(event):
                all_datasets.append(dataset_event)
        
        # Should only create 1 dataset for the 11:00-12:00 window (complete coverage)
        # The 10:00-11:00 window should be skipped due to the gap
        assert len(all_datasets) == 1
        dataset_event = all_datasets[0]
        
        # Verify the created dataset is for the complete window (11:00-12:00)
        assert isinstance(dataset_event, DataSetAvailableEvent)
        assert dataset_event.metadata.name == "test_dataset"
        assert dataset_event.dataset_location.exists()
        assert dataset_event.start_time == base_time + timedelta(hours=1)  # 11:00
        assert dataset_event.end_time == base_time + timedelta(hours=2)    # 12:00
        
        # Verify all 3 events were stored
        stored_events = await mock_event_store.get_events("ingestion_events:test_dataset:")
        assert len(stored_events) == 3
        
        # Verify only 1 window event was recorded (for the complete window)
        window_events = await mock_event_store.get_events("window_events:test_dataset:")
        assert len(window_events) == 1

    @pytest.mark.asyncio
    async def test_event_within_stc_window_creates_no_dataset(
        self,
        mock_event_store: MockEventStore,
        mock_timeseries_db: MockTimeSeriesDatabase,
        temp_data_path: Path
    ) -> None:
        """Test that events within subject-to-change window are not processed."""
        # Create metadata with hourly aggregation and 2-hour STC window
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=2))
        
        # Create aggregation service
        config = DatasetAggregatorConfig(
            delete_after_export=False,
            data_path=temp_data_path,
            page_size=500
        )
        aggregation_service = DatasetAggregatorService(config, mock_event_store, mock_timeseries_db)
        
        # Create an event that would normally satisfy a window but is within STC cutoff
        # Using recent time (within 2 hours of "now" when the test runs)
        now = datetime.now(timezone.utc)
        event_start = now - timedelta(minutes=90)  # 1.5 hours ago (within 2-hour STC window)
        event_end = event_start + timedelta(hours=1)  # Complete 1-hour coverage
        
        event = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=event_start,
            end_time=event_end
        )
        
        # Process the event
        datasets: List[DataSetAvailableEvent] = []
        async for dataset_event in await aggregation_service.handle(event):
            datasets.append(dataset_event)
        
        # Should create no datasets due to STC window restriction
        assert len(datasets) == 0
        
        # Verify the event was still stored (for future processing)
        stored_events = await mock_event_store.get_events("ingestion_events:test_dataset:")
        assert len(stored_events) == 1
        
        # Verify no window events were recorded (no windows processed)
        window_events = await mock_event_store.get_events("window_events:test_dataset:")
        assert len(window_events) == 0

    @pytest.mark.asyncio
    async def test_new_event_for_next_window_does_not_reprocess_previous(
        self,
        mock_event_store: MockEventStore,
        mock_timeseries_db: MockTimeSeriesDatabase,
        temp_data_path: Path
    ) -> None:
        """Test that adding an event for a new window doesn't reprocess existing windows."""
        # Create metadata with hourly aggregation and 1-hour STC window
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))
        
        # Create aggregation service
        config = DatasetAggregatorConfig(
            delete_after_export=False,
            data_path=temp_data_path,
            page_size=500
        )
        aggregation_service = DatasetAggregatorService(config, mock_event_store, mock_timeseries_db)
        
        # Step 1: Create and process first event for 10:00-11:00 window
        first_event = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        )
        
        first_datasets: List[DataSetAvailableEvent] = []
        async for dataset_event in await aggregation_service.handle(first_event):
            first_datasets.append(dataset_event)
        
        # Verify first window was processed
        assert len(first_datasets) == 1
        first_dataset = first_datasets[0]
        assert first_dataset.start_time == datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        assert first_dataset.end_time == datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        
        # Record the first dataset file path for comparison
        first_dataset_path = first_dataset.dataset_location
        first_dataset_mtime = first_dataset_path.stat().st_mtime
        
        # Step 2: Create and process second event for 11:00-12:00 window (next window)
        second_event = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        )
        
        second_datasets: List[DataSetAvailableEvent] = []
        async for dataset_event in await aggregation_service.handle(second_event):
            second_datasets.append(dataset_event)
        
        # Verify only the new window was processed
        assert len(second_datasets) == 1
        second_dataset = second_datasets[0]
        assert second_dataset.start_time == datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        assert second_dataset.end_time == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        # Verify the first dataset file was NOT modified (not reprocessed)
        assert first_dataset_path.exists()
        assert first_dataset_path.stat().st_mtime == first_dataset_mtime
        
        # Verify we have 2 different dataset files
        assert first_dataset.dataset_location != second_dataset.dataset_location
        
        # Verify both events were stored
        stored_events = await mock_event_store.get_events("ingestion_events:test_dataset:")
        assert len(stored_events) == 2
        
        # Verify we have window events for both windows
        window_events = await mock_event_store.get_events("window_events:test_dataset:")
        assert len(window_events) == 2

    @pytest.mark.asyncio
    async def test_new_event_overlapping_existing_window_triggers_reprocessing(
        self,
        mock_event_store: MockEventStore,
        mock_timeseries_db: MockTimeSeriesDatabase,
        temp_data_path: Path
    ) -> None:
        """Test that adding an event that overlaps an existing window triggers reprocessing."""
        # Create metadata with hourly aggregation and 1-hour STC window
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))
        
        # Create aggregation service
        config = DatasetAggregatorConfig(
            delete_after_export=False,
            data_path=temp_data_path,
            page_size=500
        )
        aggregation_service = DatasetAggregatorService(config, mock_event_store, mock_timeseries_db)
        
        # Step 1: Create and process first event for 10:00-11:00 window
        first_event = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        )
        
        first_datasets: List[DataSetAvailableEvent] = []
        async for dataset_event in await aggregation_service.handle(first_event):
            first_datasets.append(dataset_event)
        
        # Verify first window was processed
        assert len(first_datasets) == 1
        first_dataset = first_datasets[0]
        assert first_dataset.start_time == datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        assert first_dataset.end_time == datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        
        # Step 2: Create and process second event that overlaps the same window (10:30-11:30)
        # This should trigger reprocessing of the 10:00-11:00 window
        second_event = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=datetime(2024, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        )
        
        second_datasets: List[DataSetAvailableEvent] = []
        async for dataset_event in await aggregation_service.handle(second_event):
            second_datasets.append(dataset_event)
        
        # Should create 2 datasets: reprocessed 10:00-11:00 and new 11:00-12:00
        assert len(second_datasets) == 2
        
        # Sort datasets by start time for consistent comparison
        second_datasets.sort(key=lambda d: d.start_time)
        
        # Verify the first window was reprocessed (new dataset file created)
        reprocessed_dataset = second_datasets[0]
        assert reprocessed_dataset.start_time == datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        assert reprocessed_dataset.end_time == datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        
        # Verify the new window (11:00-12:00) was also processed
        new_dataset = second_datasets[1]
        assert new_dataset.start_time == datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        assert new_dataset.end_time == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        # Verify both events were stored
        stored_events = await mock_event_store.get_events("ingestion_events:test_dataset:")
        assert len(stored_events) == 2
        
        # Verify window events were recorded for both windows
        # The 10:00-11:00 window should have been updated with both event IDs
        window_events = await mock_event_store.get_events("window_events:test_dataset:")
        assert len(window_events) == 2

    @pytest.mark.asyncio
    async def test_event_spanning_partial_windows_only_processes_complete_coverage(
        self,
        mock_event_store: MockEventStore,
        mock_timeseries_db: MockTimeSeriesDatabase,
        temp_data_path: Path
    ) -> None:
        """Test that events spanning partial window boundaries only process windows with complete coverage."""
        # Create metadata with hourly aggregation and 1-hour STC window
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta(hours=1))
        
        # Create aggregation service
        config = DatasetAggregatorConfig(
            delete_after_export=False,
            data_path=temp_data_path,
            page_size=500
        )
        aggregation_service = DatasetAggregatorService(config, mock_event_store, mock_timeseries_db)
        
        # Create an event that spans partial window boundaries: 10:30-11:30
        # This overlaps with two hourly windows:
        # - 10:00-11:00 (partial coverage: only 10:30-11:00)
        # - 11:00-12:00 (partial coverage: only 11:00-11:30)
        # Neither window should be processed due to incomplete coverage
        event = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=datetime(2024, 1, 1, 10, 30, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 11, 30, 0, tzinfo=timezone.utc)
        )
        
        # Process the event
        datasets: List[DataSetAvailableEvent] = []
        async for dataset_event in await aggregation_service.handle(event):
            datasets.append(dataset_event)
        
        # Should create no datasets due to incomplete window coverage
        assert len(datasets) == 0
        
        # Verify the event was still stored (for potential future processing with other events)
        stored_events = await mock_event_store.get_events("ingestion_events:test_dataset:")
        assert len(stored_events) == 1
        
        # Verify no window events were recorded (no windows processed)
        window_events = await mock_event_store.get_events("window_events:test_dataset:")
        assert len(window_events) == 0
        
        # Now add complementary events to complete the coverage for both windows
        # Event to complete 10:00-11:00 window coverage
        complementary_event1 = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 10, 30, 0, tzinfo=timezone.utc)
        )
        
        # Event to complete 11:00-12:00 window coverage
        complementary_event2 = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=datetime(2024, 1, 1, 11, 30, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        )
        
        # Process complementary events
        for comp_event in [complementary_event1, complementary_event2]:
            async for dataset_event in await aggregation_service.handle(comp_event):
                datasets.append(dataset_event)
        
        # Now should have 2 datasets (both windows have complete coverage)
        assert len(datasets) == 2
        
        # Sort datasets by start time for consistent comparison
        datasets.sort(key=lambda d: d.start_time)
        
        # Verify both windows were processed with complete coverage
        first_dataset = datasets[0]
        assert first_dataset.start_time == datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        assert first_dataset.end_time == datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        
        second_dataset = datasets[1]
        assert second_dataset.start_time == datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        assert second_dataset.end_time == datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        # Verify all 3 events were stored
        final_stored_events = await mock_event_store.get_events("ingestion_events:test_dataset:")
        assert len(final_stored_events) == 3
        
        # Verify window events were recorded for both processed windows
        final_window_events = await mock_event_store.get_events("window_events:test_dataset:")
        assert len(final_window_events) == 2

    @pytest.mark.asyncio
    async def test_backfill_allows_dataset_creation_for_complete_window(
        self,
        mock_event_store: MockEventStore,
        mock_timeseries_db: MockTimeSeriesDatabase,
        tmp_path,
    ) -> None:
        """
        Ensure that after a later backfill event fully covers the 10:00–11:00 window,
        the aggregator produces the dataset for that window.

        Scenario:
        - Partial events first:
            B: 10:09–10:30:0
            C: 10:30:0–11:00:0
        - Backfill arrives later:
            A: 10:00–10:20 (fully covers the window)

        Expected:
        - Exactly one dataset event for [10:00, 11:00] is emitted.
        """
        # Hourly aggregation, no STC window (so the hour can be finalized immediately)
        metadata = _create_metadata(aggregation_span=timedelta(hours=1), stc_window=timedelta())

        config = DatasetAggregatorConfig(
            delete_after_export=False,
            data_path=tmp_path,
            page_size=500,
        )
        aggregation_service = DatasetAggregatorService(config, mock_event_store, mock_timeseries_db)

        base = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

        # Partial short events that appear to leave a small gap if you only compare to the previous end
        event_b = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=base + timedelta(minutes=9),     # 10:09
            end_time=base + timedelta(minutes=30),      # 10:30
        )
        event_c = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=base + timedelta(minutes=30, seconds=0),    # 10:30:00
            end_time=base + timedelta(hours=1),      # 11:00:00
        )

        # Later backfill that fully covers the window 10:00–11:00
        event_a_full = _create_data_available_event(
            event_id=uuid4(),
            metadata=metadata,
            start_time=base,                                       # 10:00
            end_time=base + timedelta(minutes=20),                 # 10:20
        )

        # Process in order simulating late backfill (B, C first; A last)
        all_datasets: List[DataSetAvailableEvent] = []
        for ev in [event_b, event_c, event_a_full]:
            async for dataset_event in await aggregation_service.handle(ev):
                all_datasets.append(dataset_event)

        assert len(all_datasets) == 1, f"Expected 1 dataset, got {len(all_datasets)}"
