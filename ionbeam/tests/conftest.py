"""Common test fixtures and utilities for ionbeam core tests."""

from datetime import datetime, timedelta
from typing import AsyncIterator, Dict, List, Optional, Tuple

import pandas as pd
import pytest

from ionbeam.observability import (
    MockIngestionMetrics,
    MockCoordinatorMetrics,
    MockBuilderMetrics,
)
from ionbeam.observability.protocols import (
    IngestionMetricsProtocol,
    CoordinatorMetricsProtocol,
    BuilderMetricsProtocol,
)
from ionbeam.models.service_models import IngestionRecord, Window, WindowBuildState
from ionbeam.storage.ingestion_record_store import IngestionRecordStore
from ionbeam.storage.ordered_queue import OrderedQueue
from ionbeam.storage.timeseries import TimeSeriesDatabase


class MockIngestionRecordStore(IngestionRecordStore):
    """In-memory mock IngestionRecordStore for testing."""

    def __init__(self) -> None:
        self._records: Dict[str, str] = {}
        self._desired_events: Dict[str, List[str]] = {}
        self._window_states: Dict[str, WindowBuildState] = {}

    def _ingestion_record_key(self, dataset: str, event_id: str) -> str:
        return f"ingestion_events:{dataset}:{event_id}"

    def _desired_events_key(self, window: Window) -> str:
        return f"{window.dataset_key}:event_ids"

    def _window_state_key(self, window: Window) -> str:
        return f"{window.dataset_key}:state"

    async def save_ingestion_record(self, record: IngestionRecord) -> None:
        key = self._ingestion_record_key(record.metadata.dataset.name, str(record.id))
        self._records[key] = record.model_dump_json()

    async def get_ingestion_records(self, dataset: str) -> List[IngestionRecord]:
        pattern = f"ingestion_events:{dataset}:"
        records = []
        for key, value in self._records.items():
            if key.startswith(pattern):
                records.append(IngestionRecord.model_validate_json(value))
        return records

    async def get_desired_event_ids(self, window: Window) -> List[str]:
        key = self._desired_events_key(window)
        return self._desired_events.get(key, [])

    async def set_desired_event_ids(self, window: Window, event_ids: List[str]) -> None:
        key = self._desired_events_key(window)
        self._desired_events[key] = sorted(event_ids)

    async def get_window_state(self, window: Window) -> Optional[WindowBuildState]:
        key = self._window_state_key(window)
        return self._window_states.get(key)

    async def set_window_state(self, window: Window, state: WindowBuildState) -> None:
        key = self._window_state_key(window)
        self._window_states[key] = state


class MockOrderedQueue(OrderedQueue):
    """In-memory mock OrderedQueue for testing."""

    def __init__(self) -> None:
        self._queue: Dict[str, int] = {}

    async def enqueue(self, window: Window, priority: int) -> None:
        self._queue[window.dataset_key] = priority

    async def dequeue_highest_priority(self) -> Optional[Tuple[Window, int]]:
        if not self._queue:
            return None

        # Find highest priority (max score)
        dataset_key = max(self._queue.keys(), key=lambda k: self._queue[k])
        priority = self._queue.pop(dataset_key)
        window = Window.from_dataset_key(dataset_key)
        return window, priority

    async def get_size(self) -> int:
        return len(self._queue)

    def get_queue_dict(self) -> Dict[str, int]:
        """Helper for tests to inspect queue state."""
        return self._queue.copy()


class MockTimeSeriesDatabase(TimeSeriesDatabase):
    """Mock timeseries database for ionbeam testing."""

    def __init__(self) -> None:
        self.write_calls: List[dict] = []
        self.delete_calls: List[tuple[str, datetime, datetime]] = []

    async def query_measurement_data(
        self,
        measurement: str,
        start_time: datetime,
        end_time: datetime,
        slice_duration: Optional[timedelta] = None,
    ) -> AsyncIterator[pd.DataFrame]:
        from ionbeam_client.constants import LatitudeColumn, LongitudeColumn

        time_range = pd.date_range(start_time, end_time, freq="1min", tz="UTC")[:-1]

        records = []
        for i, timestamp in enumerate(time_range):
            records.append(
                {
                    "_time": timestamp,
                    "_measurement": measurement,
                    "_field": "air_temperature__deg_C______",
                    "_value": 20.0 + i * 0.1,
                    "station_id": "test_station",
                }
            )
            records.append(
                {
                    "_time": timestamp,
                    "_measurement": measurement,
                    "_field": LatitudeColumn,
                    "_value": 52.5,
                    "station_id": "test_station",
                }
            )
            records.append(
                {
                    "_time": timestamp,
                    "_measurement": measurement,
                    "_field": LongitudeColumn,
                    "_value": 13.4,
                    "station_id": "test_station",
                }
            )

        df = pd.DataFrame(records)
        yield df

    async def write_dataframe(
        self,
        record: pd.DataFrame,
        measurement_name: str,
        tag_columns: List[str],
        timestamp_column: str,
    ) -> None:
        self.write_calls.append(
            {
                "record": record.copy(),
                "measurement_name": measurement_name,
                "tag_columns": tag_columns,
                "timestamp_column": timestamp_column,
            }
        )

    async def delete_measurement_data(
        self, measurement: str, start_time: datetime, end_time: datetime
    ) -> None:
        self.delete_calls.append((measurement, start_time, end_time))


class FailingTimeSeriesDatabase(TimeSeriesDatabase):
    """Mock timeseries database that always fails for testing error handling."""

    async def query_measurement_data(
        self,
        measurement: str,
        start_time: datetime,
        end_time: datetime,
        slice_duration: Optional[timedelta] = None,
    ) -> AsyncIterator[pd.DataFrame]:
        raise Exception("Simulated database failure")
        yield

    async def write_dataframe(
        self,
        record: pd.DataFrame,
        measurement_name: str,
        tag_columns: List[str],
        timestamp_column: str,
    ) -> None:
        raise Exception("Simulated database failure")

    async def delete_measurement_data(
        self, measurement: str, start_time: datetime, end_time: datetime
    ) -> None:
        raise Exception("Simulated database failure")


@pytest.fixture
def mock_ingestion_metrics() -> IngestionMetricsProtocol:
    return MockIngestionMetrics()


@pytest.fixture
def mock_coordinator_metrics() -> CoordinatorMetricsProtocol:
    return MockCoordinatorMetrics()


@pytest.fixture
def mock_builder_metrics() -> BuilderMetricsProtocol:
    return MockBuilderMetrics()


@pytest.fixture
def mock_ingestion_record_store() -> MockIngestionRecordStore:
    return MockIngestionRecordStore()


@pytest.fixture
def mock_ordered_queue() -> MockOrderedQueue:
    return MockOrderedQueue()


@pytest.fixture
def mock_timeseries_db() -> MockTimeSeriesDatabase:
    return MockTimeSeriesDatabase()


@pytest.fixture
def failing_timeseries_db() -> FailingTimeSeriesDatabase:
    return FailingTimeSeriesDatabase()
