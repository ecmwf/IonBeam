"""Common test fixtures and utilities."""

from typing import Dict, List, Optional, Tuple

import pytest

from ionbeam.observability.metrics import IonbeamMetricsProtocol
from ionbeam.services.models import IngestionRecord, Window, WindowBuildState
from ionbeam.storage.ingestion_record_store import IngestionRecordStore
from ionbeam.storage.ordered_queue import OrderedQueue


class MockHandlerMetrics:
    """Mock implementation of HandlerMetricsProtocol for testing."""
    
    def record_run(self, handler: str, status: str = "success") -> None:
        pass
    
    def observe_duration(self, handler: str, seconds: float) -> None:
        pass


class MockSourceMetrics:
    """Mock implementation of SourceMetricsProtocol for testing."""
    
    def record_ingestion_request(self, source: str, result: str) -> None:
        pass
    
    def observe_request_rows(self, source: str, count: int) -> None:
        pass
    
    def observe_fetch_duration(self, source: str, seconds: float) -> None:
        pass
    
    def observe_data_lag(self, source: str, seconds: float) -> None:
        pass


class MockIngestionMetrics:
    """Mock implementation of IngestionMetricsProtocol for testing."""
    
    def observe_dataset_points(self, dataset: str, points: int) -> None:
        pass
    
    def observe_dataset_duration(self, dataset: str, seconds: float) -> None:
        pass


class MockCoordinatorMetrics:
    """Mock implementation of CoordinatorMetricsProtocol for testing."""
    
    def window_skipped(self, dataset: str, reason: str) -> None:
        pass
    
    def window_enqueued(self, dataset: str) -> None:
        pass
    
    def set_queue_size(self, dataset: str, size: int) -> None:
        pass


class MockBuilderMetrics:
    """Mock implementation of BuilderMetricsProtocol for testing."""
    
    def build_started(self, dataset: str) -> None:
        pass
    
    def build_succeeded(self, dataset: str) -> None:
        pass
    
    def build_failed(self, dataset: str) -> None:
        pass
    
    def requeued(self, dataset: str, reason: str) -> None:
        pass
    
    def publish(self, dataset: str, result: str) -> None:
        pass
    
    def observe_build_duration(self, dataset: str, seconds: float) -> None:
        pass
    
    def observe_rows_exported(self, dataset: str, rows: int) -> None:
        pass


class MockIonbeamMetrics:
    """Mock implementation of IonbeamMetricsProtocol for testing."""
    
    def __init__(self) -> None:
        self.handlers = MockHandlerMetrics()
        self.sources = MockSourceMetrics()
        self.ingestion = MockIngestionMetrics()
        self.coordinator = MockCoordinatorMetrics()
        self.builders = MockBuilderMetrics()


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


@pytest.fixture
def mock_metrics() -> IonbeamMetricsProtocol:
    """Provide a mock metrics implementation for tests."""
    return MockIonbeamMetrics()


@pytest.fixture
def mock_ingestion_record_store() -> MockIngestionRecordStore:
    """Provide a mock ingestion record store for tests."""
    return MockIngestionRecordStore()


@pytest.fixture
def mock_ordered_queue() -> MockOrderedQueue:
    """Provide a mock ordered queue for tests."""
    return MockOrderedQueue()
