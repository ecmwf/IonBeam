"""Common test fixtures and utilities."""

from typing import AsyncIterator, Awaitable, Callable, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pytest

from ionbeam.observability.metrics import IonbeamMetricsProtocol
from ionbeam.services.models import IngestionRecord, Window, WindowBuildState
from ionbeam.storage.arrow_store import ArrowStore
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


class MockArrowStore(ArrowStore):
    """In-memory mock Arrow store for testing dataset builds."""
    
    def __init__(self) -> None:
        self._storage: Dict[str, List[pa.RecordBatch]] = {}
        self._schemas: Dict[str, Optional[pa.Schema]] = {}
    
    async def write_record_batches(
        self,
        key: str,
        batch_stream: AsyncIterator[pa.RecordBatch],
        schema: Optional[pa.Schema] = None,
        overwrite: bool = False,
    ) -> int:
        if key in self._storage and not overwrite:
            raise FileExistsError(f"Object already exists for key '{key}'")
        
        if overwrite:
            self._storage.pop(key, None)
            self._schemas.pop(key, None)
        
        batches: List[pa.RecordBatch] = []
        total_rows = 0
        actual_schema = schema
        async for batch in batch_stream:
            batches.append(batch)
            total_rows += batch.num_rows
            if actual_schema is None:
                actual_schema = batch.schema
        
        self._storage[key] = batches
        self._schemas[key] = actual_schema
        return total_rows
    
    def read_record_batches(
        self,
        key: str,
        batch_size: Optional[int] = None,
    ) -> AsyncIterator[pa.RecordBatch]:
        async def _generator():
            for batch in self._storage.get(key, []):
                yield batch
        return _generator()
    
    async def delete(self, key: str) -> None:
        self._storage.pop(key, None)
        self._schemas.pop(key, None)
    
    async def exists(self, key: str) -> bool:
        return key in self._storage
    
    def list_keys(self) -> List[str]:
        return list(self._storage.keys())
    
    def get_batches(self, key: str) -> List[pa.RecordBatch]:
        return self._storage.get(key, [])
    
    def get_total_rows(self, key: str) -> int:
        return sum(batch.num_rows for batch in self._storage.get(key, []))
    
    def clear(self) -> None:
        self._storage.clear()
        self._schemas.clear()


@pytest.fixture
def arrow_store_writer(
    mock_arrow_store: MockArrowStore,
) -> Callable[[str, pd.DataFrame, Optional[pa.Schema], bool], Awaitable[int]]:
    async def _writer(
        key: str,
        df: pd.DataFrame,
        schema: Optional[pa.Schema] = None,
        overwrite: bool = False,
    ) -> int:
        if schema is None:
            schema = pa.Table.from_pandas(df, preserve_index=False).schema
        
        async def stream():
            yield pa.RecordBatch.from_pandas(df, schema=schema, preserve_index=False)
        
        return await mock_arrow_store.write_record_batches(
            key,
            stream(),
            schema=schema,
            overwrite=overwrite,
        )
    
    return _writer


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


@pytest.fixture
def mock_arrow_store() -> MockArrowStore:
    """Provide a mock Arrow store for tests."""
    return MockArrowStore()
