"""Common test fixtures and utilities."""

from typing import List, Optional

import pytest

from ionbeam.observability.metrics import IonbeamMetricsProtocol
from ionbeam.storage.event_store import EventStore


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


class MockEventStore(EventStore):
    """In-memory mock EventStore with simple prefix pattern matching."""

    def __init__(self) -> None:
        self.events: dict[str, str] = {}

    async def get_event(self, key: str) -> Optional[str]:
        return self.events.get(key)

    async def store_event(self, key: str, event_json: str, ttl: Optional[int] = None) -> None:
        self.events[key] = event_json

    async def get_events(self, pattern: str) -> List[str]:
        prefix = pattern.replace("*", "")
        return [v for k, v in self.events.items() if k.startswith(prefix)]


@pytest.fixture
def mock_metrics() -> IonbeamMetricsProtocol:
    """Provide a mock metrics implementation for tests."""
    return MockIonbeamMetrics()


@pytest.fixture
def mock_event_store() -> EventStore:
    """Provide a mock event store for tests."""
    return MockEventStore()
