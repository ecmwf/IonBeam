"""
Dataset Coordinator

Listens to DataAvailableEvent, maintains per-window desired state (event sets),
detects drift vs. observed state (last built), and enqueues windows that need
building into a prioritized queue.
"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional

import structlog
from pydantic import BaseModel

from ..core.handler import BaseHandler
from ..models.models import DataAvailableEvent
from ..observability.metrics import IonbeamMetricsProtocol
from ..storage.ingestion_record_store import IngestionRecordStore
from ..storage.ordered_queue import OrderedQueue
from .models import CoverageAnalysis, EventSet, IngestionRecord, Window


def align_to_aggregation(ts: datetime, aggregation: timedelta) -> datetime:
    """Align timestamp to aggregation boundary (UTC)."""
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = ts - epoch
    aligned_seconds = (delta.total_seconds() // aggregation.total_seconds()) * aggregation.total_seconds()
    return epoch + timedelta(seconds=aligned_seconds)


class WindowStateManager:
    """Manages window state and event associations."""
    
    def __init__(self, record_store: IngestionRecordStore, queue: OrderedQueue):
        self.record_store = record_store
        self.queue = queue
        self.logger = structlog.get_logger(__name__)
    
    async def save_ingestion_record(self, record: IngestionRecord) -> None:
        await self.record_store.save_ingestion_record(record)
    
    async def get_ingestion_records(self, dataset: str) -> List[IngestionRecord]:
        return await self.record_store.get_ingestion_records(dataset)
    
    def analyze_coverage(self, events: List[IngestionRecord]) -> CoverageAnalysis:
        """Analyze event coverage and find gaps."""
        if not events:
            return CoverageAnalysis([], None, None, [])
        
        sorted_events = sorted(events, key=lambda e: (e.start_time, e.end_time))
        overall_start = sorted_events[0].start_time
        overall_end = max(e.end_time for e in sorted_events)
        
        gaps = []
        coverage_end = sorted_events[0].end_time
        min_gap = timedelta(seconds=1)
        
        for event in sorted_events[1:]:
            if event.start_time > coverage_end:
                gap_duration = event.start_time - coverage_end
                if gap_duration > min_gap:
                    gaps.append((coverage_end, event.start_time))
                    self.logger.warning(
                        "Data gap detected",
                        gap_start=coverage_end.isoformat(),
                        gap_end=event.start_time.isoformat(),
                        duration=str(gap_duration)
                    )
            coverage_end = max(coverage_end, event.end_time)
        
        return CoverageAnalysis(sorted_events, overall_start, overall_end, gaps)
    
    async def get_desired_events(self, window: Window) -> EventSet:
        event_ids = await self.record_store.get_desired_event_ids(window)
        return EventSet.from_list(event_ids)
    
    async def set_desired_events(self, window: Window, events: EventSet) -> None:
        await self.record_store.set_desired_event_ids(window, sorted(events.ids))
    
    async def get_observed_hash(self, window: Window) -> Optional[str]:
        state = await self.record_store.get_window_state(window)
        return state.event_ids_hash if state else None
    
    async def set_observed_hash(self, window: Window, event_hash: str) -> None:
        from ..services.models import WindowBuildState
        state = WindowBuildState(
            event_ids_hash=event_hash,
            timestamp=datetime.now(timezone.utc)
        )
        await self.record_store.set_window_state(window, state)
    
    async def enqueue_window(self, window: Window, priority: int) -> None:
        await self.queue.enqueue(window, priority)
    
    async def get_queue_size(self) -> int:
        return await self.queue.get_size()


class DatasetCoordinatorConfig(BaseModel):
    only_process_spanned_windows: bool = True
    queue_key: str = "dataset_queue"


class DatasetCoordinatorService(BaseHandler[DataAvailableEvent, None]):
    """Coordinates window readiness detection and build enqueueing."""
    
    def __init__(
        self,
        config: DatasetCoordinatorConfig,
        record_store: IngestionRecordStore,
        queue: OrderedQueue,
        metrics: IonbeamMetricsProtocol,
    ):
        super().__init__("DatasetCoordinatorService", metrics)
        self.config = config
        self.state = WindowStateManager(record_store, queue)
    
    async def _handle(self, event: DataAvailableEvent) -> None:
        dataset = event.metadata.dataset.name
        
        record = IngestionRecord(
            id=event.id,
            metadata=event.metadata,
            start_time=event.start_time,
            end_time=event.end_time,
        )
        await self.state.save_ingestion_record(record)
        
        all_records = await self.state.get_ingestion_records(dataset)
        coverage = self.state.analyze_coverage(all_records)
        
        if not coverage.overall_start:
            return
        
        windows = self._generate_windows(event, coverage)
        
        now = datetime.now(timezone.utc)
        stc_cutoff = now - event.metadata.dataset.subject_to_change_window
        
        for window in windows:
            await self._process_window(window, coverage, stc_cutoff)
    
    async def _process_window(
        self,
        window: Window,
        coverage: CoverageAnalysis,
        stc_cutoff: datetime
    ) -> None:
        overlapping = coverage.events_in_window(window)
        new_events = EventSet.from_records(overlapping)
        existing = await self.state.get_desired_events(window)
        desired = existing.union(new_events)
        await self.state.set_desired_events(window, desired)
        
        skip_reason = self._check_readiness(window, coverage, stc_cutoff)
        if skip_reason:
            self.metrics.coordinator.window_skipped(window.dataset, skip_reason)
            return
        
        observed_hash = await self.state.get_observed_hash(window)
        if observed_hash == desired.hash:
            return
        
        priority = -int(window.end.timestamp()) # TODO - implement proper utility function; at the moment this processes oldest first
        
        await self.state.enqueue_window(window, priority)
        self.metrics.coordinator.window_enqueued(window.dataset)
        
        self.logger.info(
            "Enqueued window for build",
            dataset=window.dataset,
            window_start=window.start.isoformat(),
            window_end=window.end.isoformat(),
            desired_hash=desired.hash[:8],
            observed_hash=(observed_hash[:8] if observed_hash else None),
        )
    
    def _check_readiness(
        self,
        window: Window,
        coverage: CoverageAnalysis,
        stc_cutoff: datetime
    ) -> Optional[str]:
        """Returns skip reason if not ready, None if ready."""
        if window.end >= stc_cutoff:
            return "stc_cutoff"
        
        if coverage.has_gap_in_window(window):
            return "gap"
        
        if not coverage.events_in_window(window):
            return "no_events"
        
        if not coverage.fully_covers(window):
            return "incomplete_coverage"
        
        return None
    
    def _generate_windows(
        self,
        event: DataAvailableEvent,
        coverage: CoverageAnalysis
    ) -> List[Window]:
        """Generate windows to check based on configuration."""
        aggregation = event.metadata.dataset.aggregation_span
        dataset = event.metadata.dataset.name
        
        if self.config.only_process_spanned_windows:
            start = align_to_aggregation(event.start_time, aggregation)
            end = align_to_aggregation(event.end_time, aggregation)
            if end < event.end_time:
                end += aggregation
        else:
            start = align_to_aggregation(coverage.overall_start, aggregation)
            end = coverage.overall_end
        
        windows = []
        current = start
        while current < end:
            windows.append(Window(dataset, current, aggregation))
            current += aggregation
        
        return windows
