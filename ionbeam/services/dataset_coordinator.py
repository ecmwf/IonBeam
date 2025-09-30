"""
Dataset Coordinator

Listens to DataAvailableEvent, maintains per-window desired state (event sets),
detects drift vs. observed state (last built), and enqueues windows that need
building into a prioritized queue. Builders will consume from this queue.

Key semantics (stored via EventStore):
- ingestion_events:{dataset}:{event_id}  -> IngestionRecord (JSON)
- {dataset}:{window_id}:event_ids        -> JSON list of contributing event IDs (desired state)
- {dataset}:{window_id}:state            -> JSON object of observed state written by builders
                                           e.g. {"event_ids_hash": "...", "version": 1, "timestamp": "..."}
- dataset_queue                          -> JSON object mapping "{dataset}:{window_id}" -> score
                                           (score is chosen so that older windows have higher priority with ZPOPMAX)

This module does not perform any data building; it only coordinates priority.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from isodate import duration_isoformat
from pydantic import BaseModel

from ionbeam.observability.metrics import IonbeamMetricsProtocol

from ..core.constants import MaximumCachePeriod
from ..core.handler import BaseHandler
from ..models.models import DataAvailableEvent
from ..services.models import IngestionRecord, WindowBuildState
from ..storage.event_store import EventStore


@dataclass
class EventAggregateAnalysis:
    events: List[IngestionRecord]
    overall_start: Optional[datetime]
    overall_end: Optional[datetime]
    gaps: List[Tuple[datetime, datetime]]


class DatasetCoordinatorConfig(BaseModel):
    # Windows are only considered for the span covered by the triggering event, not whole history
    only_process_spanned_windows: bool = True # this allows TTC to work
    # Key for the global build queue (modeled as a JSON mapping)
    queue_key: str = "dataset_queue"


class DatasetCoordinatorService(BaseHandler[DataAvailableEvent, None]):
    """
    Compute window readiness and enqueue build work with priority
    """
    def __init__(
        self,
        config: DatasetCoordinatorConfig,
        event_store: EventStore,
        metrics: IonbeamMetricsProtocol,
    ):
        super().__init__("DatasetCoordinatorService", metrics)
        self.config = config
        self.event_store = event_store

    def _align_to_aggregation(self, ts: datetime, aggregation: timedelta) -> datetime:
        """Align timestamp to aggregation boundary (UTC)."""
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        delta = ts - epoch
        aligned_seconds = (delta.total_seconds() // aggregation.total_seconds()) * aggregation.total_seconds()
        return epoch + timedelta(seconds=aligned_seconds)

    def _get_window_id(self, window_start: datetime, aggregation: timedelta) -> str:
        """Generate consistent window identifier."""
        return f"{window_start.isoformat()}_{duration_isoformat(aggregation)}"

    async def _load_and_parse_data_events(self, dataset_name: str) -> EventAggregateAnalysis:
        """
        Load events from the event store and calculate overall span and gaps.
        Events may arrive out of order and can backfill older periods.
        """
        event_data_list = await self.event_store.get_events(f"ingestion_events:{dataset_name}:*")

        if not event_data_list:
            self.logger.info("No previous events found", dataset=dataset_name)
            return EventAggregateAnalysis(events=[], overall_start=None, overall_end=None, gaps=[])

        events: List[IngestionRecord] = [
            IngestionRecord.model_validate_json(event_data) for event_data in event_data_list
        ]
        if not events:
            self.logger.info("No valid events found", dataset=dataset_name)
            return EventAggregateAnalysis(events=[], overall_start=None, overall_end=None, gaps=[])

        # Sort for stable coverage computation
        events.sort(key=lambda e: (e.start_time, e.end_time))

        overall_start = events[0].start_time
        overall_end = max(e.end_time for e in events)

        gaps: List[Tuple[datetime, datetime]] = []
        min_gap_threshold = timedelta(seconds=1)  # ignore sub-second timing noise
        coverage_end = events[0].end_time

        for e in events[1:]:
            if e.start_time > coverage_end:
                gap_duration = e.start_time - coverage_end
                if gap_duration > min_gap_threshold:
                    gaps.append((coverage_end, e.start_time))
                    self.logger.warning(
                        "Data gap detected",
                        dataset=dataset_name,
                        gap_start=coverage_end.isoformat(),
                        gap_end=e.start_time.isoformat(),
                        duration=str(gap_duration),
                    )
                coverage_end = e.end_time
            else:
                if e.end_time > coverage_end:
                    coverage_end = e.end_time

        return EventAggregateAnalysis(
            events=events,
            overall_start=overall_start,
            overall_end=overall_end,
            gaps=gaps,
        )

    def _should_process_window(
        self,
        window_start: datetime,
        window_end: datetime,
        stc_cutoff: datetime,
        gaps: List[Tuple[datetime, datetime]],
        events: List[IngestionRecord],
        dataset_name: str,
    ) -> bool:
        """
        True if window is before STC cutoff, has no gaps, and has full coverage by events.
        """
        if window_end >= stc_cutoff:
            self.logger.info(
                "Skipping window due to subject-to-change cutoff",
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
                stc_cutoff=stc_cutoff.isoformat(),
            )
            self.metrics.coordinator.window_skipped(dataset_name, "stc_cutoff")
            return False

        # Any gap overlapping window -> skip
        if any(gap_start < window_end and gap_end > window_start for gap_start, gap_end in gaps):
            self.logger.warning(
                "Skipping window due to gaps",
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
            )
            self.metrics.coordinator.window_skipped(dataset_name, "gap")
            return False

        # Events overlapping window
        events_in_window = [e for e in events if e.start_time < window_end and e.end_time > window_start]
        if not events_in_window:
            self.logger.info(
                "Skipping window - no overlapping events",
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
            )
            self.metrics.coordinator.window_skipped(dataset_name, "no_events")
            return False

        earliest_start = min(e.start_time for e in events_in_window)
        latest_end = max(e.end_time for e in events_in_window)
        if earliest_start > window_start or latest_end < window_end:
            self.logger.info(
                "Skipping window - incomplete coverage",
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
                data_start=earliest_start.isoformat(),
                data_end=latest_end.isoformat(),
            )
            self.metrics.coordinator.window_skipped(dataset_name, "incomplete_coverage")
            return False

        return True

    def _compute_event_set_hash(self, event_ids: Iterable[str]) -> str:
        """Deterministic hash of a set of event IDs (sorted)."""
        joined = ",".join(sorted(event_ids))
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    async def _get_window_event_ids(self, dataset_name: str, window_id: str) -> List[str]:
        key = f"{dataset_name}:{window_id}:event_ids"
        data = await self.event_store.get_event(key)
        if not data:
            return []
        try:
            ids = json.loads(data)
            return [str(i) for i in ids]
        except Exception:
            self.logger.exception("Failed to parse window event_ids", key=key)
            return []

    async def _put_window_event_ids(self, dataset_name: str, window_id: str, event_ids: List[str]) -> None:
        key = f"{dataset_name}:{window_id}:event_ids"
        # De-dup and sort for stable storage
        unique_sorted = sorted(set(event_ids))
        await self.event_store.store_event(
            key,
            json.dumps(unique_sorted),
            ttl=int(MaximumCachePeriod.total_seconds()),
        )

    async def _get_window_state_hash(self, dataset_name: str, window_id: str) -> Optional[str]:
        """Read observed state that builders set after a successful build."""
        key = f"{dataset_name}:{window_id}:state"
        data = await self.event_store.get_event(key)
        if not data:
            return None
        try:
            state = WindowBuildState.model_validate_json(data)
            return state.event_ids_hash
        except Exception:
            self.logger.exception("Failed to parse window state", key=key)
            return None

    async def _enqueue_window(self, dataset_key: str, score: int) -> None:
        """
        Insert/update a queue dataset key with a score.
        TODO uise Redis ZSET.
        """
        data = await self.event_store.get_event(self.config.queue_key)
        try:
            queue: Dict[str, int] = json.loads(data) if data else {}
        except Exception:
            self.logger.exception("Failed to parse queue mapping, recreating", key=self.config.queue_key)
            queue = {}

        # Idempotent upsert
        existing = queue.get(dataset_key)
        if existing is None or existing != score:
            queue[dataset_key] = score
            await self.event_store.store_event(self.config.queue_key, json.dumps(queue))

        dataset = dataset_key.split(":", 1)[0]
        self.metrics.coordinator.set_queue_size(dataset, len(queue))

    async def _handle(self, event: DataAvailableEvent) -> None:
        metadata = event.metadata
        dataset_name = metadata.dataset.name

        # Persist the ingestion event for tracking/coverage analysis
        ingest_key = f"ingestion_events:{dataset_name}:{event.id}"
        ingestion_record = IngestionRecord(
            id=event.id,
            metadata=metadata,
            start_time=event.start_time,
            end_time=event.end_time,
        )
        await self.event_store.store_event(
            ingest_key,
            ingestion_record.model_dump_json(),
            ttl=int(MaximumCachePeriod.total_seconds()),
        )

        # Load all events and analyze coverage/gaps
        analysis = await self._load_and_parse_data_events(dataset_name)
        if not analysis.events or analysis.overall_start is None or analysis.overall_end is None:
            return None

        # Subject-to-change cutoff
        now = datetime.now(timezone.utc)
        stc_cutoff = now - metadata.dataset.subject_to_change_window
        aggregation_span = metadata.dataset.aggregation_span

        # Determine iteration bounds
        if self.config.only_process_spanned_windows:
            start_time = self._align_to_aggregation(event.start_time, aggregation_span)
            iter_end = self._align_to_aggregation(event.end_time, aggregation_span)
            if iter_end < event.end_time:
                iter_end = iter_end + aggregation_span
            iteration_end = iter_end
        else:
            start_time = self._align_to_aggregation(analysis.overall_start, aggregation_span)
            iteration_end = analysis.overall_end

        window_start = start_time
        while window_start < iteration_end:
            window_end = window_start + aggregation_span
            window_id = self._get_window_id(window_start, aggregation_span)

            # Maintain desired state: ensure window's event_ids reflects all overlapping events
            overlapping_events = [
                e for e in analysis.events if e.start_time < window_end and e.end_time > window_start
            ]
            existing_ids = await self._get_window_event_ids(dataset_name, window_id)
            updated_ids = set(existing_ids)
            updated_ids.update(str(e.id) for e in overlapping_events)
            await self._put_window_event_ids(dataset_name, window_id, list(updated_ids))

            # Decide readiness based on STC, gaps and coverage
            if not self._should_process_window(
                window_start,
                window_end,
                stc_cutoff,
                analysis.gaps,
                analysis.events,
                dataset_name,
            ):
                window_start = window_end
                continue

            # Compute desired spec hash and compare with observed state
            desired_ids = await self._get_window_event_ids(dataset_name, window_id)
            desired_hash = self._compute_event_set_hash(desired_ids)
            observed_hash = await self._get_window_state_hash(dataset_name, window_id)

            if observed_hash != desired_hash:
                # Enqueue with priority: older windows first using negative epoch seconds and ZPOPMAX
                dataset_key = f"{dataset_name}:{window_id}"
                score = -int(window_end.timestamp())
                await self._enqueue_window(dataset_key, score)
                self.metrics.coordinator.window_enqueued(dataset_name)
                self.logger.info(
                    "Enqueued window for build",
                    dataset=dataset_name,
                    window_id=window_id,
                    window_start=window_start.isoformat(),
                    window_end=window_end.isoformat(),
                    desired_hash=desired_hash,
                    observed_hash=observed_hash,
                    score=score,
                )

            window_start = window_end

        return None
