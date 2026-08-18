# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""When to build which window.

A coverage claim answers "was this range checked"; its records answer "which
windows got rows". Each claim stores itself for gap analysis, folds each of
its records into that window's desired set, then runs one pure decision
(:func:`decide`) per spanned window. A window whose data supports a build is
scheduled for the moment it becomes worth building — past its
measured-lateness settle time and its rebuild debounce. The queue holds
windows whose moment has not yet come."""

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import structlog
from ionbeam_client.models import DataAvailableEvent
from pydantic import BaseModel
from structlog.contextvars import bound_contextvars

from ionbeam.datasets import DatasetRegistry
from ionbeam.provenance import (
    CoverageAnalysis,
    CoverageClaim,
    RecordSet,
    IngestionRecord,
    Window,
    WindowBuildState,
    align_to_aggregation,
)
from ionbeam.observability import CoordinatorMetrics
from ionbeam.storage.build_queue import BuildQueue
from ionbeam.storage.coordination_store import CoordinationStore


class DatasetCoordinatorConfig(BaseModel):
    # The settle delay is this percentile of a source's observed arrival
    # lateness, giving each source its own threshold for when a window is
    # complete enough for a first build. min_samples guards against trusting
    # a cold histogram: below the threshold, the build proceeds immediately
    # and is revisable.
    settle_percentile: float = 0.95
    settle_min_samples: int = 50
    # The hot store's retention, shared with the InfluxDB database via
    # IONBEAM_RETENTION. Windows seal at it and the lateness histogram rolls
    # over it — a window cannot be rebuilt from data that has aged out of the
    # time-series DB.
    retention: timedelta = timedelta(days=7)


@dataclass(frozen=True)
class Gate:
    """The per-dataset thresholds one handled record evaluates windows against."""

    now: datetime
    settle: timedelta                 # measured p95 arrival lateness; first build waits this long past window end
    rebuild_debounce: timedelta       # each arrival defers a revision by this
    retention: timedelta              # window sealed once now >= end + retention


@dataclass(frozen=True)
class Unchanged:
    """The desired records already match the last build — nothing to do."""


@dataclass(frozen=True)
class Sealed:
    """The window is past the retention floor: a late arrival will not be
    folded in. The data stays in the time-series DB until it ages out, not as
    part of any window build."""


@dataclass(frozen=True)
class Skip:
    """The window's data cannot support a build yet; a future record spanning
    the window re-decides it."""

    reason: str


@dataclass(frozen=True)
class Build:
    """Schedule a build for the moment the window becomes worth building."""

    eligible_at: datetime


Decision = Unchanged | Sealed | Skip | Build


def decide(
    window: Window,
    coverage: CoverageAnalysis,
    state: WindowBuildState | None,
    desired: RecordSet,
    gate: Gate,
) -> Decision:
    """One window's build decision against the dataset's coverage and gates.

    A provisional window builds once its coverage is gap-free and complete —
    no earlier than ``end + settle`` for a first build, and for a revision at
    ``now + rebuild_debounce``, re-decided on every arrival. Past the
    retention floor the window is sealed: its records expire with the hot
    store, so no build could compose them anyway."""
    if state is not None and state.record_ids_hash == desired.hash:
        return Unchanged()

    if gate.now >= window.end + gate.retention:
        return Sealed()

    if coverage.has_gap_in_window(window):
        return Skip("gap")
    if not desired.ids:
        return Skip("no_records")
    if not coverage.fully_covers(window):
        return Skip("incomplete_coverage")

    eligible_at = window.end + gate.settle
    if state is not None:
        eligible_at = max(eligible_at, gate.now + gate.rebuild_debounce)
    return Build(eligible_at=eligible_at)


class DatasetCoordinator:
    def __init__(
        self,
        config: DatasetCoordinatorConfig,
        record_store: CoordinationStore,
        queue: BuildQueue,
        coordinator_metrics: CoordinatorMetrics,
        registry: DatasetRegistry,
    ):
        self.config = config
        self.record_store = record_store
        self.queue = queue
        self._metrics = coordinator_metrics
        self._registry = registry
        self.logger = structlog.get_logger("DatasetCoordinator")

    async def handle(self, event: DataAvailableEvent) -> None:
        with bound_contextvars(correlation_id=str(event.id)):
            started = time.perf_counter()
            try:
                await self._handle(event)
            except Exception as exc:
                self.logger.exception(
                    "Coordinator failed handling claim",
                    error=str(exc),
                    elapsed_ms=int((time.perf_counter() - started) * 1000),
                )
                raise
            self.logger.info(
                "Coordinator handled claim",
                elapsed_ms=int((time.perf_counter() - started) * 1000),
            )

    async def _handle(self, event: DataAvailableEvent) -> None:
        dataset = event.metadata.name
        production = self._registry.get(dataset)

        await self.record_store.save_coverage_claim(
            dataset,
            CoverageClaim(
                id=event.id,
                start_time=event.start_time,
                end_time=event.end_time,
                arrived_at=event.arrived_at,
            ),
        )
        for window_record in event.records:
            window = Window(
                dataset, window_record.window_start, production.aggregation_span
            )
            await self.record_store.save_ingestion_record(
                IngestionRecord(
                    id=window_record.id,
                    metadata=event.metadata,
                    start_time=window.start,
                    end_time=window.end,
                    arrived_at=event.arrived_at,
                )
            )
            await self.record_store.add_desired_record_ids(
                window, [str(window_record.id)]
            )

        windows = self._spanned_windows(event, production.aggregation_span)
        coverage = self._analyze_coverage(
            await self.record_store.get_coverage_spans(
                dataset,
                min(window.start for window in windows),
                max(window.end for window in windows),
            )
        )
        gate = Gate(
            now=datetime.now(timezone.utc),
            settle=await self._settle_duration(dataset),
            rebuild_debounce=production.rebuild_debounce,
            retention=self.config.retention,
        )

        for window in windows:
            await self._process_window(window, coverage, gate)

    def _analyze_coverage(
        self, spans: list[tuple[datetime, datetime]]
    ) -> CoverageAnalysis:
        coverage = CoverageAnalysis.of(spans)
        for gap_start, gap_end in coverage.gaps:
            self.logger.warning(
                "Data gap detected",
                gap_start=gap_start.isoformat(),
                gap_end=gap_end.isoformat(),
                duration=str(gap_end - gap_start),
            )
        return coverage

    async def _settle_duration(self, dataset: str) -> timedelta:
        """The measured p95 of this source's arrival lateness. A cold histogram
        yields zero: windows build eagerly and are then revised as data lands,
        with the rebuild debounce keeping those revisions cheap.

        The histogram is filled per-datum at ingestion; this reads the settled
        estimate."""
        measured = await self.record_store.lateness_percentile(
            dataset,
            self.config.settle_percentile,
            self.config.settle_min_samples,
            int(self.config.retention.total_seconds() // 3600),
        )
        self._metrics.observe_lateness_p95(
            dataset, measured.total_seconds() if measured else 0.0
        )
        return measured or timedelta(0)

    async def _process_window(
        self, window: Window, coverage: CoverageAnalysis, gate: Gate
    ) -> None:
        desired = RecordSet.from_list(
            await self.record_store.get_desired_record_ids(window)
        )
        state = await self.record_store.get_window_state(window)

        match decide(window, coverage, state, desired, gate):
            case Unchanged():
                pass
            case Sealed():
                self._metrics.sealed_arrival_dropped(window.dataset)
            case Skip(reason=reason):
                self._metrics.window_skipped(window.dataset, reason)
            case Build(eligible_at=eligible_at):
                await self.queue.schedule(window, eligible_at)
                self._metrics.window_enqueued(window.dataset)
                self.logger.info(
                    "Scheduled window build",
                    dataset=window.dataset,
                    window_start=window.start.isoformat(),
                    window_end=window.end.isoformat(),
                    eligible_at=eligible_at.isoformat(),
                    desired_hash=desired.hash[:8],
                )

    def _spanned_windows(
        self, event: DataAvailableEvent, aggregation: timedelta
    ) -> list[Window]:
        windows = []
        current = align_to_aggregation(event.start_time, aggregation)
        while current < event.end_time:
            windows.append(Window(event.metadata.name, current, aggregation))
            current += aggregation
        return windows
