from __future__ import annotations

from typing import Optional, Protocol

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

_HANDLER_DURATION_BUCKETS = (
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
)
_RECORDS_BUCKETS = (10, 100, 1_000, 10_000, 100_000, 1_000_000)
_INGEST_POINTS_BUCKETS = (10, 100, 1_000, 5_000, 10_000, 50_000, 100_000)
_DURATION_BUCKETS = (0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0)
_DATA_LAG_BUCKETS = (1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0)


class HandlerMetricsProtocol(Protocol):
    """Protocol for handler metrics interface."""
    
    def record_run(self, handler: str, status: str = "success") -> None: ...
    def observe_duration(self, handler: str, seconds: float) -> None: ...


class SourceMetricsProtocol(Protocol):
    """Protocol for source metrics interface."""
    
    def record_ingestion_request(self, source: str, result: str) -> None: ...
    def observe_request_rows(self, source: str, count: int) -> None: ...
    def observe_fetch_duration(self, source: str, seconds: float) -> None: ...
    def observe_data_lag(self, source: str, seconds: float) -> None: ...


class IngestionMetricsProtocol(Protocol):
    """Protocol for ingestion metrics interface."""
    
    def observe_dataset_points(self, dataset: str, points: int) -> None: ...
    def observe_dataset_duration(self, dataset: str, seconds: float) -> None: ...


class CoordinatorMetricsProtocol(Protocol):
    """Protocol for coordinator metrics interface."""
    
    def window_skipped(self, dataset: str, reason: str) -> None: ...
    def window_enqueued(self, dataset: str) -> None: ...
    def set_queue_size(self, dataset: str, size: int) -> None: ...


class BuilderMetricsProtocol(Protocol):
    """Protocol for builder metrics interface."""
    
    def build_started(self, dataset: str) -> None: ...
    def build_succeeded(self, dataset: str) -> None: ...
    def build_failed(self, dataset: str) -> None: ...
    def requeued(self, dataset: str, reason: str) -> None: ...
    def publish(self, dataset: str, result: str) -> None: ...
    def observe_build_duration(self, dataset: str, seconds: float) -> None: ...
    def observe_rows_exported(self, dataset: str, rows: int) -> None: ...


class IonbeamMetricsProtocol(Protocol):
    """Protocol for the main metrics interface."""
    
    @property
    def handlers(self) -> HandlerMetricsProtocol: ...
    
    @property
    def sources(self) -> SourceMetricsProtocol: ...
    
    @property
    def ingestion(self) -> IngestionMetricsProtocol: ...
    
    @property
    def coordinator(self) -> CoordinatorMetricsProtocol: ...
    
    @property
    def builders(self) -> BuilderMetricsProtocol: ...


class HandlerMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self._runs_total = Counter(
            "ionbeam_handler_runs_total",
            "Total handler executions by handler name and status.",
            labelnames=("handler", "status"),
            registry=registry,
        )
        self._duration_seconds = Histogram(
            "ionbeam_handler_duration_seconds",
            "Handler execution duration.",
            labelnames=("handler",),
            buckets=_HANDLER_DURATION_BUCKETS,
            registry=registry,
        )

    def record_run(self, handler: str, status: str = "success") -> None:
        self._runs_total.labels(handler=handler, status=status).inc()

    def observe_duration(self, handler: str, seconds: float) -> None:
        self._duration_seconds.labels(handler=handler).observe(seconds)


class SourceMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self._ingestion_requests_total = Counter(
            "ionbeam_source_ingestion_requests_total",
            "Total ingestion requests emitted by result.",
            labelnames=("source", "result"),
            registry=registry,
        )
        self._ingestion_request_rows = Histogram(
            "ionbeam_source_ingestion_request_rows",
            "Number of rows emitted per ingestion request.",
            labelnames=("source",),
            buckets=_RECORDS_BUCKETS,
            registry=registry,
        )
        self._ingestion_fetch_duration = Histogram(
            "ionbeam_source_ingestion_fetch_duration_seconds",
            "Duration of data fetching and processing in source handler.",
            labelnames=("source",),
            buckets=_DURATION_BUCKETS,
            registry=registry,
        )
        self._data_lag_seconds = Histogram(
            "ionbeam_source_data_lag_seconds",
            "Age of data from earliest timestamp to ingestion completion.",
            labelnames=("source",),
            buckets=_DATA_LAG_BUCKETS,
            registry=registry,
        )

    def record_ingestion_request(self, source: str, result: str) -> None:
        self._ingestion_requests_total.labels(source=source, result=result).inc()

    def observe_request_rows(self, source: str, count: int) -> None:
        self._ingestion_request_rows.labels(source=source).observe(count)

    def observe_fetch_duration(self, source: str, seconds: float) -> None:
        self._ingestion_fetch_duration.labels(source=source).observe(seconds)

    def observe_data_lag(self, source: str, seconds: float) -> None:
        self._data_lag_seconds.labels(source=source).observe(seconds)


class IngestionMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self._dataset_points = Histogram(
            "ionbeam_ingestion_dataset_points",
            "Total points ingested per dataset per request.",
            labelnames=("dataset",),
            buckets=_INGEST_POINTS_BUCKETS,
            registry=registry,
        )
        self._dataset_duration = Histogram(
            "ionbeam_ingestion_dataset_duration_seconds",
            "Time to ingest data per dataset (subset of handler duration).",
            labelnames=("dataset",),
            buckets=_HANDLER_DURATION_BUCKETS,
            registry=registry,
        )

    def observe_dataset_points(self, dataset: str, points: int) -> None:
        self._dataset_points.labels(dataset=dataset).observe(points)

    def observe_dataset_duration(self, dataset: str, seconds: float) -> None:
        self._dataset_duration.labels(dataset=dataset).observe(seconds)


class CoordinatorMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self._windows_skipped_total = Counter(
            "ionbeam_coord_windows_skipped_total",
            "Total windows skipped by reason.",
            labelnames=("dataset", "reason"),
            registry=registry,
        )
        self._windows_enqueued_total = Counter(
            "ionbeam_coord_windows_enqueued_total",
            "Total windows enqueued for build.",
            labelnames=("dataset",),
            registry=registry,
        )
        self._queue_size = Gauge(
            "ionbeam_coord_queue_size",
            "Current coordinator queue size (number of window tasks).",
            labelnames=("dataset",),
            registry=registry,
        )

    def window_skipped(self, dataset: str, reason: str) -> None:
        self._windows_skipped_total.labels(dataset=dataset, reason=reason).inc()

    def window_enqueued(self, dataset: str) -> None:
        self._windows_enqueued_total.labels(dataset=dataset).inc()

    def set_queue_size(self, dataset: str, size: int) -> None:
        self._queue_size.labels(dataset=dataset).set(size)


class BuilderMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self._windows_started_total = Counter(
            "ionbeam_window_builds_started_total",
            "Total dataset window builds started.",
            labelnames=("dataset",),
            registry=registry,
        )
        self._windows_succeeded_total = Counter(
            "ionbeam_window_builds_succeeded_total",
            "Total dataset window builds succeeded.",
            labelnames=("dataset",),
            registry=registry,
        )
        self._windows_failed_total = Counter(
            "ionbeam_window_builds_failed_total",
            "Total dataset window builds failed.",
            labelnames=("dataset",),
            registry=registry,
        )
        self._windows_requeued_total = Counter(
            "ionbeam_window_requeued_total",
            "Total dataset windows requeued by reason.",
            labelnames=("dataset", "reason"),
            registry=registry,
        )
        self._windows_publish_total = Counter(
            "ionbeam_window_publish_total",
            "Total dataset window publish attempts by result.",
            labelnames=("dataset", "result"),
            registry=registry,
        )
        self._window_build_duration = Histogram(
            "ionbeam_window_build_duration_seconds",
            "Build duration per dataset window.",
            labelnames=("dataset",),
            buckets=_HANDLER_DURATION_BUCKETS,
            registry=registry,
        )
        self._window_rows_exported = Histogram(
            "ionbeam_window_rows_exported",
            "Rows exported per dataset window.",
            labelnames=("dataset",),
            buckets=_RECORDS_BUCKETS,
            registry=registry,
        )

    def build_started(self, dataset: str) -> None:
        self._windows_started_total.labels(dataset=dataset).inc()

    def build_succeeded(self, dataset: str) -> None:
        self._windows_succeeded_total.labels(dataset=dataset).inc()

    def build_failed(self, dataset: str) -> None:
        self._windows_failed_total.labels(dataset=dataset).inc()

    def requeued(self, dataset: str, reason: str) -> None:
        self._windows_requeued_total.labels(dataset=dataset, reason=reason).inc()

    def publish(self, dataset: str, result: str) -> None:
        self._windows_publish_total.labels(dataset=dataset, result=result).inc()

    def observe_build_duration(self, dataset: str, seconds: float) -> None:
        self._window_build_duration.labels(dataset=dataset).observe(seconds)

    def observe_rows_exported(self, dataset: str, rows: int) -> None:
        self._window_rows_exported.labels(dataset=dataset).observe(rows)


class IonbeamMetrics:
    def __init__(self, registry: Optional[CollectorRegistry] = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.handlers = HandlerMetrics(self.registry)
        self.sources = SourceMetrics(self.registry)
        self.ingestion = IngestionMetrics(self.registry)
        self.coordinator = CoordinatorMetrics(self.registry)
        self.builders = BuilderMetrics(self.registry)


__all__ = [
    "IonbeamMetrics",
    "IonbeamMetricsProtocol",
    "HandlerMetrics",
    "HandlerMetricsProtocol",
    "SourceMetrics",
    "SourceMetricsProtocol",
    "IngestionMetrics",
    "IngestionMetricsProtocol",
    "CoordinatorMetrics",
    "CoordinatorMetricsProtocol",
    "BuilderMetrics",
    "BuilderMetricsProtocol",
]
