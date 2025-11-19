from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry


class CoordinatorMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self._windows_skipped_total = Counter(
            name="ionbeam_coordinator_windows_skipped_total",
            documentation="Total windows skipped by reason",
            labelnames=["dataset", "reason"],
            registry=registry,
        )

        self._windows_enqueued_total = Counter(
            name="ionbeam_coordinator_windows_enqueued_total",
            documentation="Total windows enqueued for build",
            labelnames=["dataset"],
            registry=registry,
        )

        self._queue_size = Gauge(
            name="ionbeam_coordinator_queue_size",
            documentation="Current coordinator queue size (number of window tasks)",
            labelnames=["dataset"],
            registry=registry,
        )

        self._duration_seconds = Histogram(
            name="ionbeam_coordinator_duration_seconds",
            documentation="Coordinator operation duration",
            labelnames=["dataset"],
            buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
            registry=registry,
        )

    def window_skipped(self, dataset: str, reason: str) -> None:
        self._windows_skipped_total.labels(dataset=dataset, reason=reason).inc()

    def window_enqueued(self, dataset: str) -> None:
        self._windows_enqueued_total.labels(dataset=dataset).inc()

    def set_queue_size(self, dataset: str, size: int) -> None:
        self._queue_size.labels(dataset=dataset).set(size)

    def observe_duration(self, dataset: str, seconds: float) -> None:
        self._duration_seconds.labels(dataset=dataset).observe(seconds)
