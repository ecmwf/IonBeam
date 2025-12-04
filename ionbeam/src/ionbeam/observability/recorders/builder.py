from prometheus_client import Counter, Histogram, CollectorRegistry


class BuilderMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self._windows_started_total = Counter(
            name="ionbeam_builder_windows_started_total",
            documentation="Total dataset window builds started",
            labelnames=["dataset"],
            registry=registry,
        )

        self._windows_succeeded_total = Counter(
            name="ionbeam_builder_windows_succeeded_total",
            documentation="Total dataset window builds succeeded",
            labelnames=["dataset"],
            registry=registry,
        )

        self._windows_failed_total = Counter(
            name="ionbeam_builder_windows_failed_total",
            documentation="Total dataset window builds failed",
            labelnames=["dataset"],
            registry=registry,
        )

        self._windows_requeued_total = Counter(
            name="ionbeam_builder_windows_requeued_total",
            documentation="Total dataset windows requeued by reason",
            labelnames=["dataset", "reason"],
            registry=registry,
        )

        self._windows_published_total = Counter(
            name="ionbeam_builder_windows_published_total",
            documentation="Total dataset window publish attempts by status",
            labelnames=["dataset", "status"],
            registry=registry,
        )

        self._window_build_duration_seconds = Histogram(
            name="ionbeam_builder_window_build_duration_seconds",
            documentation="Build duration per dataset window",
            labelnames=["dataset"],
            buckets=[
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
            ],
            registry=registry,
        )

        self._window_rows_exported = Histogram(
            name="ionbeam_builder_window_rows_exported",
            documentation="Rows exported per dataset window",
            labelnames=["dataset"],
            buckets=[10, 100, 1_000, 10_000, 100_000, 1_000_000],
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

    def observe_build_duration(self, dataset: str, seconds: float) -> None:
        self._window_build_duration_seconds.labels(dataset=dataset).observe(seconds)

    def observe_rows_exported(self, dataset: str, rows: int) -> None:
        self._window_rows_exported.labels(dataset=dataset).observe(rows)

    def record_dataset_published(self, dataset: str, status: str) -> None:
        self._windows_published_total.labels(dataset=dataset, status=status).inc()
