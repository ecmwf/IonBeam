from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry


class SchedulerMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self._triggers_total = Counter(
            name="ionbeam_scheduler_triggers_total",
            documentation="Total trigger events by status",
            labelnames=["source", "status"],
            registry=registry,
        )

        self._trigger_delay_seconds = Histogram(
            name="ionbeam_scheduler_trigger_delay_seconds",
            documentation="Delay between expected and actual trigger time",
            labelnames=["source"],
            buckets=[0.1, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0],
            registry=registry,
        )

        self._next_run_timestamp = Gauge(
            name="ionbeam_scheduler_next_run_timestamp",
            documentation="Next scheduled run timestamp (unix epoch)",
            labelnames=["source"],
            registry=registry,
        )

        self._enabled = Gauge(
            name="ionbeam_scheduler_enabled",
            documentation="Scheduler enabled status (0=disabled, 1=enabled)",
            labelnames=["source"],
            registry=registry,
        )

    def record_trigger(self, source: str, status: str) -> None:
        self._triggers_total.labels(source=source, status=status).inc()

    def observe_trigger_delay(self, source: str, seconds: float) -> None:
        self._trigger_delay_seconds.labels(source=source).observe(seconds)

    def set_next_run_timestamp(self, source: str, timestamp: float) -> None:
        self._next_run_timestamp.labels(source=source).set(timestamp)

    def set_enabled_status(self, source: str, enabled: bool) -> None:
        self._enabled.labels(source=source).set(1 if enabled else 0)
