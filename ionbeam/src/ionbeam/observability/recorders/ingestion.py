from prometheus_client import Counter, Histogram, CollectorRegistry


class IngestionMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self._data_points = Histogram(
            name="ionbeam_ingestion_data_points_per_operation",
            documentation="Data points (observations) ingested per operation",
            labelnames=["dataset"],
            buckets=[10, 100, 1_000, 10_000, 100_000, 1_000_000],
            registry=registry,
        )

        self._data_points_total = Counter(
            name="ionbeam_ingestion_data_points_total",
            documentation="Total data points (observations) ingested successfully",
            labelnames=["dataset"],
            registry=registry,
        )

        self._duration_seconds = Histogram(
            name="ionbeam_ingestion_duration_seconds",
            documentation="Time to complete ingestion operation (read batches + write to InfluxDB)",
            labelnames=["dataset"],
            buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
            registry=registry,
        )

        self._batches_processed_total = Counter(
            name="ionbeam_ingestion_batches_processed_total",
            documentation="Number of Arrow batches processed during ingestion",
            labelnames=["dataset"],
            registry=registry,
        )

        self._errors_total = Counter(
            name="ionbeam_ingestion_errors_total",
            documentation="Total ingestion errors by type",
            labelnames=["dataset", "error_type"],
            registry=registry,
        )

    def observe_data_points(self, dataset: str, count: int) -> None:
        self._data_points.labels(dataset=dataset).observe(count)
        self._data_points_total.labels(dataset=dataset).inc(count)

    def observe_duration(self, dataset: str, seconds: float) -> None:
        self._duration_seconds.labels(dataset=dataset).observe(seconds)

    def record_batch_processed(self, dataset: str) -> None:
        self._batches_processed_total.labels(dataset=dataset).inc()

    def record_error(self, dataset: str, error_type: str) -> None:
        self._errors_total.labels(dataset=dataset, error_type=error_type).inc()
