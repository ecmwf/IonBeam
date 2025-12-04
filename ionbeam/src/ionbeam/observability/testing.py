from typing import Any


class NoOpMetrics:
    def __getattr__(self, name: str) -> Any:
        def noop(*args, **kwargs):
            pass

        return noop


class MockIngestionMetrics:
    def __init__(self):
        self.data_points = []
        self.durations = []
        self.batches_processed = 0
        self.errors = []

    def observe_data_points(self, dataset: str, count: int) -> None:
        self.data_points.append((dataset, count))

    def observe_duration(self, dataset: str, seconds: float) -> None:
        self.durations.append((dataset, seconds))

    def record_batch_processed(self, dataset: str) -> None:
        self.batches_processed += 1

    def record_error(self, dataset: str, error_type: str) -> None:
        self.errors.append((dataset, error_type))


class MockCoordinatorMetrics:
    def __init__(self):
        self.windows_skipped = []
        self.windows_enqueued = []
        self.queue_sizes = []
        self.durations = []

    def window_skipped(self, dataset: str, reason: str) -> None:
        self.windows_skipped.append((dataset, reason))

    def window_enqueued(self, dataset: str) -> None:
        self.windows_enqueued.append(dataset)

    def set_queue_size(self, dataset: str, size: int) -> None:
        self.queue_sizes.append((dataset, size))

    def observe_duration(self, dataset: str, seconds: float) -> None:
        self.durations.append((dataset, seconds))


class MockBuilderMetrics:
    def __init__(self):
        self.builds_started = []
        self.builds_succeeded = []
        self.builds_failed = []
        self.requeued_list = []
        self.build_durations = []
        self.rows_exported = []
        self.datasets_published = []

    def build_started(self, dataset: str) -> None:
        self.builds_started.append(dataset)

    def build_succeeded(self, dataset: str) -> None:
        self.builds_succeeded.append(dataset)

    def build_failed(self, dataset: str) -> None:
        self.builds_failed.append(dataset)

    def requeued(self, dataset: str, reason: str) -> None:
        self.requeued_list.append((dataset, reason))

    def observe_build_duration(self, dataset: str, seconds: float) -> None:
        self.build_durations.append((dataset, seconds))

    def observe_rows_exported(self, dataset: str, rows: int) -> None:
        self.rows_exported.append((dataset, rows))

    def record_dataset_published(self, dataset: str, status: str) -> None:
        self.datasets_published.append((dataset, status))
