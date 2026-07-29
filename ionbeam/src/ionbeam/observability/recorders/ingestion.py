# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from prometheus_client import Counter, Histogram, CollectorRegistry


class IngestionMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
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

        self._null_values_total = Counter(
            name="ionbeam_ingestion_null_values_total",
            documentation="Null values observed during ingestion by declared source column",
            labelnames=["dataset", "column"],
            registry=registry,
        )

        self._dropped_time_rows_total = Counter(
            name="ionbeam_ingestion_dropped_time_rows_total",
            documentation="Rows dropped because the structural time could not be parsed",
            labelnames=["dataset", "column"],
            registry=registry,
        )

        self._lateness_samples_total = Counter(
            name="ionbeam_ingestion_lateness_samples_total",
            documentation=(
                "Per-datum lateness observations by content novelty: 'new' rows "
                "(unseen content — genuinely new or a changed value like a QC pass) "
                "are recorded into the histogram; 'duplicate' rows (byte-identical to "
                "content already seen in their aggregation window) are suppressed; "
                "'sealed' rows belong to a window already final and are skipped"
            ),
            labelnames=["dataset", "kind"],
            registry=registry,
        )

    def observe_data_points(self, dataset: str, count: int) -> None:
        self._data_points_total.labels(dataset=dataset).inc(count)

    def observe_duration(self, dataset: str, seconds: float) -> None:
        self._duration_seconds.labels(dataset=dataset).observe(seconds)

    def record_batch_processed(self, dataset: str) -> None:
        self._batches_processed_total.labels(dataset=dataset).inc()

    def record_null_values(self, dataset: str, column: str, count: int) -> None:
        if count:
            self._null_values_total.labels(dataset=dataset, column=column).inc(count)

    def record_dropped_time_rows(self, dataset: str, column: str, count: int) -> None:
        if count:
            self._dropped_time_rows_total.labels(dataset=dataset, column=column).inc(count)

    def record_lateness_samples(
        self, dataset: str, new: int, duplicate: int, sealed: int
    ) -> None:
        """new = unseen content, recorded into the histogram (a changed value counts
        as new); duplicate = byte-identical to already-seen content, suppressed;
        sealed = rows for an already-final window, skipped. The
        duplicate/(new+duplicate) ratio shows the dedup working live; a persistent
        sealed stream marks a source delivering data too late to ever build."""
        if new:
            self._lateness_samples_total.labels(dataset=dataset, kind="new").inc(new)
        if duplicate:
            self._lateness_samples_total.labels(dataset=dataset, kind="duplicate").inc(duplicate)
        if sealed:
            self._lateness_samples_total.labels(dataset=dataset, kind="sealed").inc(sealed)
