# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from prometheus_client import CollectorRegistry, Counter, Gauge


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

        self._sealed_arrivals_dropped_total = Counter(
            name="ionbeam_coordinator_sealed_arrivals_dropped_total",
            documentation="Total late records dropped because the window was already final",
            labelnames=["dataset"],
            registry=registry,
        )

        self._lateness_p95_seconds = Gauge(
            name="ionbeam_coordinator_lateness_p95_seconds",
            documentation="Measured p95 of data-arrival lateness driving the build gate",
            labelnames=["dataset"],
            registry=registry,
        )

    def window_skipped(self, dataset: str, reason: str) -> None:
        self._windows_skipped_total.labels(dataset=dataset, reason=reason).inc()

    def window_enqueued(self, dataset: str) -> None:
        self._windows_enqueued_total.labels(dataset=dataset).inc()

    def sealed_arrival_dropped(self, dataset: str) -> None:
        self._sealed_arrivals_dropped_total.labels(dataset=dataset).inc()

    def observe_lateness_p95(self, dataset: str, seconds: float) -> None:
        self._lateness_p95_seconds.labels(dataset=dataset).set(seconds)
