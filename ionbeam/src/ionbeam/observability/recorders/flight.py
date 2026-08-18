# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import time
from contextlib import contextmanager

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class FlightMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self._requests_total = Counter(
            name="ionbeam_flight_requests_total",
            documentation="Flight RPCs by operation and outcome",
            labelnames=["op", "status"],
            registry=registry,
        )

        self._request_duration_seconds = Histogram(
            name="ionbeam_flight_request_duration_seconds",
            documentation="Flight RPC duration by operation; for ingest this spans the whole upload",
            labelnames=["op"],
            buckets=[0.01, 0.05, 0.25, 1.0, 5.0, 15.0, 60.0, 300.0, 900.0],
            registry=registry,
        )

        self._active_subscriptions = Gauge(
            name="ionbeam_flight_active_subscriptions",
            documentation="Open push-subscription exchanges by operation",
            labelnames=["op"],
            registry=registry,
        )

    @contextmanager
    def track(self, op: str):
        """Count the wrapped RPC and time it; any exception counts as an error."""
        start = time.perf_counter()
        try:
            yield
        except BaseException:
            self._requests_total.labels(op=op, status="error").inc()
            raise
        self._requests_total.labels(op=op, status="ok").inc()
        self._request_duration_seconds.labels(op=op).observe(time.perf_counter() - start)

    @contextmanager
    def track_subscription(self, op: str):
        """Count a long-lived exchange and hold it in the in-progress gauge; a
        normal teardown (client cancel, shutdown) is ok, an exception is an error."""
        with self._active_subscriptions.labels(op=op).track_inprogress():
            try:
                yield
            except BaseException:
                self._requests_total.labels(op=op, status="error").inc()
                raise
            self._requests_total.labels(op=op, status="ok").inc()
