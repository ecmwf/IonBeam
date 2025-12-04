# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from prometheus_client import Gauge, Histogram, CollectorRegistry


class HealthMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self._component_status = Gauge(
            name="ionbeam_health_check_status",
            documentation="Component health status (0=unhealthy, 1=healthy)",
            labelnames=["component"],
            registry=registry,
        )

        self._check_duration_seconds = Histogram(
            name="ionbeam_health_check_duration_seconds",
            documentation="Health check duration",
            labelnames=["component"],
            buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0],
            registry=registry,
        )

    def set_component_status(self, component: str, healthy: bool) -> None:
        self._component_status.labels(component=component).set(1 if healthy else 0)

    def observe_health_check_duration(self, component: str, seconds: float) -> None:
        self._check_duration_seconds.labels(component=component).observe(seconds)
