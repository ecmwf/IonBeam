# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from prometheus_client import CollectorRegistry, Counter, Gauge


class EventBusMetrics:
    def __init__(self, registry: CollectorRegistry) -> None:
        self._published_total = Counter(
            name="ionbeam_eventbus_published_total",
            documentation="Events published per stream",
            labelnames=["stream"],
            registry=registry,
        )

        self._delivered_total = Counter(
            name="ionbeam_eventbus_delivered_total",
            documentation="Events handed to a consumer per stream and group, redeliveries included",
            labelnames=["stream", "group"],
            registry=registry,
        )

        self._acked_total = Counter(
            name="ionbeam_eventbus_acked_total",
            documentation=(
                "Events acknowledged per stream and group. An ack certifies the "
                "subscriber's handler completed its work, so this counts finished "
                "source fetches and exports at the bus"
            ),
            labelnames=["stream", "group"],
            registry=registry,
        )

        self._reclaimed_total = Counter(
            name="ionbeam_eventbus_reclaimed_total",
            documentation=(
                "Events reclaimed from a dead or stuck consumer per stream and "
                "group. A steady rate means handlers are dying or hanging "
                "before they can ack"
            ),
            labelnames=["stream", "group"],
            registry=registry,
        )

        self._dead_lettered_total = Counter(
            name="ionbeam_eventbus_dead_lettered_total",
            documentation="Poison events parked on the dead-letter stream after repeated failed deliveries",
            labelnames=["stream", "group"],
            registry=registry,
        )

        self._last_ack_timestamp = Gauge(
            name="ionbeam_eventbus_last_ack_timestamp_seconds",
            documentation="Unix time of the last acknowledged event per stream and group",
            labelnames=["stream", "group"],
            registry=registry,
        )

    def published(self, stream: str) -> None:
        self._published_total.labels(stream=stream).inc()

    def delivered(self, stream: str, group: str) -> None:
        self._delivered_total.labels(stream=stream, group=group).inc()

    def acked(self, stream: str, group: str) -> None:
        self._acked_total.labels(stream=stream, group=group).inc()
        self._last_ack_timestamp.labels(stream=stream, group=group).set_to_current_time()

    def reclaimed(self, stream: str, group: str) -> None:
        self._reclaimed_total.labels(stream=stream, group=group).inc()

    def dead_lettered(self, stream: str, group: str) -> None:
        self._dead_lettered_total.labels(stream=stream, group=group).inc()
