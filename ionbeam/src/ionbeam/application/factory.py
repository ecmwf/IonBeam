# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Composition root: builds an IonbeamCore and its Flight server from config."""

import logging
import os
import re
import signal
import socket
import threading
from datetime import timedelta
from typing import NamedTuple

import redis.asyncio as redis
import structlog
import yaml
from prometheus_client import (
    CollectorRegistry,
    GCCollector,
    Info,
    PlatformCollector,
    ProcessCollector,
    start_http_server,
)

from ionbeam.application.core import IonbeamCore
from ionbeam.datasets import DatasetRegistry
from ionbeam.flight.server import IonbeamFlightServer
from ionbeam.handlers import (
    DatasetBuilderConfig,
    DatasetBuilder,
    DatasetCoordinatorConfig,
    DatasetCoordinator,
    Ingestion,
)
from ionbeam.messaging import InMemoryEventBus, RedisStreamsEventBus
from ionbeam.observability.logging import setup_logging
from ionbeam.observability.recorders import (
    BuilderMetrics,
    CoordinatorMetrics,
    EventBusMetrics,
    FlightMetrics,
    IngestionMetrics,
)
from ionbeam.scheduler import SourceSchedule, SourceScheduler
from ionbeam.storage.arrow_store import arrow_store_from_config
from ionbeam.storage.build_queue import RedisBuildQueue
from ionbeam.storage.influx_timeseries import InfluxTimeSeriesDatabase
from ionbeam.storage.coordination_store import RedisCoordinationStore
from ionbeam.storage.memory_coordination import (
    InMemoryBuildQueue,
    InMemoryCoordinationStore,
    InMemoryTriggerClaims,
)
from ionbeam.storage.memory_timeseries import InMemoryTimeSeriesDatabase
from ionbeam.storage.trigger_claims import RedisTriggerClaims, TriggerClaims

logger = structlog.get_logger(__name__)


class Ionbeam(NamedTuple):
    core: IonbeamCore
    metrics_registry: CollectorRegistry
    trigger_claims: TriggerClaims
    schedules: list[SourceSchedule]


def load_config() -> dict:
    path = os.getenv("IONBEAM_CONFIG_PATH", "config.yaml")
    with open(path) as f:
        return yaml.safe_load(f) or {}


_RETENTION_UNITS = {"h": "hours", "d": "days", "w": "weeks"}


def parse_retention(value: str) -> timedelta:
    """An InfluxDB-style duration (``7d``, ``168h``, ``2w``) as a timedelta, so
    one IONBEAM_RETENTION value drives both the database and this service."""
    match = re.fullmatch(r"(\d+)([hdw])", value)
    if match is None:
        raise ValueError(
            f"IONBEAM_RETENTION must be an InfluxDB duration like 7d, 168h or 2w, got {value!r}"
        )
    return timedelta(**{_RETENTION_UNITS[match.group(2)]: int(match.group(1))})


def reject_windows_beyond_retention(
    schedules: list[SourceSchedule], retention: timedelta
) -> None:
    """InfluxDB expires rows by observation time, and accepts a write of already
    expired rows with a success status. A window reaching past retention would
    therefore fetch and write data that no query can ever return, so a schedule
    configured that way fails startup rather than losing data quietly."""
    for schedule in schedules:
        reach = schedule.window_lag + schedule.window_size
        if reach >= retention:
            raise ValueError(
                f"{schedule.source_name} window reaches {reach} back, beyond the "
                f"{retention} retention; rows that old are dropped on write"
            )


def build(config: dict) -> Ionbeam:
    metrics_registry = CollectorRegistry()
    ProcessCollector(registry=metrics_registry)
    PlatformCollector(registry=metrics_registry)
    GCCollector(registry=metrics_registry)
    # the hostname is the pod name in k8s — names this replica's series for
    # scrapers that address pods by IP
    Info("ionbeam_instance", "Identity of this replica", registry=metrics_registry).info(
        {"pod": socket.gethostname()}
    )

    coordination = config["coordination"]
    redis_client = (
        redis.from_url(coordination["redis"]["url"])
        if any(
            section["adapter"] == "redis"
            for section in (
                coordination["record_store"],
                coordination["queue"],
                config["messaging"],
            )
        )
        else None
    )

    # one retention knob, shared with the InfluxDB database itself: ingestion's
    # lateness histogram, the coordinator's build gate, the builder's finalize
    # floor, and the record store TTL follow it
    retention = parse_retention(os.getenv("IONBEAM_RETENTION", "7d"))
    coordinator_config = DatasetCoordinatorConfig(
        retention=retention, **(config.get("dataset_coordinator") or {})
    )

    if coordination["record_store"]["adapter"] == "redis":
        record_store = RedisCoordinationStore(redis_client, retention=retention)
        trigger_claims: TriggerClaims = RedisTriggerClaims(redis_client)
    else:
        record_store = InMemoryCoordinationStore(retention=retention)
        trigger_claims = InMemoryTriggerClaims()

    queue = (
        RedisBuildQueue(redis_client)
        if coordination["queue"]["adapter"] == "redis"
        else InMemoryBuildQueue()
    )
    event_bus = (
        RedisStreamsEventBus(redis_client, EventBusMetrics(metrics_registry))
        if config["messaging"]["adapter"] == "redis"
        else InMemoryEventBus()
    )

    influx = config["storage"].get("influx") or {}
    timeseries_db = (
        InfluxTimeSeriesDatabase(
            host=influx["host"], database=influx["database"], token=influx.get("token")
        )
        if config["storage"]["timeseries"]["adapter"] == "influx"
        else InMemoryTimeSeriesDatabase()
    )
    arrow_store = arrow_store_from_config(config.get("arrow_store"))

    dataset_registry = DatasetRegistry.from_config(config.get("datasets"))

    core = IonbeamCore(
        ingestion=Ingestion(
            timeseries_db,
            IngestionMetrics(metrics_registry),
            record_store,
            dataset_registry,
            retention=retention,
        ),
        coordinator=DatasetCoordinator(
            coordinator_config,
            record_store,
            queue,
            CoordinatorMetrics(metrics_registry),
            dataset_registry,
        ),
        builder=DatasetBuilder(
            DatasetBuilderConfig(
                **{**(config.get("dataset_builder") or {}), "retention": retention}
            ),
            record_store,
            queue,
            timeseries_db,
            BuilderMetrics(metrics_registry),
            arrow_store,
            event_publisher=event_bus.publish_dataset_available,
            registry=dataset_registry,
        ),
        record_store=record_store,
        arrow_store=arrow_store,
        event_bus=event_bus,
    )

    scheduler_config = config.get("scheduler") or {}
    schedules = (
        [SourceSchedule(**window) for window in scheduler_config.get("windows") or []]
        if scheduler_config.get("enabled", True)
        else []
    )
    reject_windows_beyond_retention(schedules, retention)
    return Ionbeam(core, metrics_registry, trigger_claims, schedules)


def run(host: str, port: int) -> None:
    config = load_config()

    log_config = config.get("logging") or {}
    setup_logging(level=getattr(logging, log_config.get("level", "INFO")))

    ionbeam = build(config)

    metrics_config = config.get("metrics") or {}
    start_http_server(
        metrics_config.get("port", 8000), registry=ionbeam.metrics_registry
    )

    scheduler = SourceScheduler(
        ionbeam.schedules,
        ionbeam.core.trigger_source,
        ionbeam.trigger_claims.try_claim,
    )

    server = IonbeamFlightServer(
        f"grpc://{host}:{port}",
        ionbeam.core,
        FlightMetrics(ionbeam.metrics_registry),
    )
    server.spawn(ionbeam.core.start())
    server.spawn(scheduler.start())

    stop_requested = threading.Event()

    def _request_stop(signum, _frame):
        logger.info("ionbeam received signal; shutting down", signal=signum)
        stop_requested.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    # serve() blocks its thread inside gRPC, where Python signal handlers never
    # run; serve on a worker thread and keep the main thread free for signals.
    serve_thread = threading.Thread(target=server.serve, name="flight-serve")
    serve_thread.start()

    logger.info("ionbeam Flight endpoint listening", host=host, port=port)
    stop_requested.wait()

    # Stops the scheduler and builder on the server loop while it still runs,
    # then drains gRPC and stops the loop.
    server.spawn(scheduler.stop()).result()
    server.spawn(ionbeam.core.stop()).result()
    server.shutdown()
    serve_thread.join()
    logger.info("ionbeam stopped")
