# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Composition root: builds an IonbeamCore and its Flight server from config."""

import logging
import os
import signal
import threading
from datetime import timedelta
from pathlib import Path
from typing import NamedTuple

import redis.asyncio as redis
import structlog
import yaml
from prometheus_client import CollectorRegistry, start_http_server

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


def build(config: dict) -> Ionbeam:
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

    coordinator_config = DatasetCoordinatorConfig(
        **(config.get("dataset_coordinator") or {})
    )
    # one retention knob: ingestion's lateness histogram, the coordinator's build
    # gate, the builder's finalize floor, and the record store TTL all follow it
    retention = timedelta(hours=coordinator_config.lateness_retention_hours)

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
        RedisStreamsEventBus(redis_client)
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

    metrics_registry = CollectorRegistry()
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
    return Ionbeam(core, metrics_registry, trigger_claims, schedules)


def run(host: str, port: int) -> None:
    config = load_config()

    log_config = config.get("logging") or {}
    setup_logging(
        level=getattr(logging, log_config.get("level", "INFO")),
        log_dir=Path(log_config.get("log_dir", "./logs")),
        log_name=log_config.get("log_name", "ionbeam.log"),
    )

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

    server = IonbeamFlightServer(f"grpc://{host}:{port}", ionbeam.core)
    server.spawn(ionbeam.core.start())
    server.spawn(scheduler.start())

    stop_requested = threading.Event()

    def _request_stop(signum, _frame):
        logger.info("ionbeam received signal; shutting down", signal=signum)
        stop_requested.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    # serve() blocks its thread inside gRPC where Python signal handlers never run,
    # so serve on a worker thread and keep the main thread free to react to signals.
    serve_thread = threading.Thread(target=server.serve, name="flight-serve")
    serve_thread.start()

    logger.info("ionbeam Flight endpoint listening", host=host, port=port)
    stop_requested.wait()

    # Stop the scheduler and builder on the server loop while it still runs,
    # then drain gRPC and stop the loop.
    server.spawn(scheduler.stop()).result()
    server.spawn(ionbeam.core.stop()).result()
    server.shutdown()
    serve_thread.join()
    logger.info("ionbeam stopped")
