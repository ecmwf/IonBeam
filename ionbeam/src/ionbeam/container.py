# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Dependency injection container for the ionbeam service."""

import os
from datetime import timedelta

import redis.asyncio as redis
from dependency_injector import containers, providers
from prometheus_client import CollectorRegistry

from ionbeam.application.core import IonbeamCore
from ionbeam.datasets import DatasetRegistry
from ionbeam.handlers import (
    DatasetBuilderConfig,
    DatasetBuilderHandler,
    DatasetCoordinatorConfig,
    DatasetCoordinatorHandler,
    IngestionHandler,
)
from ionbeam.messaging import InMemoryEventBus, RedisStreamsEventBus
from ionbeam.observability.recorders import (
    BuilderMetrics,
    CoordinatorMetrics,
    IngestionMetrics,
)
from ionbeam.scheduler import SourceSchedule
from ionbeam.storage.arrow_store import arrow_store_from_config
from ionbeam.storage.build_queue import RedisBuildQueue
from ionbeam.storage.influx_timeseries import InfluxTimeSeriesDatabase
from ionbeam.storage.ingestion_record_store import RedisIngestionRecordStore
from ionbeam.storage.memory_coordination import (
    InMemoryBuildQueue,
    InMemoryRecordStore,
    InMemoryTriggerClaims,
)
from ionbeam.storage.memory_timeseries import InMemoryTimeSeriesDatabase
from ionbeam.storage.trigger_claims import RedisTriggerClaims

config_path = os.getenv("IONBEAM_CONFIG_PATH", "config.yaml")


def _source_schedules(cfg) -> list[SourceSchedule]:
    if not cfg or not cfg.get("enabled", True):
        return []
    return [SourceSchedule(**window) for window in cfg.get("windows") or []]


class IonbeamCoreContainer(containers.DeclarativeContainer):
    config = providers.Configuration(yaml_files=[config_path])

    registry = providers.Singleton(CollectorRegistry)

    ingestion_metrics = providers.Singleton(IngestionMetrics, registry=registry)
    coordinator_metrics = providers.Singleton(CoordinatorMetrics, registry=registry)
    builder_metrics = providers.Singleton(BuilderMetrics, registry=registry)

    # Singleton, not Resource: the Redis client is awaitable (lazy connect),
    # and Resource treats an awaitable init result as an async resource,
    # flipping the whole provider graph async.
    redis_client = providers.Singleton(
        redis.from_url,
        config.coordination.redis.url,
    )

    timeseries_db = providers.Selector(
        config.storage.timeseries.adapter,
        influx=providers.Singleton(
            InfluxTimeSeriesDatabase,
            host=config.storage.influx.host,
            database=config.storage.influx.database,
            token=config.storage.influx.token,
        ),
        memory=providers.Singleton(InMemoryTimeSeriesDatabase),
    )

    dataset_coordinator_config = providers.Factory(
        lambda cfg: DatasetCoordinatorConfig(**cfg)
        if cfg
        else DatasetCoordinatorConfig(),
        config.dataset_coordinator,
    )

    # one retention knob: ingestion's lateness histogram, the coordinator's build
    # gate, the builder's finalize floor, and the record store TTL all follow it
    lateness_retention_hours = dataset_coordinator_config.provided.lateness_retention_hours
    record_retention = providers.Factory(
        lambda hours: timedelta(hours=hours), lateness_retention_hours
    )

    ingestion_record_store = providers.Selector(
        config.coordination.record_store.adapter,
        redis=providers.Singleton(
            RedisIngestionRecordStore,
            client=redis_client,
            retention=record_retention,
        ),
        memory=providers.Singleton(InMemoryRecordStore, retention=record_retention),
    )

    build_queue = providers.Selector(
        config.coordination.queue.adapter,
        redis=providers.Singleton(RedisBuildQueue, client=redis_client),
        memory=providers.Singleton(InMemoryBuildQueue),
    )

    arrow_store = providers.Singleton(arrow_store_from_config, config.arrow_store)

    event_bus = providers.Selector(
        config.messaging.adapter,
        memory=providers.Singleton(InMemoryEventBus),
        redis=providers.Singleton(RedisStreamsEventBus, client=redis_client),
    )

    # Server-side, per-dataset production config (aggregation span, presentation
    # metadata, and finaliser thresholds) — the concerns a source has no business
    # owning. Unconfigured datasets fall back to defaults so ingestion still works.
    dataset_registry = providers.Singleton(
        DatasetRegistry.from_config,
        config.datasets,
    )

    ingestion_handler = providers.Factory(
        IngestionHandler,
        timeseries_db=timeseries_db,
        ingestion_metrics=ingestion_metrics,
        record_store=ingestion_record_store,
        registry=dataset_registry,
        retention=record_retention,
    )
    dataset_coordinator_handler = providers.Factory(
        DatasetCoordinatorHandler,
        config=dataset_coordinator_config,
        record_store=ingestion_record_store,
        queue=build_queue,
        coordinator_metrics=coordinator_metrics,
        registry=dataset_registry,
    )

    dataset_event_publisher = providers.Factory(
        lambda bus: bus.publish_dataset_available, event_bus
    )

    dataset_builder_config = providers.Factory(
        lambda cfg, retention: DatasetBuilderConfig(**{**cfg, "retention": retention}),
        config.dataset_builder,
        record_retention,
    )
    dataset_builder_handler = providers.Factory(
        DatasetBuilderHandler,
        config=dataset_builder_config,
        record_store=ingestion_record_store,
        queue=build_queue,
        timeseries_db=timeseries_db,
        arrow_store=arrow_store,
        builder_metrics=builder_metrics,
        event_publisher=dataset_event_publisher,
        registry=dataset_registry,
    )

    source_schedules = providers.Factory(_source_schedules, config.scheduler)

    trigger_claims = providers.Selector(
        config.coordination.record_store.adapter,
        redis=providers.Singleton(RedisTriggerClaims, client=redis_client),
        memory=providers.Singleton(InMemoryTriggerClaims),
    )

    ionbeam_core = providers.Factory(
        IonbeamCore,
        ingestion=ingestion_handler,
        coordinator=dataset_coordinator_handler,
        builder=dataset_builder_handler,
        record_store=ingestion_record_store,
        arrow_store=arrow_store,
        event_bus=event_bus,
    )
