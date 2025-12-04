# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

"""Dependency injection container for ionbeam services."""

import os
from pathlib import Path

import redis.asyncio as redis
from dependency_injector import containers, providers
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange
from faststream.rabbit.prometheus import RabbitPrometheusMiddleware
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from prometheus_client import CollectorRegistry

from ionbeam.handlers import (
    DatasetBuilderConfig,
    DatasetBuilderHandler,
    DatasetCoordinatorConfig,
    DatasetCoordinatorHandler,
    IngestionConfig,
    IngestionHandler,
)
from ionbeam.observability.recorders import (
    IngestionMetrics,
    CoordinatorMetrics,
    BuilderMetrics,
    SchedulerMetrics,
    HealthMetrics,
)
from ionbeam.scheduler import SchedulerConfig, SourceScheduler
from ionbeam.storage.arrow_store import LocalFileSystemStore
from ionbeam.storage.ingestion_record_store import RedisIngestionRecordStore
from ionbeam.storage.ordered_queue import RedisOrderedQueue
from ionbeam.storage.timeseries import InfluxDBTimeSeriesDatabase

config_path = os.getenv("IONBEAM_CONFIG_PATH", "config.yaml")


class IonbeamCoreContainer(containers.DeclarativeContainer):
    """Minimal container for ionbeam services (no data sources)."""

    config = providers.Configuration(yaml_files=[config_path])

    registry = providers.Singleton(CollectorRegistry)

    ingestion_metrics = providers.Singleton(IngestionMetrics, registry=registry)
    coordinator_metrics = providers.Singleton(CoordinatorMetrics, registry=registry)
    builder_metrics = providers.Singleton(BuilderMetrics, registry=registry)
    scheduler_metrics = providers.Singleton(SchedulerMetrics, registry=registry)
    health_metrics = providers.Singleton(HealthMetrics, registry=registry)

    broker = providers.Resource(
        RabbitBroker,
        url=config.broker.url,
        max_consumers=1,
        middlewares=providers.Callable(
            tuple,
            providers.List(
                providers.Singleton(
                    RabbitPrometheusMiddleware,
                    registry=registry,
                    app_name="ionbeam",
                    metrics_prefix="faststream",
                )
            ),
        ),
    )

    # Shared redis client resource
    redis_client = providers.Resource(
        redis.from_url,
        config.redis.redis_url,
    )

    # Ingestion record store using shared client
    ingestion_record_store = providers.Factory(
        RedisIngestionRecordStore,
        client=redis_client,
    )

    # Ordered queue using shared client
    ordered_queue = providers.Factory(
        RedisOrderedQueue,
        client=redis_client,
        queue_key=config.dataset_aggregator.queue_key,
    )

    # Shared influxdb client resource
    influxdb_client = providers.Resource(
        InfluxDBClientAsync,
        url=config.influxdb_common.influxdb_url,
        token=config.influxdb_common.influxdb_token,
        org=config.influxdb_common.influxdb_org,
        enable_gzip=True,
        timeout=300000,
    )

    timeseries_db = providers.Factory(
        InfluxDBTimeSeriesDatabase,
        client=influxdb_client,
        bucket=config.influxdb_common.influxdb_bucket,
        org=config.influxdb_common.influxdb_org,
    )

    arrow_store = providers.Selector(
        config.arrow_store.type,
        local_filesystem=providers.Singleton(
            LocalFileSystemStore,
            base_path=providers.Callable(
                lambda cfg: Path(cfg.get("data_path", "./data/")), config.arrow_store
            ),
        ),
    )

    # Ingestion handler
    ingestion_handler = providers.Factory(
        IngestionHandler,
        config=IngestionConfig(),
        timeseries_db=timeseries_db,
        ingestion_metrics=ingestion_metrics,
        arrow_store=arrow_store,
    )

    # Dataset coordinator handler
    dataset_coordinator_config = providers.Factory(
        lambda cfg: DatasetCoordinatorConfig(**cfg)
        if cfg
        else DatasetCoordinatorConfig(),
        config.dataset_coordinator,
    )
    dataset_coordinator_handler = providers.Factory(
        DatasetCoordinatorHandler,
        config=dataset_coordinator_config,
        record_store=ingestion_record_store,
        queue=ordered_queue,
        coordinator_metrics=coordinator_metrics,
    )

    # Dataset builder event publisher (wrapper around broker.publish)
    dataset_fanout_exchange = providers.Singleton(
        RabbitExchange,
        "ionbeam.dataset.available",
        type=ExchangeType.FANOUT,
        durable=True,
    )

    dataset_event_publisher = providers.Factory(
        lambda broker_instance, exchange: lambda event: broker_instance.publish(
            event, exchange=exchange
        ),
        broker_instance=broker,
        exchange=dataset_fanout_exchange,
    )

    # Dataset builder handler
    dataset_builder_config = providers.Factory(
        lambda cfg: DatasetBuilderConfig(**cfg), config.dataset_aggregator
    )
    dataset_builder_handler = providers.Factory(
        DatasetBuilderHandler,
        config=dataset_builder_config,
        record_store=ingestion_record_store,
        queue=ordered_queue,
        timeseries_db=timeseries_db,
        arrow_store=arrow_store,
        builder_metrics=builder_metrics,
        event_publisher=dataset_event_publisher,
    )

    # Source scheduler
    scheduler_config = providers.Factory(
        lambda cfg: SchedulerConfig(**cfg)
        if cfg
        else SchedulerConfig(enabled=False, windows=[]),
        config.scheduler,
    )
    source_scheduler = providers.Factory(
        SourceScheduler, config=scheduler_config, broker=broker
    )
