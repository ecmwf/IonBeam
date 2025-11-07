import os
from pathlib import Path

import redis.asyncio as redis
from dependency_injector import containers, providers
from faststream.rabbit import RabbitBroker
from faststream.rabbit.prometheus import RabbitPrometheusMiddleware
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from prometheus_client import CollectorRegistry

from ionbeam.observability.metrics import IonbeamMetrics
from ionbeam.sources.metno.config import NetAtmoHTTPConfig, NetAtmoMQTTConfig
from ionbeam.sources.metno.netatmo_mqtt import NetAtmoMQTTSource
from ionbeam.sources.metno.netatmo_qc_mqtt import NetAtmoQCMQTTSource
from ionbeam.storage.arrow_store import LocalFileSystemStore

from ..projections.ionbeam_legacy.projection_service import IonbeamLegacyConfig, IonbeamLegacyProjectionService
from ..projections.odb.projection_service import ODBProjectionService, ODBProjectionServiceConfig
from ..projections.pygeoapi.projection_service import (
    PyGeoApiConfig,
    PyGeoApiProjectionService,
)
from ..scheduler.source_scheduler import SchedulerConfig, SourceScheduler
from ..services.dataset_builder import DatasetBuilder, DatasetBuilderConfig
from ..services.dataset_coordinator import DatasetCoordinatorConfig, DatasetCoordinatorService
from ..services.ingestion import IngestionConfig, IngestionService
from ..sources.acronet import AcronetConfig, AcronetSource
from ..sources.ioncannon import IonCannonConfig, IonCannonSource
from ..sources.meteotracker import MeteoTrackerConfig, MeteoTrackerSource
from ..sources.metno.netatmo import NetAtmoSource
from ..sources.metno.netatmo_archive import NetAtmoArchiveConfig, NetAtmoArchiveSource
from ..sources.sensor_community import SensorCommunityConfig, SensorCommunitySource
from ..storage.ingestion_record_store import RedisIngestionRecordStore
from ..storage.ordered_queue import RedisOrderedQueue
from ..storage.timeseries import InfluxDBTimeSeriesDatabase

config_path = os.getenv("IONBEAM_CONFIG_PATH", "config.yaml")


class IonbeamContainer(containers.DeclarativeContainer):
    config = providers.Configuration(yaml_files=[config_path])

    registry = providers.Singleton(CollectorRegistry)
    metrics = providers.Singleton(IonbeamMetrics, registry=registry)

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

    # shared redis client resource
    redis_client = providers.Resource(
        redis.from_url,
        config.redis.redis_url,
    )

    # ingestion record store using shared client
    ingestion_record_store = providers.Factory(
        RedisIngestionRecordStore,
        client=redis_client,
    )

    # ordered queue using shared client
    ordered_queue = providers.Factory(
        RedisOrderedQueue,
        client=redis_client,
        queue_key=config.dataset_aggregator.queue_key,
    )

    # shared influxdb client resource
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
            base_path=providers.Callable(lambda cfg: Path(cfg.get("data_path", "./data/")), config.arrow_store),
        ),
    )

    # source scheduler
    scheduler_config = providers.Factory(lambda cfg: SchedulerConfig(**cfg), config.sources.scheduler)
    source_scheduler = providers.Factory(SourceScheduler, config=scheduler_config, broker=broker)

    # sensor_community
    sensor_community_config = providers.Factory(lambda cfg: SensorCommunityConfig(**cfg), config.sources.sensor_community)
    sensor_community_source = providers.Factory(SensorCommunitySource, config=sensor_community_config, metrics=metrics, arrow_store=arrow_store)

    # meteotracker
    meteotracker_config = providers.Factory(lambda cfg: MeteoTrackerConfig(**cfg), config.sources.meteotracker)
    meteotracker_source = providers.Factory(MeteoTrackerSource, config=meteotracker_config, metrics=metrics, arrow_store=arrow_store)

    # acronet
    acronet_config = providers.Factory(lambda cfg: AcronetConfig(**cfg), config.sources.acronet)
    acronet_source = providers.Factory(AcronetSource, config=acronet_config, metrics=metrics, arrow_store=arrow_store)

    # netatmo http
    netatmo_http_config = providers.Factory(lambda cfg: NetAtmoHTTPConfig(**cfg), config.sources.netatmo.http)
    netatmo_http_source = providers.Factory(NetAtmoSource, config=netatmo_http_config, metrics=metrics, arrow_store=arrow_store)

    # netatmo - mqtt
    netatmo_mqtt_config = providers.Factory(lambda cfg: NetAtmoMQTTConfig(**cfg), config.sources.netatmo.mqtt)
    netatmo_mqtt_source = providers.Factory(
        NetAtmoMQTTSource,
        config=netatmo_mqtt_config,
        metrics=metrics,
        arrow_store=arrow_store,
    )

    # netatmo QC - mqtt
    netatmo_qc_mqtt_source = providers.Factory(
        NetAtmoQCMQTTSource,
        config=netatmo_mqtt_config,
        metrics=metrics,
        arrow_store=arrow_store,
    )

    # netatmo - archive
    netatmo_archive_config = providers.Factory(lambda cfg: NetAtmoArchiveConfig(**cfg), config.sources.netatmo_archive)
    netatmo_archive_source = providers.Factory(NetAtmoArchiveSource, config=netatmo_archive_config, metrics=metrics, arrow_store=arrow_store)

    # ioncannon - stress tester
    # ion_cannon_config = providers.Factory(lambda cfg: IonCannonConfig(**cfg), config.sources.ioncannon)
    ion_cannon_source = providers.Factory(IonCannonSource, config=IonCannonConfig(), metrics=metrics, arrow_store=arrow_store)

    # ingestion service
    ingestion_service = providers.Factory(
        IngestionService, config=IngestionConfig(), timeseries_db=timeseries_db, metrics=metrics, arrow_store=arrow_store
    )

    # dataset coordinator service
    dataset_coordinator_service = providers.Factory(
        DatasetCoordinatorService,
        config=DatasetCoordinatorConfig(),
        record_store=ingestion_record_store,
        queue=ordered_queue,
        metrics=metrics,
    )

    # dataset builder service
    dataset_builder_config = providers.Factory(lambda cfg: DatasetBuilderConfig(**cfg), config.dataset_aggregator)
    dataset_builder_service = providers.Factory(
        DatasetBuilder,
        config=dataset_builder_config,
        record_store=ingestion_record_store,
        queue=ordered_queue,
        timeseries_db=timeseries_db,
        arrow_store=arrow_store,
        broker=broker,
        metrics=metrics,
    )

    # PyGeoAPI projection service
    pygeoapi_projection_service_config = providers.Factory(lambda cfg: PyGeoApiConfig(**cfg), config.projections.pygeoapi_service)
    pygeoapi_projection_service = providers.Factory(
        PyGeoApiProjectionService, config=pygeoapi_projection_service_config, metrics=metrics, arrow_store=arrow_store
    )

    # ODB projection service
    odb_projection_service_config = providers.Factory(lambda cfg: ODBProjectionServiceConfig(**cfg), config.projections.odb_service)
    odb_projection_service = providers.Factory(ODBProjectionService, config=odb_projection_service_config, metrics=metrics, arrow_store=arrow_store)

    # Ionbeam Legacy projection service
    ionbeam_legacy_projection_service_config = providers.Factory(
        lambda cfg: IonbeamLegacyConfig(**cfg),
        config.projections.ionbeam_legacy,
    )
    ionbeam_legacy_projection_service = providers.Factory(
        IonbeamLegacyProjectionService,
        config=ionbeam_legacy_projection_service_config,
        metrics=metrics,
        arrow_store=arrow_store,
    )
