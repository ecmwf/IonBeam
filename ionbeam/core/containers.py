import os

import redis.asyncio as redis
from dependency_injector import containers, providers
from faststream.rabbit import RabbitBroker
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

from ionbeam.sources.metno.netatmo_mqtt import NetAtmoMQTTConfig, NetAtmoMQTTSource

from ..projections.odb.projection_service import ODBProjectionService, ODBProjectionServiceConfig
from ..projections.pygeoapi.projection_service import (
    PyGeoApiConfig,
    PyGeoApiProjectionService,
)
from ..scheduler.source_scheduler import SchedulerConfig, SourceScheduler
from ..services.dataset_builder import DatasetBuilder, DatasetBuilderConfig
from ..services.dataset_coordinator import DatasetCoordinatorConfig, DatasetCoordinatorService
from ..services.ingestion import IngestionConfig, IngestionService
from ..sources.ioncannon import IonCannonConfig, IonCannonSource
from ..sources.meteotracker import MeteoTrackerConfig, MeteoTrackerSource
from ..sources.metno.netatmo import NetAtmoConfig, NetAtmoSource
from ..sources.metno.netatmo_archive import NetAtmoArchiveConfig, NetAtmoArchiveSource
from ..sources.sensor_community import SensorCommunityConfig, SensorCommunitySource
from ..storage.event_store import RedisEventStore
from ..storage.timeseries import InfluxDBTimeSeriesDatabase

config_path = os.getenv("IONBEAM_CONFIG_PATH", "config.yaml")


class IonbeamContainer(containers.DeclarativeContainer):
    config = providers.Configuration(yaml_files=[config_path])

    broker = providers.Resource(RabbitBroker, url=config.broker.url, max_consumers=1)

    # shared redis client resource
    redis_client = providers.Resource(
        redis.from_url,
        config.redis.redis_url
    )

    # event store using shared client
    event_store = providers.Factory(
        RedisEventStore,
        client=redis_client
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
        org=config.influxdb_common.influxdb_org
    )

    # source scheduler
    scheduler_config = providers.Factory(lambda cfg: SchedulerConfig(**cfg), config.sources.scheduler)
    source_scheduler = providers.Factory(SourceScheduler, config=scheduler_config, broker=broker)

    # sensor_community
    sensor_community_config = providers.Factory(lambda cfg: SensorCommunityConfig(**cfg), config.sources.sensor_community)
    sensor_community_source = providers.Factory(SensorCommunitySource, config=sensor_community_config)

    # meteotracker
    meteotracker_config = providers.Factory(lambda cfg: MeteoTrackerConfig(**cfg), config.sources.meteotracker)
    meteotracker_source = providers.Factory(MeteoTrackerSource, config=meteotracker_config)

    # netatmo
    netatmo_config = providers.Factory(lambda cfg: NetAtmoConfig(**cfg), config.sources.netatmo)
    netatmo_source = providers.Factory(NetAtmoSource, config=netatmo_config)
    
    # netatmo - mqtt
    netatmo_mqtt_config = providers.Factory(lambda cfg: NetAtmoMQTTConfig(**cfg), config.sources.netatmo_mqtt)
    netatmo_mqtt_source = providers.Factory(NetAtmoMQTTSource, config=netatmo_mqtt_config)

    # netatmo - archive
    netatmo_archive_config = providers.Factory(lambda cfg: NetAtmoArchiveConfig(**cfg), config.sources.netatmo_archive)
    netatmo_archive_source = providers.Factory(NetAtmoArchiveSource, config=netatmo_archive_config)

    # ioncannon - stress tester
    ion_cannon_config = providers.Factory(lambda cfg: IonCannonConfig(**cfg), config.sources.ioncannon)
    ion_cannon_source = providers.Factory(IonCannonSource, config=ion_cannon_config)

    # ingestion service
    ingestion_service = providers.Factory(IngestionService, config=IngestionConfig(), timeseries_db=timeseries_db)

    # dataset aggregator service (deprecated - replaced by coordinator/builder)
    # dataset_aggregator_config = providers.Factory(lambda cfg: DatasetAggregatorConfig(**cfg), config.dataset_aggregator)
    # dataset_aggregator_service = providers.Factory(DatasetAggregatorService, config=dataset_aggregator_config, event_store=event_store, timeseries_db=timeseries_db)

    # dataset coordinator service
    # dataset_coordinator_config = providers.Factory(lambda cfg: DatasetCoordinatorConfig(), config.dataset_coordinator)
    dataset_coordinator_service = providers.Factory(DatasetCoordinatorService, config=DatasetCoordinatorConfig(), event_store=event_store)

    # dataset builder service
    # dataset_builder_config = providers.Factory(lambda cfg: DatasetBuilderConfig(**cfg), config.dataset_builder)
    dataset_builder_service = providers.Factory(DatasetBuilder, config=DatasetBuilderConfig(), event_store=event_store, timeseries_db=timeseries_db)

    # PyGeoAPI projection service
    pygeoapi_projection_service_config = providers.Factory(lambda cfg: PyGeoApiConfig(**cfg), config.projections.pygeoapi_service)
    pygeoapi_projection_service = providers.Factory(PyGeoApiProjectionService, config=pygeoapi_projection_service_config)

    # ODB projection service
    odb_projection_service_config = providers.Factory(lambda cfg: ODBProjectionServiceConfig(**cfg), config.projections.odb_service)
    odb_projection_service = providers.Factory(ODBProjectionService, config=odb_projection_service_config)
