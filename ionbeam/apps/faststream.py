import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import structlog
from structlog.stdlib import ProcessorFormatter, LoggerFactory
from structlog.processors import TimeStamper, JSONRenderer
from structlog.contextvars import merge_contextvars
from structlog import dev as structlog_dev

from dependency_injector.wiring import Provide, inject
from faststream import FastStream
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from ..core.containers import IonbeamContainer
from ..models.models import DataAvailableEvent, DataSetAvailableEvent, IngestDataCommand, StartSourceCommand
from ..projections.odb.projection_service import ODBProjectionService
from ..projections.pygeoapi.projection_service import PyGeoApiProjectionService
from ..scheduler.source_scheduler import SourceScheduler
from ..services.dataset_aggregation import DatasetAggregatorService
from ..services.ingestion import IngestionService
from ..sources.ioncannon import IonCannonSource
from ..sources.meteotracker import MeteoTrackerSource
from ..sources.metno.netatmo import NetAtmoSource
from ..sources.metno.netatmo_archive import NetAtmoArchiveSource
from ..sources.metno.netatmo_mqtt import NetAtmoMQTTSource
from ..sources.sensor_community import SensorCommunitySource

dataset_available_exchange = RabbitExchange("ionbeam.dataset.available", type=ExchangeType.FANOUT)

def setup_logging(level: int = logging.INFO, log_dir: Path = Path("."), log_name: str = "ionbeam.log") -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # structlog + stdlib integration
    pre_chain = [
        merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        TimeStamper(fmt="iso", utc=True),
    ]

    # Console: pretty, human-friendly
    console_formatter = ProcessorFormatter(
        processor=structlog_dev.ConsoleRenderer(colors=True),
        foreign_pre_chain=pre_chain,
    )

    # File: JSON for machine processing
    json_formatter = ProcessorFormatter(
        processor=JSONRenderer(),
        foreign_pre_chain=pre_chain,
    )

    # Console handler (stdout/stderr)
    if not any(getattr(h, "name", None) == "console" for h in root_logger.handlers):
        console = logging.StreamHandler()
        console.set_name("console")
        console.setLevel(level)
        console.setFormatter(console_formatter)
        root_logger.addHandler(console)

    # File handler (rotates nightly)
    log_dir.mkdir(parents=True, exist_ok=True)
    if not any(getattr(h, "name", None) == "file" for h in root_logger.handlers):
        file_handler = TimedRotatingFileHandler(log_dir / log_name, when="midnight", backupCount=5, encoding="utf-8")
        file_handler.set_name("file")
        file_handler.setLevel(level)
        file_handler.setFormatter(json_formatter)
        root_logger.addHandler(file_handler)

    # Configure structlog to hand off to ProcessorFormatter
    structlog.configure(
        processors=[
            merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            TimeStamper(fmt="iso", utc=True),
            ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Align third-party loggers
    logging.getLogger("httpx").setLevel(level)

@inject
async def create_faststream_handlers(
    broker: RabbitBroker = Provide[IonbeamContainer.broker],
    netatmo_source: NetAtmoSource = Provide[IonbeamContainer.netatmo_source],
    ioncannon_source: IonCannonSource = Provide[IonbeamContainer.ion_cannon_source],
    sensor_community_source: SensorCommunitySource = Provide[IonbeamContainer.sensor_community_source],
    meteotracker_source: MeteoTrackerSource = Provide[IonbeamContainer.meteotracker_source],
    netatmo_archive_source: NetAtmoArchiveSource = Provide[IonbeamContainer.netatmo_archive_source],
    ingestion_service: IngestionService = Provide[IonbeamContainer.ingestion_service],
    dataset_aggregation_service: DatasetAggregatorService = Provide[IonbeamContainer.dataset_aggregator_service],
    pygeoapi_projection_service: PyGeoApiProjectionService = Provide[IonbeamContainer.pygeoapi_projection_service],
    odb_projection_service: ODBProjectionService = Provide[IonbeamContainer.odb_projection_service],
):
    ingestion_fanout = RabbitExchange(
        "ionbeam.data.available",
        type=ExchangeType.FANOUT,
        durable=True,
    )
    dataset_fanout = RabbitExchange(
        "ionbeam.dataset.available",
        type=ExchangeType.FANOUT,
        durable=True,
    )
    aggregation_q = RabbitQueue(
        "ionbeam.data.available.aggregation",
        durable=True,
    )
    pygeoapi_q = RabbitQueue(
        "ionbeam.dataset.available.pygeoapi",
        durable=True,
    )
    odb_q = RabbitQueue(
        "ionbeam.dataset.available.odb",
        durable=True,
    )

    @broker.subscriber("ionbeam.source.netatmo.start")
    async def handle_netatmo(command: StartSourceCommand):
        if (result := await netatmo_source.handle(command)):
            await broker.publish(result, "ionbeam.ingestion.ingestV1")

    @broker.subscriber("ionbeam.source.ioncannon.start")
    async def handle_ioncannon(command: StartSourceCommand):
        if (result := await ioncannon_source.handle(command)):
            await broker.publish(result, "ionbeam.ingestion.ingestV1")

    @broker.subscriber("ionbeam.source.sensor_community.start")
    async def handle_sensor_community(command: StartSourceCommand):
        if (result := await sensor_community_source.handle(command)):
            await broker.publish(result, "ionbeam.ingestion.ingestV1")

    @broker.subscriber("ionbeam.source.meteotracker.start")
    async def handle_meteotracker(command: StartSourceCommand):
        if (result := await meteotracker_source.handle(command)):
            await broker.publish(result, "ionbeam.ingestion.ingestV1")

    @broker.subscriber("ionbeam.source.netatmo_archive.start")
    async def handle_netatmo_archive(command: StartSourceCommand):
        if (result := await netatmo_archive_source.handle(command)):
            await broker.publish(result, "ionbeam.ingestion.ingestV1")

    @broker.subscriber("ionbeam.ingestion.ingestV1")
    async def handle_ingestion(command: IngestDataCommand) -> None:
        event: DataAvailableEvent = await ingestion_service.handle(command)
        await broker.publish(event, exchange=ingestion_fanout)

    @broker.subscriber(aggregation_q, ingestion_fanout)
    async def handle_dataset_aggregation(event: DataAvailableEvent):
        for dataset_event in await dataset_aggregation_service.handle(event):
            await broker.publish(dataset_event, exchange=dataset_fanout)

    @broker.subscriber(pygeoapi_q, dataset_fanout)
    async def handle_pygeoapi_projection(event: DataSetAvailableEvent):
        await pygeoapi_projection_service.handle(event)

    @broker.subscriber(odb_q, dataset_fanout)
    async def handle_odb_projection(event: DataSetAvailableEvent):
        await odb_projection_service.handle(event)

async def factory():
    setup_logging()
    
    container = IonbeamContainer()
    container.wire(modules=["ionbeam.apps.faststream"])
    
    init = container.init_resources()
    if(init is not None):
        await init
    await create_faststream_handlers()
    
    broker: RabbitBroker = await container.broker() # type: ignore
    scheduler: SourceScheduler = await container.source_scheduler() # type: ignore
    netatmo_mqtt_source: NetAtmoMQTTSource = container.netatmo_mqtt_source() # type: ignore


    app = FastStream(broker)

    @app.on_startup
    async def startup():
        """Start the source scheduler when the app starts"""
        scheduler.start()
        await netatmo_mqtt_source.start()
    
    @app.on_shutdown
    async def shutdown():
        """Stop the source scheduler when the app shuts down"""
        await scheduler.stop()
        shutdown = container.shutdown_resources()
        if(shutdown is not None):
            await shutdown
        await netatmo_mqtt_source.stop()
    
    return app
