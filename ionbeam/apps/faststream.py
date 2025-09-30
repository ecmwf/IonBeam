import logging
import logging.handlers
from pathlib import Path

import structlog
from dependency_injector.wiring import Provide, inject
from faststream.asgi import AsgiFastStream
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from prometheus_client import CollectorRegistry, make_asgi_app
from structlog import dev as structlog_dev
from structlog.contextvars import merge_contextvars
from structlog.processors import TimeStamper
from structlog.stdlib import LoggerFactory, ProcessorFormatter

from ..core.containers import IonbeamContainer
from ..models.models import DataAvailableEvent, DataSetAvailableEvent, IngestDataCommand, StartSourceCommand
from ..projections.odb.projection_service import ODBProjectionService
from ..projections.pygeoapi.projection_service import PyGeoApiProjectionService
from ..scheduler.source_scheduler import SourceScheduler
from ..services.dataset_builder import DatasetBuilder
from ..services.dataset_coordinator import DatasetCoordinatorService
from ..services.ingestion import IngestionService
from ..sources.ioncannon import IonCannonSource
from ..sources.meteotracker import MeteoTrackerSource
from ..sources.metno.netatmo import NetAtmoSource
from ..sources.metno.netatmo_archive import NetAtmoArchiveSource
from ..sources.metno.netatmo_mqtt import NetAtmoMQTTSource
from ..sources.sensor_community import SensorCommunitySource


def setup_logging(level: int = logging.INFO, log_dir: Path = Path("."), log_name: str = "ionbeam.log") -> None:
    # Ensure log directory exists
    log_dir.mkdir(parents=True, exist_ok=True)

    # Shared processors that enrich stdlib (foreign) logs before rendering
    timestamper = TimeStamper(fmt="iso", utc=True)
    foreign_pre_chain = [
        merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.ExtraAdder(),
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # Handlers: pretty console + JSON file via ProcessorFormatter
    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(
        ProcessorFormatter(
            processors=[
                ProcessorFormatter.remove_processors_meta,
                structlog_dev.ConsoleRenderer(colors=True),
            ],
            foreign_pre_chain=foreign_pre_chain,
        )
    )

    file_handler = logging.handlers.WatchedFileHandler(str(log_dir / log_name))
    file_handler.setLevel(level)
    file_handler.setFormatter(
        ProcessorFormatter(
            processors=[
                ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(),
            ],
            foreign_pre_chain=foreign_pre_chain,
        )
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    root.addHandler(console)
    root.addHandler(file_handler)

    # structlog: build event dict, hand off rendering to ProcessorFormatter
    structlog.configure(
        processors=[
            merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.PositionalArgumentsFormatter(),
            timestamper,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

@inject
async def create_faststream_handlers(
    broker: RabbitBroker = Provide[IonbeamContainer.broker],
    netatmo_source: NetAtmoSource = Provide[IonbeamContainer.netatmo_source],
    ioncannon_source: IonCannonSource = Provide[IonbeamContainer.ion_cannon_source],
    sensor_community_source: SensorCommunitySource = Provide[IonbeamContainer.sensor_community_source],
    meteotracker_source: MeteoTrackerSource = Provide[IonbeamContainer.meteotracker_source],
    netatmo_archive_source: NetAtmoArchiveSource = Provide[IonbeamContainer.netatmo_archive_source],
    ingestion_service: IngestionService = Provide[IonbeamContainer.ingestion_service],
    dataset_coordinator_service: DatasetCoordinatorService = Provide[IonbeamContainer.dataset_coordinator_service],
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
    async def handle_dataset_coordination(event: DataAvailableEvent):
        await dataset_coordinator_service.handle(event)

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
    
    registry: CollectorRegistry = container.registry() # type: ignore
    broker: RabbitBroker = await container.broker() # type: ignore
    scheduler: SourceScheduler = await container.source_scheduler() # type: ignore
    netatmo_mqtt_source: NetAtmoMQTTSource = container.netatmo_mqtt_source() # type: ignore
    dataset_builder: DatasetBuilder = await container.dataset_builder_service() # type: ignore


    # app = FastStream(broker, logging.getLogger("faststream")).as_asgi(
    #     asyncapi_path="/docs/asyncapi",
    # )

    app = AsgiFastStream(
        broker,
        title="Ionbeam",
        version="0.1.0", # TODO
        description="An event-driven platform for stream based processing of IoT observations",
        asyncapi_path="/docs",
        asgi_routes=[
            ("/metrics", make_asgi_app(registry)),
        ]
    )
    # Logging is configured globally via structlog + logging. No per-logger overrides needed.

    @app.on_startup
    async def startup():
        """Start the source scheduler when the app starts"""
        scheduler.start()
        await dataset_builder.start()
        # await netatmo_mqtt_source.start()
    
    @app.on_shutdown
    async def shutdown():
        """Stop the source scheduler when the app shuts down"""
        await dataset_builder.stop()
        await scheduler.stop()
        shutdown = container.shutdown_resources()
        if(shutdown is not None):
            await shutdown
        # await netatmo_mqtt_source.stop()
    
    return app
