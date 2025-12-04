import logging
from pathlib import Path

from dependency_injector.wiring import Provide, inject
from faststream.asgi import AsgiFastStream
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from ionbeam_client.models import DataAvailableEvent, IngestDataCommand
from prometheus_client import CollectorRegistry, make_asgi_app

from ionbeam.container import IonbeamCoreContainer
from ionbeam.handlers import (
    DatasetBuilderHandler,
    DatasetCoordinatorHandler,
    IngestionHandler,
)
from ionbeam.observability.logging import setup_logging
from ionbeam.scheduler import SourceScheduler


@inject
async def create_message_handlers(
    broker: RabbitBroker = Provide[IonbeamCoreContainer.broker],
    ingestion_handler: IngestionHandler = Provide[
        IonbeamCoreContainer.ingestion_handler
    ],
    dataset_coordinator_handler: DatasetCoordinatorHandler = Provide[
        IonbeamCoreContainer.dataset_coordinator_handler
    ],
):
    ingestion_command_exchange = RabbitExchange(
        "ionbeam.ingestion",
        type=ExchangeType.DIRECT,
        durable=True,
    )

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

    ingestion_q = RabbitQueue(
        "ionbeam.ingestion.ingestV1",
        durable=True,
        routing_key="ingestV1",
    )

    aggregation_q = RabbitQueue(
        "ionbeam.data.available.aggregation",
        durable=True,
    )

    @broker.subscriber(ingestion_q, ingestion_command_exchange)
    async def handle_ingestion(command: IngestDataCommand) -> None:
        event: DataAvailableEvent = await ingestion_handler.handle(command)
        await broker.publish(event, exchange=ingestion_fanout)

    @broker.subscriber(aggregation_q, ingestion_fanout)
    async def handle_dataset_coordination(event: DataAvailableEvent):
        await dataset_coordinator_handler.handle(event)

    broker.publisher(exchange=dataset_fanout)


async def factory(with_builder: bool = False, with_scheduler: bool = True):
    container = IonbeamCoreContainer()

    log_config = container.config.logging()
    log_level = getattr(logging, log_config.get("level", "INFO"))
    log_dir = Path(log_config.get("log_dir", "./logs"))
    log_name = log_config.get("log_name", "ionbeam.log")
    setup_logging(level=log_level, log_dir=log_dir, log_name=log_name)
    container.wire(modules=[__name__])
    init = container.init_resources()
    if init is not None:
        await init

    await create_message_handlers()

    broker: RabbitBroker = await container.broker()  # type: ignore
    registry: CollectorRegistry = container.registry()

    dataset_builder: DatasetBuilderHandler | None = None
    if with_builder:
        dataset_builder = await container.dataset_builder_handler()  # type: ignore

    scheduler: SourceScheduler | None = None
    if with_scheduler:
        scheduler = await container.source_scheduler()  # type: ignore

    app = AsgiFastStream(
        broker,
        title="Ionbeam",
        version="0.1.0",
        description="Event-driven IoT data ingestion and aggregation platform",
        asyncapi_path="/docs",
        asgi_routes=[
            ("/metrics", make_asgi_app(registry)),
        ],
    )

    @app.on_startup
    async def startup():
        if dataset_builder:
            await dataset_builder.start()
        if scheduler:
            scheduler.start()

    @app.on_shutdown
    async def shutdown():
        if scheduler:
            await scheduler.stop()
        if dataset_builder:
            await dataset_builder.stop()
        shutdown_resources = container.shutdown_resources()
        if shutdown_resources is not None:
            await shutdown_resources

    return app
