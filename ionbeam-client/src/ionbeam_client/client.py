from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator, List, Optional, Set
from uuid import UUID, uuid4

import aio_pika
import pyarrow as pa
import structlog

from .amqp import (
    AMQPConsumer,
    AMQPPublisher,
    ExportHandler,
    TriggerHandler,
    create_export_message_handler,
    create_trigger_message_handler,
    get_export_queue_name,
    get_trigger_queue_name,
)
from .arrow_tools import schema_from_ingestion_map
from .config import IonbeamClientConfig, _ARROW_STORE_PATH, _INGESTION_EXCHANGE, _INGESTION_ROUTING_KEY
from .models import IngestDataCommand, IngestionMetadata
from .transfer import ArrowStore, LocalFileSystemStore


class IonbeamClient:
    def __init__(self, config: IonbeamClientConfig):
        self.config = config or IonbeamClientConfig()
        self.logger = structlog.get_logger(__name__)

        # Initialize Arrow store using internal configuration
        # Currently uses local filesystem, will migrate to S3-compatible object store
        self._arrow_store: ArrowStore = LocalFileSystemStore(
            base_path=Path(_ARROW_STORE_PATH)
        )

        # Initialize publisher (will be configured with channel on connect)
        self._publisher = AMQPPublisher(
            routing_key=_INGESTION_ROUTING_KEY,
            exchange=_INGESTION_EXCHANGE,
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
        )

        # Shared AMQP connection and channel
        self._amqp_connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self._amqp_channel: Optional[aio_pika.abc.AbstractChannel] = None

        # Consumers list
        self._consumers: List[AMQPConsumer] = []

        self._connected = False

    async def __aenter__(self) -> "IonbeamClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def connect(self) -> None:
        if self._connected:
            return

        self.logger.info("Connecting ionbeam client", url=self.config.amqp_url)

        try:
            # Create shared AMQP connection
            self._amqp_connection = await aio_pika.connect_robust(
                self.config.amqp_url,
                timeout=self.config.connection_timeout,
            )
            self.logger.info("Established AMQP connection")

            # Create single shared channel for publisher and all consumers
            self._amqp_channel = await self._amqp_connection.channel()
            self.logger.debug("Created shared AMQP channel")

            # Configure publisher with the shared channel
            await self._publisher.set_channel(self._amqp_channel)

            # Configure and start all consumers with the shared channel
            for consumer in self._consumers:
                await consumer.set_channel(self._amqp_channel)
                # Call the stored setup handler to start consuming
                if hasattr(consumer, "_setup_handler"):
                    await consumer._setup_handler()  # type: ignore

            self._connected = True
            self.logger.info("Ionbeam client connected")

        except Exception as e:
            self.logger.error("Failed to connect ionbeam client", error=str(e))
            raise

    async def close(self) -> None:
        if not self._connected:
            return

        self.logger.info("Closing ionbeam client")

        # Stop all consumers
        for consumer in self._consumers:
            await consumer.stop()

        # Release publisher channel reference
        await self._publisher.close()

        # Close shared channel
        if self._amqp_channel:
            try:
                await self._amqp_channel.close()
                self.logger.debug("Closed shared AMQP channel")
            except Exception as e:
                self.logger.warning("Error closing AMQP channel", error=str(e))
            finally:
                self._amqp_channel = None

        # Close shared connection
        if self._amqp_connection:
            try:
                await self._amqp_connection.close()
                self.logger.info("Closed AMQP connection")
            except Exception as e:
                self.logger.warning("Error closing AMQP connection", error=str(e))
            finally:
                self._amqp_connection = None

        self._connected = False
        self.logger.info("Ionbeam client closed")

    def _generate_object_key(
        self,
        dataset_name: str,
        start_time: datetime,
        end_time: datetime,
    ) -> str:
        def format_timestamp(dt: datetime) -> str:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.strftime("%Y%m%dT%H%M%SZ")

        start_s = format_timestamp(start_time)
        end_s = format_timestamp(end_time)
        now_s = format_timestamp(datetime.now(timezone.utc))

        return f"raw/{dataset_name}/{start_s}-{end_s}_{now_s}"

    async def ingest(
        self,
        batch_stream: AsyncIterator[pa.RecordBatch],
        metadata: IngestionMetadata,
        start_time: datetime,
        end_time: datetime,
        *,
        ingestion_id: Optional[UUID] = None,
    ) -> IngestDataCommand:
        if not self._connected:
            raise RuntimeError(
                "Client not connected. Use async context manager or call connect() first."
            )

        command_id = ingestion_id or uuid4()
        dataset_name = metadata.dataset.name

        self.logger.info(
            "Starting ingestion",
            command_id=str(command_id),
            dataset=dataset_name,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
        )

        object_key = self._generate_object_key(dataset_name, start_time, end_time)

        schema = schema_from_ingestion_map(metadata.ingestion_map)

        try:
            total_rows = await self._arrow_store.write_record_batches(
                key=object_key,
                batch_stream=batch_stream,
                schema=schema,
                overwrite=False,
            )

            self.logger.info(
                "Wrote data to arrow store",
                command_id=str(command_id),
                key=object_key,
                rows=total_rows,
            )

            if total_rows == 0:
                self.logger.warning(
                    "No data written (empty stream)",
                    command_id=str(command_id),
                )
                raise ValueError("Cannot ingest empty data stream")

        except Exception as e:
            self.logger.error(
                "Failed to write data to arrow store",
                command_id=str(command_id),
                error=str(e),
            )
            raise

        command = IngestDataCommand(
            id=command_id,
            metadata=metadata,
            payload_location=object_key,
            start_time=start_time,
            end_time=end_time,
        )

        try:
            await self._publisher.publish(command)

            self.logger.info(
                "Ingestion completed successfully",
                command_id=str(command_id),
                dataset=dataset_name,
                rows=total_rows,
            )

        except Exception as e:
            self.logger.error(
                "Failed to publish ingestion command",
                command_id=str(command_id),
                payload_location=object_key,
                error=str(e),
            )
            self.logger.error(
                "Data written but not published - manual recovery required",
                payload_location=object_key,
            )
            raise

        return command

    def register_trigger_handler(
        self,
        source_name: str,
        handler: TriggerHandler,
    ) -> None:
        if self._connected:
            raise RuntimeError(
                "Cannot register trigger handler after connecting. Call this before connect()."
            )

        # Generate queue name for this trigger source
        queue_name = get_trigger_queue_name(source_name)

        self.logger.info(
            "Registering trigger handler",
            source=source_name,
            queue=queue_name,
        )

        # Create consumer for this trigger queue
        consumer = AMQPConsumer(
            queue_name=queue_name,
            prefetch_count=1,
        )
        self._consumers.append(consumer)

        # Store handler setup for later use during connect()
        async def setup_handler():
            message_handler = create_trigger_message_handler(handler, source_name)
            await consumer.start(message_handler)

        # Store the setup coroutine to be called after set_channel during connect
        consumer._setup_handler = setup_handler  # type: ignore

    def register_export_handler(
        self,
        exporter_name: str,
        handler: ExportHandler,
        dataset_filter: Optional[Set[str]] = None,
        batch_size: Optional[int] = None,
    ) -> None:
        if self._connected:
            raise RuntimeError(
                "Cannot register export handler after connecting. Call this before connect()."
            )

        # Generate queue and exchange names for this exporter
        queue_name = get_export_queue_name(exporter_name)
        exchange_name = "ionbeam.dataset.available"

        self.logger.info(
            "Registering export handler",
            exporter=exporter_name,
            queue=queue_name,
            exchange=exchange_name,
            dataset_filter=sorted(dataset_filter) if dataset_filter else None,
        )

        # Create consumer for this export queue
        consumer = AMQPConsumer(
            queue_name=queue_name,
            exchange_name=exchange_name,
            exchange_type=aio_pika.ExchangeType.FANOUT,
            prefetch_count=1,
        )
        self._consumers.append(consumer)

        # Store handler setup for later use during connect()
        async def setup_handler():
            message_handler = create_export_message_handler(
                handler,
                self._arrow_store,
                exporter_name,
                dataset_filter=dataset_filter,
                batch_size=batch_size or self.config.write_batch_size,
            )
            await consumer.start(message_handler)

        # Store the setup coroutine to be called after set_channel during connect
        consumer._setup_handler = setup_handler  # type: ignore


async def ingest(
    batch_stream: AsyncIterator[pa.RecordBatch],
    metadata: IngestionMetadata,
    start_time: datetime,
    end_time: datetime,
    config: IonbeamClientConfig,
) -> IngestDataCommand:
    async with IonbeamClient(config) as client:
        return await client.ingest(
            batch_stream=batch_stream,
            metadata=metadata,
            start_time=start_time,
            end_time=end_time,
        )
