"""AMQP infrastructure for ionbeam client.

This module contains all AMQP-related functionality:
- Publisher for ingestion commands
- Generic consumer for queues
- Message handlers for triggers and exports
"""

import asyncio
import re
from datetime import datetime
from typing import AsyncIterator, Awaitable, Callable, Optional, Set

import aio_pika
import pyarrow as pa
import structlog

from .models import DataSetAvailableEvent, IngestDataCommand, StartSourceCommand
from .transfer import ArrowStore

logger = structlog.get_logger(__name__)


# Public type aliases
TriggerHandler = Callable[[datetime, datetime], Awaitable[None]]
ExportHandler = Callable[
    [DataSetAvailableEvent, AsyncIterator[pa.RecordBatch]], Awaitable[None]
]
MessageHandler = Callable[[aio_pika.abc.AbstractIncomingMessage], Awaitable[None]]


class AMQPPublisher:
    """Publishes ingestion commands to AMQP."""

    def __init__(
        self,
        routing_key: str,
        exchange: str = "",
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        self.routing_key = routing_key
        self.exchange = exchange
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self.logger = logger.bind(component="publisher")
        self._channel: Optional[aio_pika.abc.AbstractChannel] = None

    async def set_channel(self, channel: aio_pika.abc.AbstractChannel) -> None:
        """Attach an already-open AMQP channel managed by the client."""
        self._channel = channel
        self.logger.info(
            "Configured publisher channel",
            routing_key=self.routing_key,
            exchange=self.exchange,
        )

    async def publish(self, command: IngestDataCommand) -> None:
        """Publish an ingestion command with retry logic."""
        if self._channel is None:
            raise RuntimeError(
                "Publisher channel not configured. Call set_channel() first."
            )

        message_body = command.model_dump_json().encode()

        for attempt in range(self.max_retries):
            try:
                # Get or declare the exchange
                exchange = await self._channel.get_exchange(self.exchange or "")
                
                await exchange.publish(
                    aio_pika.Message(
                        body=message_body,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=self.routing_key,
                )

                self.logger.info(
                    "Published ingestion command",
                    command_id=str(command.id),
                    payload_location=command.payload_location,
                    exchange=self.exchange,
                    routing_key=self.routing_key,
                )
                return

            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2**attempt)
                    self.logger.warning(
                        "Failed to publish, retrying",
                        error=str(e),
                        attempt=attempt + 1,
                        max_retries=self.max_retries,
                        retry_in=wait_time,
                    )
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(
                        "Failed to publish after all retries",
                        error=str(e),
                        command_id=str(command.id),
                        payload_location=command.payload_location,
                    )
                    raise

    async def close(self) -> None:
        """Release channel reference (channel is owned by client)."""
        self._channel = None


class AMQPConsumer:
    """Generic AMQP consumer that manages queue setup and consumption."""

    def __init__(
        self,
        queue_name: str,
        *,
        exchange_name: Optional[str] = None,
        exchange_type: aio_pika.ExchangeType = aio_pika.ExchangeType.FANOUT,
        prefetch_count: int = 1,
        durable: bool = True,
    ):
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.prefetch_count = prefetch_count
        self.durable = durable

        self.logger = logger.bind(component="consumer", queue=queue_name)
        self._channel: Optional[aio_pika.abc.AbstractChannel] = None
        self._queue: Optional[aio_pika.abc.AbstractQueue] = None
        self._consumer_tag: Optional[str] = None

    async def set_channel(self, channel: aio_pika.abc.AbstractChannel) -> None:
        """Attach an already-open AMQP channel managed by the client."""
        self._channel = channel
        await self._channel.set_qos(prefetch_count=self.prefetch_count)
        self.logger.debug("Configured consumer channel")

    async def start(self, handler: MessageHandler) -> None:
        """Start consuming messages from the queue."""
        if self._channel is None:
            raise RuntimeError(
                "Consumer channel not configured. Call set_channel() first."
            )

        try:
            # Declare exchange if specified
            if self.exchange_name:
                exchange = await self._channel.declare_exchange(
                    self.exchange_name,
                    self.exchange_type,
                    durable=self.durable,
                )
                self.logger.debug("Declared exchange", exchange=self.exchange_name)

            # Declare queue
            self._queue = await self._channel.declare_queue(
                self.queue_name,
                durable=self.durable,
            )
            self.logger.debug("Declared queue")

            # Bind queue to exchange if specified
            if self.exchange_name:
                await self._queue.bind(exchange)
                self.logger.debug("Bound queue to exchange")

            # Start consuming
            self._consumer_tag = await self._queue.consume(handler)

            self.logger.info(
                "Started consuming messages",
                exchange=self.exchange_name,
                prefetch=self.prefetch_count,
            )

        except Exception as e:
            self.logger.error("Failed to start consumer", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop consuming messages and clean up."""
        if self._consumer_tag and self._queue:
            try:
                await self._queue.cancel(self._consumer_tag)
                self.logger.debug("Cancelled consumer")
            except Exception as e:
                self.logger.warning("Error cancelling consumer", error=str(e))
            finally:
                self._consumer_tag = None

        # Clear references (but don't close channel - that's owned by client)
        self._queue = None
        self._channel = None
        self.logger.info("Stopped consumer")


def create_trigger_message_handler(
    handler: TriggerHandler,
    source_name: str,
) -> MessageHandler:
    """Create an AMQP message handler that processes trigger commands.

    Args:
        handler: User-provided trigger handler function
        source_name: Name of the source for logging

    Returns:
        Async function that processes AMQP messages
    """
    bound_logger = logger.bind(component="trigger", source=source_name)

    async def handle_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
        async with message.process():
            try:
                command = StartSourceCommand.model_validate_json(message.body)

                bound_logger.info(
                    "Received trigger command",
                    command_id=str(command.id),
                    start=command.start_time.isoformat(),
                    end=command.end_time.isoformat(),
                )

                await handler(command.start_time, command.end_time)

                bound_logger.info(
                    "Trigger handler completed successfully",
                    command_id=str(command.id),
                )

            except Exception:
                bound_logger.exception("Failed to handle trigger command")
                raise

    return handle_message


def create_export_message_handler(
    handler: ExportHandler,
    arrow_store: ArrowStore,
    exporter_name: str,
    *,
    dataset_filter: Optional[Set[str]] = None,
    batch_size: int = 65536,
) -> MessageHandler:
    """Create an AMQP message handler that processes dataset export events.

    Args:
        handler: User-provided export handler function
        arrow_store: Arrow store for reading dataset files
        exporter_name: Name of the exporter for logging
        dataset_filter: Optional set of dataset names to process
        batch_size: Batch size for reading record batches

    Returns:
        Async function that processes AMQP messages
    """
    bound_logger = logger.bind(component="export", exporter=exporter_name)
    if dataset_filter:
        bound_logger = bound_logger.bind(dataset_filter=sorted(dataset_filter))

    def should_process_dataset(dataset_name: str) -> bool:
        if dataset_filter is None:
            return True
        return dataset_name in dataset_filter

    async def handle_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
        async with message.process():
            try:
                event = DataSetAvailableEvent.model_validate_json(message.body)

                dataset_name = event.metadata.name

                if not should_process_dataset(dataset_name):
                    bound_logger.debug(
                        "Skipping dataset (filtered)",
                        event_id=str(event.id),
                        dataset=dataset_name,
                    )
                    return

                bound_logger.info(
                    "Received dataset available event",
                    event_id=str(event.id),
                    dataset=dataset_name,
                    location=event.dataset_location,
                    start=event.start_time.isoformat(),
                    end=event.end_time.isoformat(),
                )

                batch_stream = arrow_store.read_record_batches(
                    event.dataset_location, batch_size=batch_size
                )

                await handler(event, batch_stream)

                bound_logger.info(
                    "Export handler completed successfully",
                    event_id=str(event.id),
                    dataset=dataset_name,
                )

            except Exception:
                bound_logger.exception("Failed to handle dataset available event")
                raise

    return handle_message


def get_trigger_queue_name(source_name: str) -> str:
    """Generate standardized queue name for a trigger source."""
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", source_name.strip())
    return f"ionbeam.source.{sanitized}.start"


def get_export_queue_name(exporter_name: str) -> str:
    """Generate standardized queue name for an exporter."""
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", exporter_name.strip())
    return f"ionbeam.dataset.available.{sanitized}"
