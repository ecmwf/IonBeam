# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
import random
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import AsyncIterator, Awaitable, Callable, List, Optional, Set
from uuid import UUID, uuid4

import pyarrow as pa
import pyarrow.flight as flight
import structlog

from .config import IonbeamClientConfig
from .models import IngestDataCommand, IngestionMetadata
from .schema_metadata import SCHEMA_HASH

class IngestRejected(ValueError):
    """The stream can never be ingested as-is (contract violation, empty
    stream); retrying the same data is pointless."""


@dataclass(frozen=True)
class AvailableDataset:
    """A pushed announcement that a dataset build can be fetched.

    ``info`` is a server-minted :class:`flight.FlightInfo`; stream the data by
    redeeming every endpoint's ticket with ``connection.do_get``, in endpoint
    order. Structure and semantics travel in the streamed schema, readable via
    :mod:`ionbeam_client.schema_metadata`.

    A higher ``version`` of the same time range supersedes this build whenever
    it arrives. No revision is published at or after ``revisable_until``, so
    the build is immutable once the clock passes it.
    """

    id: UUID
    dataset: str
    start_time: datetime
    end_time: datetime
    version: int
    revisable_until: datetime
    info: flight.FlightInfo


# The trigger's command id is deterministic per (schedule, boundary); a handler
# that ingests under it (ingestion_id=trigger_id) makes a redelivered trigger
# replay as the same claims and records instead of fresh data.
TriggerHandler = Callable[[datetime, datetime, UUID], Awaitable[None]]
# The handler reads the canonical data it needs itself (via do_get on the
# connection), so it is handed the connection and the announcement, nothing
# pre-fetched.
ExportHandler = Callable[[flight.FlightClient, AvailableDataset], None]

_MAX_RETRY_DELAY = 60.0

# The client's side of a subscription exchange carries no record batches — only
# per-event completion acks as app-metadata frames (the Flight-native channel
# for application acknowledgements), so this schema is deliberately empty.
_ACK_SCHEMA = pa.schema([])

# Keepalive interval must exceed the server's 300s ping floor
# (min_recv_ping_interval_without_data): faster pings sever the connection
# mid-handler. max_pings_without_data=0 keeps pinging the quiet stream at all.
_GRPC_KEEPALIVE_OPTIONS = [
    ("grpc.keepalive_time_ms", 600_000),
    ("grpc.keepalive_timeout_ms", 20_000),
    ("grpc.http2.max_pings_without_data", 0),
]


class _FlightSubscription:
    """A dedicated Flight connection holding one DoExchange stream open,
    reconnecting until stopped."""

    def __init__(
        self,
        name: str,
        url: str,
        command: dict,
        on_batch: Callable[[flight.FlightClient, pa.RecordBatch], str],
        retry_delay: float,
        shutdown_timeout: float,
    ):
        self.logger = structlog.get_logger(__name__).bind(subscription=name)
        self._url = url
        self._descriptor = flight.FlightDescriptor.for_command(
            json.dumps(command).encode("utf-8")
        )
        self._on_batch = on_batch
        self._retry_delay = retry_delay
        self._shutdown_timeout = shutdown_timeout
        self._closing = threading.Event()
        self._connection: Optional[flight.FlightClient] = None
        self._reader: Optional[flight.FlightStreamReader] = None
        self._thread = threading.Thread(
            target=self._run, name=f"ionbeam-{name}", daemon=True
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        # Signal first so no new event is picked up, then cancel the read cursor to
        # unblock an idle subscription. Joins for the full grace window so a running
        # handler's in-flight work (an export, a fetch) completes; k8s SIGKILL at
        # grace end is the hard bound.
        self._closing.set()
        reader = self._reader
        if reader is not None:
            try:
                reader.cancel()
            except flight.FlightError:
                pass  # cancelling a stream that just died races shutdown
        self._thread.join(timeout=self._shutdown_timeout)
        if self._thread.is_alive():
            self.logger.warning(
                "Subscription handler still running at shutdown deadline",
                timeout=self._shutdown_timeout,
            )

    def _run(self) -> None:
        backoff = self._retry_delay
        while not self._closing.is_set():
            try:
                self._connection = flight.connect(
                    self._url, generic_options=_GRPC_KEEPALIVE_OPTIONS
                )
                self._stream(self._connection)
                backoff = self._retry_delay
            except Exception as exc:
                if self._closing.is_set():
                    return
                self.logger.warning(
                    "Subscription stream failed, reconnecting",
                    error=str(exc),
                    retry_in=backoff,
                )
            finally:
                self._reader = None
                connection, self._connection = self._connection, None
                if connection is not None:
                    try:
                        connection.close()
                    except flight.FlightError:
                        pass  # closing a connection whose stream just died can race
            # Full jitter, so replicas retrying a down server don't reconnect in lockstep.
            self._closing.wait(random.uniform(0, backoff))
            backoff = min(backoff * 2, _MAX_RETRY_DELAY)

    def _stream(self, connection: flight.FlightClient) -> None:
        writer, reader = connection.do_exchange(self._descriptor)
        self._reader = reader
        try:
            # begin() flushes the descriptor so the server starts pushing; the
            # writer then stays open to carry per-event completion acks back.
            writer.begin(_ACK_SCHEMA)
            self.logger.info("Subscription stream open")
            for chunk in reader:
                if self._closing.is_set():
                    return
                if chunk.data is None:
                    continue
                # A handler failure propagates and tears the stream down without
                # acking, so the server leaves the event pending for redelivery.
                # The handler returns the event id it completed; echoing it lets
                # the server verify the ack matches what it delivered.
                completed_id = self._on_batch(connection, chunk.data)
                writer.write_metadata(completed_id.encode())
        finally:
            try:
                writer.close()
            except flight.FlightError:
                pass  # the stream is already torn down when we were cancelled


class IonbeamClient:
    def __init__(self, config: IonbeamClientConfig):
        self.config = config
        self.logger = structlog.get_logger(__name__)

        self._flight: Optional[flight.FlightClient] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._subscriptions: List[_FlightSubscription] = []
        self._registered_datasets: dict[str, str] = {}
        self._connected = False
        # Names this process to the server's consumer group for the lifetime of
        # the client, across reconnects: a subscription that drops takes back
        # its own unacked events on reattach instead of waiting out the bus's
        # reclaim threshold. Replicas generate distinct ids and so never share
        # pending work.
        self._subscriber_id = uuid4().hex

    async def __aenter__(self) -> "IonbeamClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def connect(self) -> None:
        if self._connected:
            return

        self.logger.info("Connecting ionbeam client", url=self.config.flight_url)

        self._loop = asyncio.get_running_loop()
        self._flight = flight.connect(
            self.config.flight_url, generic_options=_GRPC_KEEPALIVE_OPTIONS
        )
        for subscription in self._subscriptions:
            subscription.start()

        self._connected = True
        self.logger.info("Ionbeam client connected")

    async def close(self) -> None:
        if not self._connected:
            return

        self.logger.info("Closing ionbeam client")

        for subscription in self._subscriptions:
            await asyncio.to_thread(subscription.stop)

        if self._flight is not None:
            await asyncio.to_thread(self._flight.close)
            self._flight = None

        self._connected = False
        self.logger.info("Ionbeam client closed")

    async def register_dataset(self, metadata: IngestionMetadata) -> None:
        if not self._connected:
            raise RuntimeError(
                "Client not connected. Use async context manager or call connect() first."
            )

        dataset_name = metadata.name
        schema_hash = metadata.schema_hash()
        if self._registered_datasets.get(dataset_name) == schema_hash:
            return

        action = flight.Action(
            "register_dataset",
            metadata.model_dump_json().encode("utf-8"),
        )
        try:
            results = await asyncio.to_thread(self._flight.do_action, action)
            await asyncio.to_thread(list, results)
        except flight.FlightError:
            self.logger.exception("Failed to register dataset", dataset=dataset_name)
            raise

        self._registered_datasets[dataset_name] = schema_hash
        self.logger.info(
            "Dataset registered",
            dataset=dataset_name,
            schema_hash=schema_hash,
        )

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

        await self.register_dataset(metadata)

        command_id = ingestion_id or uuid4()
        dataset_name = metadata.name

        self.logger.info(
            "Starting ingestion",
            command_id=str(command_id),
            dataset=dataset_name,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
        )

        descriptor = flight.FlightDescriptor.for_command(
            json.dumps(
                {
                    "op": "ingest",
                    "id": str(command_id),
                    "metadata": metadata.model_dump(mode="json"),
                    "start": start_time.isoformat(),
                    "end": end_time.isoformat(),
                }
            ).encode("utf-8")
        )

        writer = None
        reader = None
        response = None
        total_rows = 0

        try:
            async for batch in batch_stream:
                if writer is None:
                    self._check_stream_contract(batch.schema, metadata)
                    writer, reader = await asyncio.to_thread(
                        self._flight.do_put, descriptor, batch.schema
                    )
                await asyncio.to_thread(writer.write_batch, batch)
                total_rows += batch.num_rows

            if writer is None:
                self.logger.warning(
                    "No data written (empty stream)",
                    command_id=str(command_id),
                )
                raise IngestRejected("Cannot ingest empty data stream")

            await asyncio.to_thread(writer.done_writing)
            response = await asyncio.to_thread(reader.read)
        except flight.FlightError as e:
            self.logger.error(
                "Failed to ingest data stream",
                command_id=str(command_id),
                error=str(e),
            )
            raise
        finally:
            if writer is not None:
                try:
                    await asyncio.to_thread(writer.close)
                except flight.FlightError:
                    pass

        ingested_rows = json.loads(response.to_pybytes().decode("utf-8"))["rows"]

        self.logger.info(
            "Ingestion completed successfully",
            command_id=str(command_id),
            dataset=dataset_name,
            rows=total_rows,
            ingested_rows=ingested_rows,
        )

        return IngestDataCommand(
            id=command_id,
            metadata=metadata,
            start_time=start_time,
            end_time=end_time,
        )

    @staticmethod
    def _check_stream_contract(schema: pa.Schema, metadata: IngestionMetadata) -> None:
        """Fail fast at the source before opening a Flight stream the server will reject."""
        expected = metadata.dataset_schema.canonical_columns
        if list(schema.names) != expected:
            raise IngestRejected(
                f"stream columns {list(schema.names)} do not match the declared "
                f"canonical columns {expected}; batch frames with "
                "canonical_record_batches(frames, metadata)"
            )
        stamped = (schema.metadata or {}).get(SCHEMA_HASH.encode())
        if stamped != metadata.schema_hash().encode():
            raise IngestRejected(
                "stream schema is missing or carries a stale "
                f"{SCHEMA_HASH} stamp; batch frames with "
                "canonical_record_batches(frames, metadata)"
            )

    def register_trigger_handler(
        self,
        source_name: str,
        handler: TriggerHandler,
    ) -> None:
        if self._connected:
            raise RuntimeError(
                "Cannot register trigger handler after connecting. Call this before connect()."
            )

        bound_logger = self.logger.bind(component="trigger", source=source_name)
        bound_logger.info("Registering trigger handler")

        def on_batch(connection: flight.FlightClient, batch: pa.RecordBatch) -> str:
            command_id = batch.column("id")[0].as_py()
            start = batch.column("start")[0].as_py()
            end = batch.column("end")[0].as_py()

            bound_logger.info(
                "Received trigger command",
                start=start.isoformat(),
                end=end.isoformat(),
            )

            try:
                asyncio.run_coroutine_threadsafe(
                    handler(start, end, UUID(command_id)), self._loop
                ).result()
                bound_logger.info("Trigger handler completed successfully")
            except Exception:
                # Propagate: the subscription tears down unacked, so the bus
                # redelivers the trigger instead of losing the fetch.
                bound_logger.exception("Failed to handle trigger command")
                raise
            return command_id

        self._subscriptions.append(
            _FlightSubscription(
                name=f"triggers-{source_name}",
                url=self.config.flight_url,
                command={
                    "op": "await_triggers",
                    "source_name": source_name,
                    "subscriber": self._subscriber_id,
                },
                on_batch=on_batch,
                retry_delay=self.config.retry_delay,
                shutdown_timeout=self.config.shutdown_timeout,
            )
        )

    def register_export_handler(
        self,
        exporter_name: str,
        handler: ExportHandler,
        dataset_filter: Optional[Set[str]] = None,
    ) -> None:
        if self._connected:
            raise RuntimeError(
                "Cannot register export handler after connecting. Call this before connect()."
            )

        bound_logger = self.logger.bind(component="export", exporter=exporter_name)
        bound_logger.info(
            "Registering export handler",
            dataset_filter=sorted(dataset_filter) if dataset_filter else None,
        )

        def on_batch(connection: flight.FlightClient, batch: pa.RecordBatch) -> str:
            event = AvailableDataset(
                id=UUID(batch.column("id")[0].as_py()),
                dataset=batch.column("dataset")[0].as_py(),
                start_time=batch.column("start")[0].as_py(),
                end_time=batch.column("end")[0].as_py(),
                version=batch.column("version")[0].as_py(),
                revisable_until=batch.column("revisable_until")[0].as_py(),
                info=flight.FlightInfo.deserialize(batch.column("info")[0].as_py()),
            )

            bound_logger.info(
                "Received dataset available event",
                event_id=str(event.id),
                dataset=event.dataset,
                start=event.start_time.isoformat(),
                end=event.end_time.isoformat(),
                version=event.version,
                revisable_until=event.revisable_until.isoformat(),
            )

            try:
                handler(connection, event)
                bound_logger.info(
                    "Export handler completed successfully",
                    event_id=str(event.id),
                    dataset=event.dataset,
                )
            except Exception:
                # Propagate: the subscription tears down unacked, so the bus
                # redelivers the event instead of losing the export.
                bound_logger.exception("Failed to handle dataset available event")
                raise
            return str(event.id)

        command = {
            "op": "await_datasets",
            "exporter_name": exporter_name,
            "subscriber": self._subscriber_id,
        }
        if dataset_filter:
            command["datasets"] = sorted(dataset_filter)

        self._subscriptions.append(
            _FlightSubscription(
                name=f"datasets-{exporter_name}",
                url=self.config.flight_url,
                command=command,
                on_batch=on_batch,
                retry_delay=self.config.retry_delay,
                shutdown_timeout=self.config.shutdown_timeout,
            )
        )
