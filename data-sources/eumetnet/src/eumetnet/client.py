# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
import ssl
import time
from typing import Dict, List
from uuid import UUID, uuid4

import aiomqtt
import pandas as pd
import structlog
from ionbeam_client import IngestRejected, IonbeamClient
from ionbeam_client.models import IngestionMetadata

from .models import NetAtmoMQTTConfig
from .netatmo_processing import netatmo_record_batch_stream


class NetAtmoMQTTSource:
    def __init__(
        self,
        config: NetAtmoMQTTConfig,
        client: IonbeamClient,
        metadata: IngestionMetadata,
        topics: List[str],
    ):
        self.config = config
        self.client = client
        self.metadata = metadata
        self.topics = topics
        self._buffer: List[dict] = []
        # Drained batches whose ingest failed, keyed by a stable ingestion id so a
        # retry dedupes at the server instead of reading as new data. Retried each
        # cycle and drained on shutdown, so a SIGTERM never loses buffered records.
        self._pending: Dict[UUID, List[dict]] = {}
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._mqtt_task: asyncio.Task | None = None
        self._agg_task: asyncio.Task | None = None
        self.logger = structlog.get_logger(__name__)
        self._source_name = self.metadata.name
        self._last_flush = time.monotonic()
        self._identifier = self.config.client_id

    async def start(self):
        if self._mqtt_task and not self._mqtt_task.done():
            self.logger.warning("MQTT task called while already running; ignoring.")
            return
        if self._agg_task and not self._agg_task.done():
            self.logger.warning("sink task called while already running; ignoring.")
            return

        if self._stop.is_set():
            self._stop = asyncio.Event()

        self._mqtt_task = asyncio.create_task(self._listen())
        self._agg_task = asyncio.create_task(self._aggregate_and_publish())

    async def stop(self):
        self._stop.set()

        # Stop taking new messages, but let the aggregator finish its current flush
        # and exit rather than cancelling it — a cancel would drop the in-RAM buffer.
        if self._mqtt_task:
            self._mqtt_task.cancel()
        tasks = [t for t in (self._mqtt_task, self._agg_task) if t]
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for idx, res in enumerate(results):
                if isinstance(res, Exception) and not isinstance(
                    res, asyncio.CancelledError
                ):
                    self.logger.warning(
                        "Task raised during stop()",
                        task=str(tasks[idx]),
                        error=str(res),
                    )

        await self._final_flush()

        self._mqtt_task = None
        self._agg_task = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()

    async def _listen(self):
        tls_context = ssl.create_default_context() if self.config.use_tls else None
        while not self._stop.is_set():
            try:
                async with aiomqtt.Client(
                    hostname=self.config.host,
                    port=self.config.port,
                    username=self.config.username,
                    password=self.config.password,
                    identifier=self._identifier,
                    keepalive=self.config.keepalive,
                    tls_context=tls_context,
                    clean_session=False,
                ) as mqtt_client:
                    for topic in self.topics:
                        await mqtt_client.subscribe(topic, qos=1)
                    self.logger.info(
                        "Subscribed to Netatmo MQTT",
                        client_id=self._identifier,
                        topics=self.topics,
                        dataset=self._source_name,
                        qos=1,
                    )
                    async for msg in mqtt_client.messages:
                        if self._stop.is_set():
                            break
                        await self._handle_message(msg)
            except Exception as e:
                self.logger.warning("MQTT connection error", error=str(e), retry_in=5)
                await asyncio.sleep(5)

    async def _handle_message(self, msg):
        try:
            data = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            self.logger.error("unable to parse message", payload=msg.payload)
            return
        async with self._lock:
            if len(self._buffer) >= self.config.max_buffer_size:
                self.logger.warning(
                    "Buffer full; dropping message",
                    max_size=self.config.max_buffer_size,
                )
                return
            self._buffer.append(data)

    async def _aggregate_and_publish(self):
        try:
            while not self._stop.is_set():
                await self._retry_pending()

                elapsed = time.monotonic() - self._last_flush
                flush_due_to_size = len(self._buffer) >= self.config.flush_max_records
                flush_due_to_time = (len(self._buffer) > 0) and (
                    elapsed >= self.config.flush_interval_seconds
                )

                if not flush_due_to_size and not flush_due_to_time:
                    await self._sleep(
                        min(5, max(0, self.config.flush_interval_seconds - elapsed))
                    )
                    continue

                await self._drain_and_flush()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.error("Aggregation task failed", error=str(e))

    async def _sleep(self, seconds: float) -> None:
        """Sleep, but wake immediately once a shutdown is requested."""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def _drain_and_flush(self) -> None:
        # Hold the buffer back while a large retry backlog is unsent, so an outage
        # bounds memory at max_buffer_size (the live buffer's drop guard) instead
        # of growing _pending without limit.
        if sum(len(b) for b in self._pending.values()) >= self.config.max_buffer_size:
            await self._sleep(5)
            return

        async with self._lock:
            drained = self._buffer
            self._buffer = []

        if not drained:
            return

        # Record the batch as pending before flushing: a failed (or cancelled)
        # flush then leaves it for retry or the shutdown drain rather than losing it.
        ingestion_id = uuid4()
        self._pending[ingestion_id] = drained
        if await self._flush(drained, ingestion_id):
            self._pending.pop(ingestion_id, None)

    async def _retry_pending(self) -> None:
        for ingestion_id, drained in list(self._pending.items()):
            if self._stop.is_set():
                return
            if await self._flush(drained, ingestion_id):
                self._pending.pop(ingestion_id, None)
            else:
                break  # broker still unavailable; back off until the next cycle

    async def _final_flush(self) -> None:
        """Best-effort drain of the buffer and any pending batches on shutdown."""
        async with self._lock:
            drained = self._buffer
            self._buffer = []
        if drained:
            self._pending[uuid4()] = drained

        for ingestion_id, batch in list(self._pending.items()):
            if await self._flush(batch, ingestion_id):
                self._pending.pop(ingestion_id, None)

        if self._pending:
            unsent = sum(len(b) for b in self._pending.values())
            self.logger.error(
                "Shutdown flush incomplete; records unsent",
                dataset=self._source_name,
                records=unsent,
            )

    async def _flush(self, drained: List[dict], ingestion_id: UUID) -> bool:
        """Ingest one drained batch. Returns True when the data is durably sent
        (or is unparseable and can be dropped); False on a transient failure the
        caller should retry."""
        times = pd.to_datetime(
            [d.get("properties", {}).get("datetime") for d in drained],
            utc=True,
            format="ISO8601",
            errors="coerce",
        ).dropna()
        if times.empty:
            self.logger.warning("No valid datetimes in drained buffer; dropping")
            return True
        start_time = times.min().to_pydatetime()
        end_time = times.max().to_pydatetime()

        try:
            batch_stream = netatmo_record_batch_stream(
                drained,
                self.metadata,
                batch_size=self.config.flush_max_records,
                logger=self.logger,
            )
            await self.client.ingest(
                batch_stream=batch_stream,
                metadata=self.metadata,
                start_time=start_time,
                end_time=end_time,
                ingestion_id=ingestion_id,
            )
        except IngestRejected as e:
            # permanent: retrying can never succeed and would block every
            # batch behind it
            self.logger.error(
                "Batch rejected by the ingest client; dropping",
                dataset=self._source_name,
                records=len(drained),
                error=str(e),
            )
            return True
        except Exception as e:
            self.logger.error(
                "Failed to ingest data; will retry",
                dataset=self._source_name,
                error=str(e),
            )
            return False

        self._last_flush = time.monotonic()
        self.logger.info(
            "Ingested Netatmo data",
            dataset=self._source_name,
            start=start_time.isoformat(),
            end=end_time.isoformat(),
            records=len(drained),
        )
        return True
