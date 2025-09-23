import asyncio
import json
import structlog
import ssl
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List
from uuid import uuid4

import aio_pika
import aiomqtt
import pandas as pd
from pydantic import BaseModel

from ionbeam.models.models import IngestDataCommand

from .netatmo import netatmo_metadata
from .netatmo_processing import process_netatmo_geojson_messages_to_df, netatmo_dataframe_stream
from ionbeam.utilities.parquet_tools import stream_dataframes_to_parquet


class NetAtmoMQTTConfig(BaseModel):
    host: str
    port: int = 8883
    username: str
    password: str
    client_id: str
    keepalive: int = 120
    use_tls: bool = True
    data_path: Path
    url: str
    routing_key: str = "ionbeam.ingestion.ingestV1"
    flush_interval_seconds: int = 60


class NetAtmoMQTTSource:
    def __init__(self, config: NetAtmoMQTTConfig):
        self.config = config
        self._buffer: List[dict] = []
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._mqtt_task: asyncio.Task | None = None
        self._agg_task: asyncio.Task | None = None
        self.logger = structlog.get_logger(__name__)
        self.metadata = netatmo_metadata
        self._last_flush = time.monotonic()
        
    async def start(self):
        self._mqtt_task = asyncio.create_task(self._listen())
        self._agg_task = asyncio.create_task(self._aggregate_and_publish())

    async def stop(self):
        self._stop.set()
        if self._mqtt_task:
            self._mqtt_task.cancel()
            try:
                await self._mqtt_task
            except asyncio.CancelledError:
                pass
        if self._agg_task:
            self._agg_task.cancel()
            await self._agg_task
        
    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self):
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
                    identifier=self.config.client_id,
                    keepalive=self.config.keepalive,
                    tls_context=tls_context,
                ) as client:
                    await client.subscribe("raw-obs/+/netatmo/#")
                    self.logger.info("Subscribed to Netatmo MQTT")
                    async for msg in client.messages:
                        if self._stop.is_set():
                            break
                        # self.logger.info("Handling %s", msg)
                        await self._handle_message(msg)
            except Exception as e:
                self.logger.warning("MQTT connection error", error=str(e), retry_in=5)
                await asyncio.sleep(5)

    async def _handle_message(self, msg):
        try:
            data = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            return
        async with self._lock:
            self._buffer.append(data)

    async def _aggregate_and_publish(self):
        while not self._stop.is_set():
            flush_due_to_size = len(self._buffer) >= 50000
            flush_due_to_time = (len(self._buffer) > 0) and ((time.monotonic() - self._last_flush) >= self.config.flush_interval_seconds)

            if not flush_due_to_size and not flush_due_to_time:
                await asyncio.sleep(5)  # idle wait
                continue

            async with self._lock:
                drained = self._buffer
                self._buffer = []

            if not drained:
                await asyncio.sleep(1)
                continue

            # Determine time bounds from drained buffer without materializing full DataFrame
            times = pd.to_datetime(
                [d.get("properties", {}).get("datetime") for d in drained],
                utc=True,
                errors="coerce",
            ).dropna()
            if times.empty:
                self.logger.warning("No valid datetimes in drained buffer")
                continue
            start_time = times.min()
            end_time = times.max()

            # Save to file using streaming writer
            self.config.data_path.mkdir(parents=True, exist_ok=True)
            path = self.config.data_path / f"{self.metadata.dataset.name}_{start_time}-{end_time}_{datetime.now(timezone.utc)}.parquet"

            async def drained_stream():
                for obj in drained:
                    yield obj

            try:
                df_stream = netatmo_dataframe_stream(drained_stream(), batch_size=50000, logger=self.logger)
                rows = await stream_dataframes_to_parquet(df_stream, path, schema_fields=None)
                self.logger.info("Wrote parquet file", rows=rows, path=str(path), start=start_time.isoformat(), end=end_time.isoformat())
            except Exception as e:
                self.logger.error("Failed to write parquet file", error=str(e), path=str(path))
                continue

            ingestion_command = IngestDataCommand(
                id=uuid4(),
                metadata=self.metadata,
                payload_location=path,
                start_time=start_time,
                end_time=end_time,
            )

            try:
                connection = await aio_pika.connect_robust(self.config.url)
                async with connection:
                    channel = await connection.channel()
                    exchange = channel.default_exchange
                    await exchange.publish(
                        aio_pika.Message(body=ingestion_command.model_dump_json().encode()),
                        routing_key=self.config.routing_key,
                    )
            except Exception as e:
                self.logger.error("AMQP publish error", error=str(e))
            finally:
                self._last_flush = time.monotonic()

    def process_message_buffer_to_df(self, buffer: List[dict]):
        # Delegates to shared processing utility for reuse across tools.
        return process_netatmo_geojson_messages_to_df(buffer, self.logger)
