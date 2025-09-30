import asyncio
import json
import ssl
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List
from uuid import uuid4

import aio_pika
import aiomqtt
import pandas as pd
import structlog
from pydantic import BaseModel

from ionbeam.models.models import IngestDataCommand
from ionbeam.observability.metrics import IonbeamMetricsProtocol
from ionbeam.utilities.parquet_tools import stream_dataframes_to_parquet

from .netatmo import netatmo_metadata
from .netatmo_processing import netatmo_dataframe_stream


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
    flush_max_records: int = 50000
    max_buffer_size: int = 200000


class NetAtmoMQTTSource:
    def __init__(self, config: NetAtmoMQTTConfig, metrics: IonbeamMetricsProtocol):
        self.config = config
        self._buffer: List[dict] = []
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._mqtt_task: asyncio.Task | None = None
        self._agg_task: asyncio.Task | None = None
        self.logger = structlog.get_logger(__name__)
        self.metadata = netatmo_metadata
        self.metrics = metrics
        self._source_name = self.metadata.dataset.name
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

        tasks = [t for t in (self._mqtt_task, self._agg_task) if t]
        for t in tasks:
            t.cancel()

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for idx, res in enumerate(results):
                if isinstance(res, Exception) and not isinstance(res, asyncio.CancelledError):
                    self.logger.warning("Task raised during stop()", task=str(tasks[idx]), error=str(res))

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
                ) as client:
                    await client.subscribe("raw-obs/+/netatmo/#", qos=1)
                    self.logger.info("Subscribed to Netatmo MQTT", client_id=self._identifier, qos=1)
                    async for msg in client.messages:
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
                self.logger.warning("Buffer full; dropping message", max_size=self.config.max_buffer_size)
                return
            self._buffer.append(data)

    @staticmethod
    def _ts_for_path(dt) -> str:
        if isinstance(dt, pd.Timestamp):
            dt = dt.to_pydatetime()
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y%m%dT%H%M%SZ")

    async def _aggregate_and_publish(self):
        try:
            connection = await aio_pika.connect_robust(self.config.url)
            async with connection:
                channel = await connection.channel()
                source = self._source_name

                while not self._stop.is_set():
                    size_threshold = self.config.flush_max_records
                    elapsed = time.monotonic() - self._last_flush
                    flush_due_to_size = len(self._buffer) >= size_threshold
                    flush_due_to_time = (len(self._buffer) > 0) and (elapsed >= self.config.flush_interval_seconds)

                    if not flush_due_to_size and not flush_due_to_time:
                        sleep_for = min(5, max(0, self.config.flush_interval_seconds - elapsed))
                        await asyncio.sleep(sleep_for)
                        continue

                    async with self._lock:
                        drained = self._buffer
                        self._buffer = []

                    if not drained:
                        await asyncio.sleep(1)
                        continue

                    times = pd.to_datetime(
                        [d.get("properties", {}).get("datetime") for d in drained],
                        utc=True,
                        errors="coerce",
                    ).dropna()
                    if times.empty:
                        self.logger.warning("No valid datetimes in drained buffer")
                        continue
                    start_time = times.min().to_pydatetime()
                    end_time = times.max().to_pydatetime()

                    self.config.data_path.mkdir(parents=True, exist_ok=True)
                    start_s = self._ts_for_path(start_time)
                    end_s = self._ts_for_path(end_time)
                    now_s = self._ts_for_path(datetime.now(timezone.utc))
                    path = self.config.data_path / f"{self.metadata.dataset.name}_{start_s}-{end_s}_{now_s}.parquet"

                    async def drained_stream():
                        for obj in drained:
                            yield obj

                    try:
                        df_stream = netatmo_dataframe_stream(
                            drained_stream(),
                            batch_size=self.config.flush_max_records,
                            logger=self.logger,
                        )
                        rows = await stream_dataframes_to_parquet(df_stream, path, schema_fields=None)
                        now_utc = datetime.now(timezone.utc)
                        lag_seconds = max(0.0, (now_utc - start_time).total_seconds())
                        self.metrics.sources.observe_data_lag(source, lag_seconds)
                        self.metrics.sources.observe_request_rows(source, int(rows))
                        self.metrics.sources.record_ingestion_request(source, "success")
                        self.logger.info(
                            "Wrote parquet file",
                            rows=rows,
                            path=str(path),
                            start=start_time.isoformat(),
                            end=end_time.isoformat(),
                        )
                    except Exception as e:
                        self.metrics.sources.record_ingestion_request(source, "error")
                        self.logger.error("Failed to write parquet file", error=str(e), path=str(path))
                        continue

                    ingestion_command = IngestDataCommand(
                        id=uuid4(),
                        metadata=self.metadata,
                        payload_location=path,
                        start_time=start_time,
                        end_time=end_time,
                    )

                    published = False
                    for attempt in range(3):
                        try:
                            await channel.default_exchange.publish(
                                aio_pika.Message(body=ingestion_command.model_dump_json().encode()),
                                routing_key=self.config.routing_key,
                            )
                            published = True
                            break
                        except Exception as e:
                            if attempt < 2:
                                self.logger.warning("Failed to publish", error=str(e), path=str(path))
                                await asyncio.sleep(1)
                            else:
                                self.logger.error("AMQP publish error", error=str(e), path=str(path))
                    if not published:
                        self.logger.error("Failed to publish ingestion command after retries", path=str(path))
                    self._last_flush = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.metrics.sources.record_ingestion_request(self._source_name, "error")
            self.logger.error("Aggregate/publish task error", error=str(e))
