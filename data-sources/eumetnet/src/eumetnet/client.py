# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import asyncio
import json
import ssl
import time
from typing import List

import aiomqtt
import pandas as pd
import structlog
from ionbeam_client import IonbeamClient
from ionbeam_client.arrow_tools import schema_from_ingestion_map
from ionbeam_client.models import IngestionMetadata

from .models import NetAtmoMQTTConfig
from .netatmo_processing import netatmo_record_batch_stream


class NetAtmoMQTTSource:
    def __init__(
        self,
        config: NetAtmoMQTTConfig,
        client: IonbeamClient,
        metadata: IngestionMetadata,
        topic: str,
        client_id_suffix: str = "",
    ):
        self.config = config
        self.client = client
        self.metadata = metadata
        self.topic = topic
        self._buffer: List[dict] = []
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._mqtt_task: asyncio.Task | None = None
        self._agg_task: asyncio.Task | None = None
        self.logger = structlog.get_logger(__name__)
        self._source_name = self.metadata.dataset.name
        self._last_flush = time.monotonic()
        self._arrow_schema = schema_from_ingestion_map(self.metadata.ingestion_map)

        self._identifier = f"{self.config.client_id}{client_id_suffix}"

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
                if isinstance(res, Exception) and not isinstance(
                    res, asyncio.CancelledError
                ):
                    self.logger.warning(
                        "Task raised during stop()",
                        task=str(tasks[idx]),
                        error=str(res),
                    )

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
                    await mqtt_client.subscribe(self.topic, qos=1)
                    self.logger.info(
                        "Subscribed to Netatmo MQTT",
                        client_id=self._identifier,
                        topic=self.topic,
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
                size_threshold = self.config.flush_max_records
                elapsed = time.monotonic() - self._last_flush
                flush_due_to_size = len(self._buffer) >= size_threshold
                flush_due_to_time = (len(self._buffer) > 0) and (
                    elapsed >= self.config.flush_interval_seconds
                )

                if not flush_due_to_size and not flush_due_to_time:
                    sleep_for = min(
                        5, max(0, self.config.flush_interval_seconds - elapsed)
                    )
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

                try:
                    batch_stream = netatmo_record_batch_stream(
                        drained,
                        batch_size=self.config.flush_max_records,
                        logger=self.logger,
                        schema=self._arrow_schema,
                    )

                    await self.client.ingest(
                        batch_stream=batch_stream,
                        metadata=self.metadata,
                        start_time=start_time,
                        end_time=end_time,
                    )

                    self.logger.info(
                        "Ingested Netatmo data",
                        dataset=self._source_name,
                        start=start_time.isoformat(),
                        end=end_time.isoformat(),
                    )
                except Exception as e:
                    self.logger.error(
                        "Failed to ingest data", dataset=self._source_name, error=str(e)
                    )
                    continue

                self._last_flush = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.error("Aggregation task failed", error=str(e))
