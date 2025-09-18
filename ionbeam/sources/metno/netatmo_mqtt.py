import asyncio
import json
import logging
import ssl
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


class NetAtmoMQTTSource:
    def __init__(self, config: NetAtmoMQTTConfig):
        self.config = config
        self._buffer: List[dict] = []
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._mqtt_task: asyncio.Task | None = None
        self._agg_task: asyncio.Task | None = None
        self.logger = logging.getLogger(__name__)
        self.metadata = netatmo_metadata
        self.logger.info("In init")
        
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
        self.logger.info("In __aenter__")
        await self.start()
        return self

    async def __aexit__(self):
        await self.stop()

    async def _listen(self):
        self.logger.info("In listener")
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
                    self.logger.info("Subscribed to netatmo")
                    async for msg in client.messages:
                        if self._stop.is_set():
                            break
                        # self.logger.info("Handling %s", msg)
                        await self._handle_message(msg)
            except Exception as e:
                self.logger.info(f"MQTT error: {e}, retrying in 5s")
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
            self.logger.info("Aggregate and publish worker started")
            if(len(self._buffer) < 50000): # TODO - add time check to always purge buffer
                await asyncio.sleep(5)  # every 5 seconds
                continue
            async with self._lock:
                self.logger.info("Draining...")
                drained = self._buffer
                self._buffer = []
            if not drained:
                continue
            await self._process_and_send(drained)

    async def _process_and_send(self, buffer: List[dict]):
        self.logger.info("prcessing %s messages...", len(buffer))
        rows = []
        for idx, d in enumerate(buffer):
            if not isinstance(d, dict):
                self.logger.warning("chunk[%d]: not a mapping; skipping", idx)
                continue

            props = d.get("properties")
            if not isinstance(props, dict):
                self.logger.warning("chunk[%d]: missing 'properties'; skipping", idx)
                continue

            content = props.get("content")
            if not isinstance(content, dict):
                self.logger.warning("chunk[%d]: missing 'properties.content'; skipping", idx)
                continue

            geom = d.get("geometry")
            if not isinstance(geom, dict):
                self.logger.warning("chunk[%d]: missing 'geometry'; skipping", idx)
                continue

            station_id = props.get("platform")
            if not station_id:
                self.logger.warning("chunk[%d]: missing 'properties.platform'; skipping", idx)
                continue

            obs_time = pd.to_datetime(props.get("datetime"), utc=True, errors="coerce")
            if pd.isna(obs_time):
                self.logger.warning("chunk[%d]: invalid 'properties.datetime'; skipping", idx)
                continue

            pub_time = pd.to_datetime(props.get("pubtime"), utc=True, errors="coerce")

            standard_name = content.get("standard_name")
            if not standard_name:
                self.logger.warning("chunk[%d]: missing 'content.standard_name'; skipping", idx)
                continue

            # Build parameter from available parts (no extra sanitization)
            level = props.get("level")
            method = props.get("function")
            period = props.get("period")
            parts = [standard_name, level, method, period]
            param = ":".join([str(p) for p in parts if p not in (None, "")])

            coords = geom.get("coordinates")
            if not isinstance(coords, dict):
                self.logger.warning("chunk[%d]: 'geometry.coordinates' must be a mapping with lat/lon; skipping", idx)
                continue

            try:
                lat = float(coords.get("lat"))
                lon = float(coords.get("lon"))
            except (TypeError, ValueError):
                self.logger.warning("chunk[%d]: invalid lat/lon; skipping", idx)
                continue

            value = pd.to_numeric(content.get("value"), errors="coerce")
            # Keep NaN values; they will show up as NaN in the pivot.

            rows.append(
                {
                    "_row_idx": idx,  # arrival order within this chunk (fallback tie-break)
                    "station_id": station_id,
                    "datetime": obs_time,
                    "lat": lat,
                    "lon": lon,
                    "pubtime": pub_time,  # may be NaT
                    "parameter": param,
                    "value": value,
                }
            )

        if not rows:
            return pd.DataFrame(columns=["station_id", "datetime", "lat", "lon"])

        df = pd.DataFrame(rows)
        key = ["station_id", "datetime", "lat", "lon", "parameter"]

        # Keep the latest by pubtime; if equal/missing pubtime, prefer latest arrival.
        # We sort so the desired record is LAST, then drop_duplicates(keep="last").
        df["has_pubtime"] = df["pubtime"].notna()
        df = df.sort_values(
            by=["has_pubtime", "pubtime", "_row_idx"],
            ascending=[True, True, True],  # NaT/False first, earlier pubtime first, earlier arrival first
            kind="stable",
        )
        dedup = df.drop_duplicates(subset=key, keep="last").drop(columns=["has_pubtime", "_row_idx"])

        # Pivot to wide
        wide = dedup.pivot(index=["station_id", "datetime", "lat", "lon"], columns="parameter", values="value").reset_index()
        wide.columns.name = None
        wide = wide.sort_values(["station_id", "datetime"]).reset_index(drop=True)

        start_time = df["datetime"].min()
        end_time = df["datetime"].max()

        # Save to file
        self.config.data_path.mkdir(parents=True, exist_ok=True)
        path = self.config.data_path / f"{self.metadata.dataset.name}_{start_time}-{end_time}_{datetime.now(timezone.utc)}.parquet"
        wide.to_parquet(path, engine="pyarrow")

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
            self.logger.info(f"AMQP send error: {e}")
