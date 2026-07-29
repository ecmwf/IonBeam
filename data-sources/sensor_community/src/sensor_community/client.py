# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
import gzip
import io
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from time import monotonic
from typing import AsyncIterator, Iterable, Optional
from uuid import UUID

import httpx
import ijson
import numpy as np
import pandas as pd
import structlog
from aiostream import stream
from bs4 import BeautifulSoup
from httpx_retries import Retry, RetryTransport
from ionbeam_client import IonbeamClient
from ionbeam_client.arrow_tools import canonical_record_batches
from ionbeam_client.dataframe_tools import drop_undeclared_columns
from ionbeam_client.models import (
    DatasetSchema,
    IngestionMetadata,
    Tag,
    TimeCoordinate,
    cf,
    geographic_point_coordinates,
)

from .models import SensorCommunityConfig

# Archive CSV column -> the canonical column it lands in.
ARCHIVE_COLUMNS = {
    "timestamp": "time",
    "temperature": "air_temperature",
    "humidity": "relative_humidity",
    "pressure": "air_pressure",
    "pressure_sealevel": "air_pressure_at_sea_level",
    "P0": "mass_concentration_of_pm1_ambient_aerosol_in_air",
    "P1": "mass_concentration_of_pm10_ambient_aerosol_in_air",
    "P2": "mass_concentration_of_pm2p5_ambient_aerosol_in_air",
}


@dataclass
class SensorMetadata:
    url: str
    date: datetime
    sensor_type: str
    sensor_id: str
    last_updated: datetime


@dataclass
class SensorDataChunk:
    id: str
    metadata: SensorMetadata
    data: pd.DataFrame


retry_transport = RetryTransport(retry=Retry(total=5, backoff_factor=0.5))

# sensor.community requires clients to identify themselves.
USER_AGENT = "ionbeam (ECMWF; https://github.com/ecmwf/IonBeam)"

# Live API value_type -> the canonical column it lands in.
LIVE_VALUE_COLUMNS = {
    "temperature": "air_temperature",
    "humidity": "relative_humidity",
    "pressure": "air_pressure",
    "pressure_at_sealevel": "air_pressure_at_sea_level",
    "P0": "mass_concentration_of_pm1_ambient_aerosol_in_air",
    "P1": "mass_concentration_of_pm10_ambient_aerosol_in_air",
    "P2": "mass_concentration_of_pm2p5_ambient_aerosol_in_air",
}

# Must outlive the live dump's 5-minute lookback.
SEEN_IDS_RETENTION_SECONDS = 600


def live_records_to_frame(records: list[dict]) -> pd.DataFrame:
    rows = []
    for record in records:
        values = {
            LIVE_VALUE_COLUMNS[v["value_type"]]: v["value"]
            for v in record["sensordatavalues"]
            if v["value_type"] in LIVE_VALUE_COLUMNS
        }
        if not values:
            continue
        rows.append(
            {
                "time": record["timestamp"],
                "lat": record["location"]["latitude"],
                "lon": record["location"]["longitude"],
                "sensor_id": str(record["sensor"]["id"]),
                "sensor_type": record["sensor"]["sensor_type"]["name"],
                **values,
            }
        )
    return pd.DataFrame(rows)


class SensorCommunitySource:
    def __init__(self, config: SensorCommunityConfig):
        self._config = config
        self.logger = structlog.get_logger(__name__)
        self.metadata: IngestionMetadata = IngestionMetadata(
            # v2: typed semantics (CfSemantics) instead of the stringly
            # scheme/standard_name/attrs trio.
            # v3: relative_humidity unit corrected to % — the API delivers
            # percent; the v2 label "1" was wrong for the same values.
            version=3,
            name="sensor.community",
            dataset_schema=DatasetSchema(
                time=TimeCoordinate(),
                coordinates=geographic_point_coordinates(),
                variables=[
                    cf("air_temperature", "degC"),
                    cf("relative_humidity", "%"),
                    cf("air_pressure", "Pa"),
                    cf("air_pressure_at_sea_level", "Pa"),
                    cf("mass_concentration_of_pm1_ambient_aerosol_in_air", "ug m-3"),
                    cf("mass_concentration_of_pm10_ambient_aerosol_in_air", "ug m-3"),
                    cf("mass_concentration_of_pm2p5_ambient_aerosol_in_air", "ug m-3"),
                ],
                tags=[
                    Tag(name="sensor_id"),
                    Tag(name="sensor_type"),
                ],
            ),
        )

    async def fetch(
        self,
        start_time: datetime,
        end_time: datetime,
        client: IonbeamClient,
        ingestion_id: Optional[UUID] = None,
        limit=None,
    ) -> None:
        async def dataframe_stream() -> AsyncIterator[pd.DataFrame]:
            async for chunk in self.crawl_sensor_data_in_chunks(
                start_time, end_time, limit
            ):
                if chunk.data is not None and not chunk.data.empty:
                    yield chunk.data

        batch_stream = canonical_record_batches(dataframe_stream(), self.metadata)

        await client.ingest(
            batch_stream=batch_stream,
            metadata=self.metadata,
            start_time=start_time,
            end_time=end_time,
            ingestion_id=ingestion_id,
        )

        self.logger.info(
            "Sensor.community ingestion completed",
            start=start_time.isoformat(),
            end=end_time.isoformat(),
        )

    async def poll_live(self, client: IonbeamClient, stop: asyncio.Event) -> None:
        """Ingest the live API dump (the last ~5 minutes of measurements,
        regenerated every minute) until ``stop`` is set. Measurement ids are
        marked seen only once ingested, so a failed poll retries them."""
        seen: dict[int, float] = {}

        async with httpx.AsyncClient(
            timeout=self._config.timeout_seconds,
            transport=retry_transport,
            headers={"User-Agent": USER_AGENT},
        ) as http:
            while not stop.is_set():
                started = monotonic()
                seen = {
                    measurement_id: at
                    for measurement_id, at in seen.items()
                    if started - at < SEEN_IDS_RETENTION_SECONDS
                }
                try:
                    ingested = await self._ingest_live_dump(http, client, seen)
                    seen.update((measurement_id, started) for measurement_id in ingested)
                except Exception:
                    self.logger.exception("Live poll failed; retrying next interval")

                delay = self._config.poll_interval_seconds - (monotonic() - started)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=max(delay, 0))
                except asyncio.TimeoutError:
                    pass

    async def _ingest_live_dump(
        self, http: httpx.AsyncClient, client: IonbeamClient, seen: dict[int, float]
    ) -> set[int]:
        response = await http.get(self._config.live_url)
        response.raise_for_status()
        # Stream-parse the dump: json.loads would materialize every record's
        # dict tree at once (~15x the dump's bytes), and the dump grows with
        # the network's diurnal cycle — parse peak must scale with the *new*
        # records instead.
        records = [
            r
            for r in ijson.items(response.content, "item", use_float=True)
            if r["id"] not in seen
        ]
        frame = live_records_to_frame(records)

        if not frame.empty:
            times = pd.to_datetime(frame["time"], utc=True)
            await client.ingest(
                batch_stream=canonical_record_batches([frame], self.metadata),
                metadata=self.metadata,
                start_time=times.min().to_pydatetime(),
                end_time=times.max().to_pydatetime(),
            )
            self.logger.info(
                "Ingested live measurements",
                records=len(frame),
                start=times.min().isoformat(),
                end=times.max().isoformat(),
            )

        return {r["id"] for r in records}

    async def load_raw_to_df(self, url: str, client: httpx.AsyncClient):
        # A 404 is a sensor with no archive file that day — checked, nothing
        # there. Any other failure must raise: continuing would claim a range
        # as swept that was skipped on error.
        response = await client.get(url)
        if response.status_code == 404:
            self.logger.info("No archive CSV for sensor", url=url)
            return None
        response.raise_for_status()

        try:
            raw_data = io.BytesIO(response.content)

            if url.endswith(".csv.gz"):
                with gzip.GzipFile(fileobj=raw_data, mode="rb") as gz:
                    raw_data = io.BytesIO(gz.read())

            df = pd.read_csv(raw_data, delimiter=";")
            df = (
                df.replace("unknown", np.nan)
                .replace("", np.nan)
                .replace("unavailable", np.nan)
            )
            df = df.rename(columns=ARCHIVE_COLUMNS)
            return drop_undeclared_columns(df, self.metadata.dataset_schema)
        except Exception:
            # Corrupt content is permanent for this file; a re-fetch returns
            # the same bytes, so skip it rather than poison the trigger.
            self.logger.exception("Unreadable archive CSV", url=url)
            return None

    async def get_sensor_urls_by_date(
        self, timestamp: date, client: httpx.AsyncClient
    ) -> AsyncIterator[SensorMetadata]:
        path = (
            "/{year}/{year}-{month:02d}-{day:02d}/"
            if timestamp.year <= 2023
            else "/{year}-{month:02d}-{day:02d}/"
        ).format(year=timestamp.year, month=timestamp.month, day=timestamp.day)
        url = f"{self._config.base_url}{path}"

        self.logger.info("Fetching sensor data", date=str(timestamp))
        response = await client.get(url)
        # An unreachable or unpublished day listing must raise, not read as an
        # empty day — a claimed sweep of it would never be re-fetched.
        response.raise_for_status()

        soup = BeautifulSoup(response.text, features="lxml")

        sensor_file_rows = soup.find_all("tr")[3:-1]
        for row in sensor_file_rows:
            sensor_file = (
                row.find("a", href=True)["href"] if row.find("a", href=True) else None
            )
            last_updated = row.find_all("td")[2].text.strip()
            if None in [sensor_file, last_updated]:
                self.logger.error("Failed to parse %s", row)
                continue

            pattern = re.compile(r"(\d{4}-\d{2}-\d{2})_(.*)_sensor_(.*).(csv.gz|csv)")
            match = pattern.match(sensor_file)
            if match:
                date_val = match.group(1)
                sensor_type = match.group(2)
                sensor_id = match.group(3)
                yield SensorMetadata(
                    url + sensor_file, date_val, sensor_type, sensor_id, last_updated
                )

    def _split_by_day(self, start_time: datetime, end_time: datetime) -> Iterable[date]:
        """
        Splits the window into daily chunks. Uses half-open interval [start_time, end_time).

        - Sensor.community archives data into per-day folders, with csv per-sensor.
        """
        current_date = start_time.date()
        end_date = end_time.date()

        # If end_time is exactly at midnight, don't include that day
        if end_time.time() == time(0, 0, 0):
            end_date = end_date - timedelta(days=1)

        while current_date <= end_date:
            yield current_date
            current_date += timedelta(days=1)

    async def crawl_sensor_data_in_chunks(
        self, start_time: datetime, end_time: datetime, limit=None
    ) -> AsyncIterator[SensorDataChunk]:
        """
        Performs the crawling logic

        - divide collection window into chunks that make sense for source (1 day, as that's how sensor.community archives data)
        - per chunk data is scraped
        - chunked data is parsed and mapped into canonical form
        - canonicalized data is yielded as dataframe
        """
        async with httpx.AsyncClient(
            timeout=self._config.timeout_seconds,
            transport=retry_transport,
            headers={"User-Agent": USER_AGENT},
        ) as client:
            for chunk in self._split_by_day(start_time, end_time):
                i = 0

                async with stream.chunks(
                    self.get_sensor_urls_by_date(chunk, client),
                    self._config.concurrency,
                ).stream() as chunk_stream:
                    async for sensor_group in chunk_stream:
                        if limit and i >= limit:
                            break

                        tasks = [
                            self.load_raw_to_df(sensor.url, client)
                            for sensor in sensor_group
                        ]
                        results = await asyncio.gather(*tasks)
                        for sensor, result in zip(sensor_group, results):
                            if result is not None:
                                yield SensorDataChunk(
                                    f"{sensor.sensor_id}_{sensor.sensor_type}_{sensor.last_updated}",
                                    sensor,
                                    result,
                                )
                                i += 1
