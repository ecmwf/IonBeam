import asyncio
import gzip
import io
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import AsyncIterator, Iterable

import httpx
import numpy as np
import pandas as pd
import structlog
from aiostream import stream
from bs4 import BeautifulSoup
from httpx_retries import Retry, RetryTransport
from ionbeam_client import IonbeamClient, coerce_types
from ionbeam_client.arrow_tools import (
    dataframes_to_record_batches,
    schema_from_ingestion_map,
)
from ionbeam_client.models import (
    CanonicalVariable,
    DataIngestionMap,
    DatasetMetadata,
    IngestionMetadata,
    LatitudeAxis,
    LongitudeAxis,
    MetadataVariable,
    TimeAxis,
)

from .models import SensorCommunityConfig


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


class SensorCommunitySource:
    def __init__(self, config: SensorCommunityConfig):
        self._config = config
        self.logger = structlog.get_logger(__name__)
        self.metadata: IngestionMetadata = IngestionMetadata(
            dataset=DatasetMetadata(
                name="sensor.community",
                description="raw IoT data collected from sensor.community",
                subject_to_change_window=timedelta(days=1),
                aggregation_span=timedelta(hours=1),
                source_links=[],
                keywords=["sensor.community", "iot", "data"],
            ),
            ingestion_map=DataIngestionMap(
                datetime=TimeAxis(from_col="timestamp"),
                lat=LatitudeAxis(standard_name="latitude", cf_unit="degrees_north"),
                lon=LongitudeAxis(standard_name="longitude", cf_unit="degrees_east"),
                canonical_variables=[
                    CanonicalVariable(
                        column="temperature",
                        standard_name="air_temperature",
                        cf_unit="degC",
                    ),
                    CanonicalVariable(
                        column="humidity",
                        standard_name="relative_humidity",
                        cf_unit="1",
                    ),
                    CanonicalVariable(
                        column="pressure", standard_name="air_pressure", cf_unit="Pa"
                    ),
                    CanonicalVariable(
                        column="pressure_sealevel",
                        standard_name="air_pressure_at_sea_level",
                        cf_unit="Pa",
                    ),
                    CanonicalVariable(
                        column="P0",
                        standard_name="mass_concentration_of_pm1_ambient_aerosol_in_air",
                        cf_unit="ug m-3",
                    ),
                    CanonicalVariable(
                        column="P1",
                        standard_name="mass_concentration_of_pm10_ambient_aerosol_in_air",
                        cf_unit="ug m-3",
                    ),
                    CanonicalVariable(
                        column="P2",
                        standard_name="mass_concentration_of_pm2p5_ambient_aerosol_in_air",
                        cf_unit="ug m-3",
                    ),
                ],
                metadata_variables=[
                    MetadataVariable(column="sensor_id", dtype="int32"),
                    MetadataVariable(column="sensor_type"),
                ],
            ),
        )

    async def fetch(
        self,
        start_time: datetime,
        end_time: datetime,
        client: IonbeamClient,
        limit=None,
    ) -> None:
        schema = schema_from_ingestion_map(self.metadata.ingestion_map)
        
        async def dataframe_stream() -> AsyncIterator[pd.DataFrame]:
            async for chunk in self.crawl_sensor_data_in_chunks(
                start_time, end_time, limit
            ):
                if chunk.data is not None and not chunk.data.empty:
                    yield chunk.data

        batch_stream = dataframes_to_record_batches(
            dataframe_stream(),
            schema=schema,
            preserve_index=False,
        )

        await client.ingest(
            batch_stream=batch_stream,
            metadata=self.metadata,
            start_time=start_time,
            end_time=end_time,
        )

        self.logger.info(
            "Sensor.community ingestion completed",
            start=start_time.isoformat(),
            end=end_time.isoformat(),
        )

    async def load_raw_to_df(self, url: str, client: httpx.AsyncClient):
        try:
            response = await client.get(url)
            if response.status_code != 200:
                self.logger.error("Fetching CSV %s failed", url)
                return None

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
            df = coerce_types(df, self.metadata.ingestion_map)
            return df
        except Exception as e:
            self.logger.exception(e)
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

        self.logger.info("Fetching sensor data for %s", timestamp)
        response = await client.get(url)

        if response.status_code != 200:
            self.logger.error("Request for sensors for %s failed", timestamp)
            return

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
            timeout=self._config.timeout_seconds, transport=retry_transport
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
                            if isinstance(result, Exception):
                                self.logger.error(
                                    "Failed to fetch sensor df %s \n %s",
                                    result,
                                    sensor_group,
                                )
                            elif result is not None:
                                yield SensorDataChunk(
                                    f"{sensor.sensor_id}_{sensor.sensor_type}_{sensor.last_updated}",
                                    sensor,
                                    result,
                                )
                                i += 1
