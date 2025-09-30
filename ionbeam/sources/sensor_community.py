import asyncio
import gzip
import io
import logging
import pathlib
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from time import perf_counter
from typing import AsyncIterator, Iterable, List, Optional, Tuple
from uuid import UUID, uuid4

import httpx
import numpy as np
import pandas as pd
import pyarrow as pa
from aiostream import stream
from bs4 import BeautifulSoup
from httpx_retries import Retry, RetryTransport
from pydantic import BaseModel

from ionbeam.observability.metrics import IonbeamMetricsProtocol

from ..core.constants import LatitudeColumn, LongitudeColumn, ObservationTimestampColumn
from ..core.handler import BaseHandler
from ..models.models import (
    CanonicalVariable,
    DataIngestionMap,
    DatasetMetadata,
    IngestDataCommand,
    IngestionMetadata,
    LatitudeAxis,
    LongitudeAxis,
    MetadataVariable,
    StartSourceCommand,
    TimeAxis,
)
from ..utilities.dataframe_tools import coerce_types
from ..utilities.parquet_tools import stream_dataframes_to_parquet


@dataclass
class Request:
    id: UUID
    start_time: datetime
    end_time: datetime


class SensorCommunityConfig(BaseModel):
    base_url: str = "https://archive.sensor.community"
    timeout_seconds: int = 60
    concurrency: int = 10
    data_path: pathlib.Path
    use_cache: Optional[bool] = True


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


class SensorCommunitySource(BaseHandler[StartSourceCommand, Optional[IngestDataCommand]]):
    def __init__(self, config: SensorCommunityConfig, metrics: IonbeamMetricsProtocol):
        super().__init__("SensorCommunitySource", metrics)
        self._config = config
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
                    CanonicalVariable(column="temperature", standard_name="air_temperature", cf_unit="degC"),
                    CanonicalVariable(column="humidity", standard_name="relative_humidity", cf_unit="1"),
                    CanonicalVariable(column="pressure", standard_name="air_pressure", cf_unit="Pa"),
                    CanonicalVariable(column="pressure_sealevel", standard_name="air_pressure_at_sea_level", cf_unit="Pa"),
                    CanonicalVariable(column="P0", standard_name="mass_concentration_of_pm1_ambient_aerosol_in_air", cf_unit="ug m-3"),
                    CanonicalVariable(column="P1", standard_name="mass_concentration_of_pm10_ambient_aerosol_in_air", cf_unit="ug m-3"),
                    CanonicalVariable(column="P2", standard_name="mass_concentration_of_pm2p5_ambient_aerosol_in_air", cf_unit="ug m-3"),
                ],
                metadata_variables=[
                    MetadataVariable(column="sensor_id", dtype="int32"),
                    MetadataVariable(column="sensor_type"),
                ],
            ),
        )

    async def load_raw_to_df(self, url: str, client: httpx.AsyncClient):
        # @cached(f'raw_sensor_data_{url}', timeout=timedelta(days=365), use_cache=self._config.use_cache)
        async def _fetch():
            response = await client.get(url)
            if response.status_code != 200:
                self.logger.error("Fetching CSV %s failed", url)
                return
            return io.BytesIO(response.content)

        try:
            # Only some datasets have compression(?)
            raw_data = await _fetch()
            if raw_data is None:
                return None
            if url.endswith(".csv.gz"):
                with gzip.GzipFile(fileobj=raw_data, mode="rb") as gz:
                    raw_data = io.BytesIO(gz.read())

            df = pd.read_csv(raw_data, delimiter=";")
            df = df.replace("unknown", np.nan).replace("", np.nan).replace("unavailable", np.nan)
            df = coerce_types(df, self.metadata.ingestion_map)
            return df
        except Exception as e:
            self.logger.exception(e)

    async def get_sensor_urls_by_date(self, timestamp: date, client: httpx.AsyncClient) -> AsyncIterator[SensorMetadata]:
        """
        Reads the contents page of a specified date, returns all sensor file references
        """

        # handle weird logic where folder structure is different before
        path = ("/{year}/{year}-{month:02d}-{day:02d}/" if timestamp.year <= 2023 else "/{year}-{month:02d}-{day:02d}/").format(
            year=timestamp.year, month=timestamp.month, day=timestamp.day
        )
        url = f"{self._config.base_url}{path}"

        # @cached(f'raw_sensor_list{url}', timeout=timedelta(days=365), use_cache=self._config.use_cache)
        async def _fetch():
            self.logger.info("Fetching sensor data for %s", timestamp)
            response = await client.get(url)

            if response.status_code != 200:
                self.logger.error("Request for sensors for %s failed", timestamp)
                return None

            return response.text

        result = await _fetch()
        if result:
            soup = BeautifulSoup(result, features="lxml")

            sensor_file_rows = soup.find_all("tr")[3:-1]
            for row in sensor_file_rows:
                sensor_file = row.find("a", href=True)["href"] if row.find("a", href=True) else None
                last_updated = row.find_all("td")[2].text.strip()
                if None in [sensor_file, last_updated]:
                    logging.error("Failed to parse %s", row)
                    continue

                pattern = re.compile(r"(\d{4}-\d{2}-\d{2})_(.*)_sensor_(.*).(csv.gz|csv)")
                match = pattern.match(sensor_file)
                if match:
                    date_val = match.group(1)
                    sensor_type = match.group(2)
                    sensor_id = match.group(3)
                    yield SensorMetadata(url + sensor_file, date_val, sensor_type, sensor_id, last_updated)

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

    async def crawl_sensor_data_in_chunks(self, start_time: datetime, end_time: datetime, limit=None) -> AsyncIterator[SensorDataChunk]:
        """
        Performs the crawling logic

        - divide collection window into chunks that make sense for source (1 day, as that's how sensor.community archives data)
        - per chunk data is scraped
        - chunked data is parsed and mapped into canonical form
        - canonicalized data is yielded as dataframe
        """
        async with httpx.AsyncClient(timeout=self._config.timeout_seconds, transport=retry_transport) as client:
            for chunk in self._split_by_day(start_time, end_time):
                i = 0

                # TODO - request can be just hours, so we'll need to truncate data for consistency, even if we HAVE to pull it
                async with stream.chunks(
                    self.get_sensor_urls_by_date(chunk, client),
                    self._config.concurrency,
                ).stream() as chunk_stream:
                    async for sensor_group in chunk_stream:
                        if limit and i >= limit:
                            break

                        tasks = [self.load_raw_to_df(sensor.url, client) for sensor in sensor_group]
                        results = await asyncio.gather(*tasks)
                        for sensor, result in zip(sensor_group, results):
                            if isinstance(result, Exception):
                                self.logger.error(
                                    "Failed to fetch sensor df %s \n %s",
                                    result,
                                    sensor_group,
                                )
                            else:
                                yield SensorDataChunk(
                                    sensor,
                                    f"{sensor.sensor_id}_{sensor.sensor_type}_{sensor.last_updated}",
                                    result,
                                )
                                i += 1

    async def _handle(self, event: StartSourceCommand) -> Optional[IngestDataCommand]:
        dataset_name = self.metadata.dataset.name
        request_start = perf_counter()
        total_rows = 0

        try:
            self._config.data_path.mkdir(parents=True, exist_ok=True)
            path = (
                self._config.data_path
                / f"{self.metadata.dataset.name}_{event.start_time}-{event.end_time}_{datetime.now(timezone.utc)}.parquet"
            )

            # Create async generator that yields just the dataframes
            async def dataframe_stream():
                async for chunk in self.crawl_sensor_data_in_chunks(event.start_time, event.end_time):
                    if chunk.data is not None:
                        yield chunk.data
            
            # Build schema from ingestion_map
            schema_fields: List[Tuple[str, pa.DataType]] = [
                (self.metadata.ingestion_map.datetime.from_col or ObservationTimestampColumn, pa.timestamp("ns", tz="UTC")),
                (self.metadata.ingestion_map.lat.from_col or LatitudeColumn, pa.float64()),
                (self.metadata.ingestion_map.lon.from_col or LongitudeColumn, pa.float64()),
            ]

            for var in self.metadata.ingestion_map.canonical_variables + self.metadata.ingestion_map.metadata_variables:
                pa_type = pa.string() if var.dtype == "string" or var.dtype == "object" else pa.from_numpy_dtype(np.dtype(var.dtype))
                schema_fields.append((var.column, pa_type))
    
            
            # Stream data directly to parquet file using helper
            total_rows = await stream_dataframes_to_parquet(
                dataframe_stream(),
                path,
                schema_fields,
            )

            request_duration = perf_counter() - request_start
            self.metrics.sources.observe_fetch_duration(dataset_name, request_duration)
            self.metrics.sources.observe_request_rows(dataset_name, int(total_rows))

            if total_rows == 0:
                self.metrics.sources.record_ingestion_request(dataset_name, "empty")
                self.logger.warning("No data collected")
                return None

            self.metrics.sources.record_ingestion_request(dataset_name, "success")

            self.logger.info(f"Saved {total_rows} rows to {path}")

            return IngestDataCommand(
                id=uuid4(),
                metadata=self.metadata,
                payload_location=path,
                start_time=event.start_time,
                end_time=event.end_time,
            )

        except Exception as e:
            request_duration = perf_counter() - request_start
            self.metrics.sources.observe_fetch_duration(dataset_name, request_duration)
            self.metrics.sources.observe_request_rows(dataset_name, int(total_rows))
            self.metrics.sources.record_ingestion_request(dataset_name, "error")
            self.logger.exception(e)
            return None
