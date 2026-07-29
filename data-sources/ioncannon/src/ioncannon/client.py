# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import random
import string
from datetime import datetime, timedelta
from typing import AsyncIterator, Optional
from uuid import UUID

import pandas as pd
import structlog
from ionbeam_client import IonbeamClient
from ionbeam_client.canonical_stream import canonical_record_batches
from ionbeam_client.models import (
    DatasetSchema,
    IngestionMetadata,
    Tag,
    TimeCoordinate,
    cf,
    geographic_point_coordinates,
)

from .models import IonCannonConfig


class IonCannonSource:
    """
    IonCannon: A configurable load test source that generates synthetic data
    based on DatasetSchema configurations.
    """

    def __init__(self, config: IonCannonConfig):
        self.config = config
        self.logger = structlog.get_logger(__name__)

        self.metadata: IngestionMetadata = IngestionMetadata(
            version=3,
            name="ioncannon",
            dataset_schema=DatasetSchema(
                time=TimeCoordinate(),
                coordinates=geographic_point_coordinates(),
                variables=[
                    cf("air_temperature", "degC"),
                    cf("air_pressure", "Pa"),
                    cf("relative_humidity", "%"),
                    cf("wind_speed", "m s-1"),
                ],
                tags=[
                    Tag(name="station_id"),
                    Tag(name="sensor_type"),
                    Tag(name="location_type"),
                ],
            ),
        )

        # Pre-generate fixed pools of metadata values to control cardinality
        self._metadata_value_pools = self._generate_metadata_value_pools()

    def _generate_metadata_value_pools(self) -> dict[str, list[str]]:
        """
        Generate a fixed pool of values for each metadata variable to control cardinality.
        This prevents high cardinality issues in InfluxDB tags.
        """
        pools = {}
        cardinality = self.config.metadata_cardinality

        for var in self.metadata.dataset_schema.tags:
            column_name = var.name
            pool = []
            for i in range(cardinality):
                suffix = "".join(
                    random.choices(string.ascii_lowercase + string.digits, k=3)
                )
                pool.append(f"{column_name}-{suffix}")

            pools[column_name] = pool

        return pools

    async def generate_data_chunk(
        self, start_time: datetime, end_time: datetime
    ) -> AsyncIterator[pd.DataFrame]:
        freq = timedelta(minutes=self.config.measurement_frequency_minutes)
        timestamps = pd.date_range(start_time, end_time, freq=freq, inclusive="left")

        if len(timestamps) == 0:
            return

        for station_id in range(self.config.num_stations):
            rows = []
            lat = random.uniform(self.config.min_lat, self.config.max_lat)
            lon = random.uniform(self.config.min_lon, self.config.max_lon)

            for timestamp in timestamps:
                row = {
                    "time": timestamp,
                    "lat": lat,
                    "lon": lon,
                    "station_id": f"SYNTH_{station_id:04d}",
                }

                for var in self.metadata.dataset_schema.variables:
                    row[var.name] = random.uniform(0, 100)

                for var in self.metadata.dataset_schema.tags:
                    if var.name != "station_id":
                        row[var.name] = random.choice(
                            self._metadata_value_pools[var.name]
                        )

                rows.append(row)

            if rows:
                yield pd.DataFrame(rows)

    async def fetch(
        self,
        start_time: datetime,
        end_time: datetime,
        client: IonbeamClient,
        ingestion_id: Optional[UUID] = None,
    ) -> None:
        """Fetch and ingest IonCannon synthetic data for the given time window."""

        self.logger.info(
            "Starting IonCannon data generation",
            start=start_time.isoformat(),
            end=end_time.isoformat(),
        )

        async def dataframe_stream() -> AsyncIterator[pd.DataFrame]:
            async for df in self.generate_data_chunk(start_time, end_time):
                yield df

        batch_stream = canonical_record_batches(dataframe_stream(), self.metadata)

        await client.ingest(
            batch_stream=batch_stream,
            metadata=self.metadata,
            start_time=start_time,
            end_time=end_time,
            ingestion_id=ingestion_id,
        )

        self.logger.info(
            "IonCannon ingestion completed",
            start=start_time.isoformat(),
            end=end_time.isoformat(),
        )
