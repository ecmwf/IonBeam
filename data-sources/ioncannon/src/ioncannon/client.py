import random
import string
from datetime import datetime, timedelta
from typing import AsyncIterator

import pandas as pd
import structlog
from ionbeam_client import IonbeamClient
from ionbeam_client.arrow_tools import (
    dataframes_to_record_batches,
    schema_from_ingestion_map,
)
from ionbeam_client.constants import (
    LatitudeColumn,
    LongitudeColumn,
    ObservationTimestampColumn,
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

from .models import IonCannonConfig


class IonCannonSource:
    """
    IonCannon: A configurable load test source that generates synthetic data
    based on DataIngestionMap configurations.
    """

    def __init__(self, config: IonCannonConfig):
        self.config = config
        self.logger = structlog.get_logger(__name__)

        # Define metadata similar to netatmo structure
        self.metadata: IngestionMetadata = IngestionMetadata(
            dataset=DatasetMetadata(
                name="ioncannon",
                aggregation_span=timedelta(hours=1),
                description="Synthetic load test data for performance testing",
                source_links=[],
                keywords=["synthetic", "loadtest", "performance"],
            ),
            ingestion_map=DataIngestionMap(
                datetime=TimeAxis(from_col=ObservationTimestampColumn),
                lat=LatitudeAxis(standard_name="latitude", cf_unit="degrees_north"),
                lon=LongitudeAxis(standard_name="longitude", cf_unit="degrees_east"),
                canonical_variables=[
                    CanonicalVariable(
                        column="temperature",
                        standard_name="air_temperature",
                        cf_unit="degC",
                    ),
                    CanonicalVariable(
                        column="pressure", standard_name="air_pressure", cf_unit="Pa"
                    ),
                    CanonicalVariable(
                        column="humidity",
                        standard_name="relative_humidity",
                        cf_unit="1",
                    ),
                    CanonicalVariable(
                        column="wind_speed", standard_name="wind_speed", cf_unit="m s-1"
                    ),
                ],
                metadata_variables=[
                    MetadataVariable(column="station_id"),
                    MetadataVariable(column="sensor_type"),
                    MetadataVariable(column="location_type"),
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

        for var in self.metadata.ingestion_map.metadata_variables:
            column_name = var.column
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
                    ObservationTimestampColumn: timestamp,
                    LatitudeColumn: lat,
                    LongitudeColumn: lon,
                    "station_id": f"SYNTH_{station_id:04d}",
                }

                for var in self.metadata.ingestion_map.canonical_variables:
                    row[var.column] = random.uniform(0, 100)

                for var in self.metadata.ingestion_map.metadata_variables:
                    if var.column != "station_id":
                        row[var.column] = random.choice(
                            self._metadata_value_pools[var.column]
                        )

                rows.append(row)

            if rows:
                yield pd.DataFrame(rows)

    async def fetch(
        self,
        start_time: datetime,
        end_time: datetime,
        client: IonbeamClient,
    ) -> None:
        """Fetch and ingest IonCannon synthetic data for the given time window."""

        self.logger.info(
            "Starting IonCannon data generation",
            start=start_time.isoformat(),
            end=end_time.isoformat(),
        )

        schema = schema_from_ingestion_map(self.metadata.ingestion_map)

        async def dataframe_stream() -> AsyncIterator[pd.DataFrame]:
            async for df in self.generate_data_chunk(start_time, end_time):
                if df is not None and not df.empty:
                    yield df

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
            "IonCannon ingestion completed",
            start=start_time.isoformat(),
            end=end_time.isoformat(),
        )
