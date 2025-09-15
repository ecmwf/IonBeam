import pathlib
import random
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, List, Optional, Tuple
from uuid import uuid4

import numpy as np
import pandas as pd
import pyarrow as pa
from pydantic import BaseModel

from ionbeam.core.handler import BaseHandler
from ionbeam.utilities.parquet_tools import stream_dataframes_to_parquet

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


class IonCannonConfig(BaseModel):
    data_path: pathlib.Path
    
    # Core parameters
    num_stations: int = 1000
    measurement_frequency: timedelta = timedelta(minutes=10)
    
    # Geographic bounds for station placement (Central Europe)
    geographic_bounds: dict = {
        "min_lat": 47.0, "max_lat": 55.0,
        "min_lon": 5.0, "max_lon": 15.0
    }


class IonCannonSource(BaseHandler[StartSourceCommand, Optional[IngestDataCommand]]):
    """
    IonCannon: A simple configurable load test source that generates synthetic data
    where shape is based on DataIngestionMap configurations.
    """
    
    def __init__(self, config: IonCannonConfig):
        super().__init__("IonCannonSource")
        self._config = config
        
        self.metadata: IngestionMetadata = IngestionMetadata(
            dataset=DatasetMetadata(
                name="ioncannon_loadtest",
                aggregation_span=timedelta(hours=1),
                description="Synthetic load test data for performance testing",
                source_links=[],
                keywords=["synthetic", "loadtest", "performance"],
            ),
            ingestion_map=DataIngestionMap(
                datetime=TimeAxis(),
                lat=LatitudeAxis(standard_name="latitude", cf_unit="degrees_north"),
                lon=LongitudeAxis(standard_name="longitude", cf_unit="degrees_east"),
                canonical_variables=[
                    CanonicalVariable(
                        column="air_pressure_at_mean_sea_level_2_0_mean_PT1H",
                        standard_name="air_pressure_at_mean_sea_level",
                        cf_unit="hPa",
                        level=2.0,
                        method="mean",
                        period="PT1H",
                    ),
                    CanonicalVariable(
                        column="air_temperature_2_0_maximum_PT1H",
                        standard_name="air_temperature",
                        cf_unit="degC",
                        level=2.0,
                        method="maximum",
                        period="PT1H",
                    ),
                    CanonicalVariable(
                        column="air_temperature_2_0_minimum_PT1H",
                        standard_name="air_temperature",
                        cf_unit="degC",
                        level=2.0,
                        method="minimum",
                        period="PT1H",
                    ),
                    CanonicalVariable(
                        column="air_temperature_2_0_point_PT0S",
                        standard_name="air_temperature",
                        cf_unit="degC",
                        level=2.0,
                        method="point",
                        period="PT0S",
                    ),
                    CanonicalVariable(
                        column="dew_point_temperature_2_0_point_PT0S",
                        standard_name="dew_point_temperature",
                        cf_unit="degC",
                        level=2.0,
                        method="point",
                        period="PT0S",
                    ),
                    CanonicalVariable(
                        column="relative_humidity_2_0_point_PT0S",
                        standard_name="relative_humidity",
                        cf_unit="1",
                        level=2.0,
                        method="point",
                        period="PT0S",
                    ),
                    CanonicalVariable(
                        column="solar_irradiance_2_0_mean_PT1H",
                        standard_name="solar_irradiance",
                        cf_unit="W m-2",
                        level=2.0,
                        method="mean",
                        period="PT1H",
                    ),
                    CanonicalVariable(
                        column="surface_air_pressure_2_0_point_PT0S",
                        standard_name="surface_air_pressure",
                        cf_unit="hPa",
                        level=2.0,
                        method="point",
                        period="PT0S",
                    ),
                    CanonicalVariable(
                        column="wind_from_direction_10_0_mean_PT1H",
                        standard_name="wind_from_direction",
                        cf_unit="degree",
                        level=10.0,
                        method="mean",
                        period="PT1H",
                    ),
                    CanonicalVariable(
                        column="wind_speed_10_0_mean_PT1H",
                        standard_name="wind_speed",
                        cf_unit="m s-1",
                        level=10.0,
                        method="mean",
                        period="PT1H",
                    ),
                ],
                metadata_variables=[
                    MetadataVariable(column="station_id"),
                ],
            ),
            version=1,
        )

    async def generate_data_chunk(self, start_time: datetime, end_time: datetime) -> AsyncIterator[pd.DataFrame]:
        """Generate synthetic data for the time window"""
        
        freq = self._config.measurement_frequency
        timestamps = pd.date_range(start_time, end_time, freq=freq, inclusive='left')
        
        if len(timestamps) == 0:
            return
        
        bounds = self._config.geographic_bounds
        
        # Generate data for each station
        for station_id in range(self._config.num_stations):
            rows = []
            
            # Generate random station location
            lat = random.uniform(bounds["min_lat"], bounds["max_lat"])
            lon = random.uniform(bounds["min_lon"], bounds["max_lon"])
            
            for timestamp in timestamps:
                row = {
                    "datetime": timestamp,
                    "lat": lat,
                    "lon": lon,
                    "station_id": f"SYNTH_{station_id:04d}",
                }
                
                # Generate simple random values for all canonical variables
                for var in self.metadata.ingestion_map.canonical_variables:
                    row[var.column] = random.uniform(0, 100)
                
                rows.append(row)
            
            if rows:
                yield pd.DataFrame(rows)

    async def _handle(self, event: StartSourceCommand) -> Optional[IngestDataCommand]:
        self.logger.info(f"Starting IonCannon load test: {event}")
        
        try:
            self._config.data_path.mkdir(parents=True, exist_ok=True)
            path = (
                self._config.data_path / 
                f"{self.metadata.dataset.name}_{event.start_time}-{event.end_time}_{datetime.now(timezone.utc)}.parquet"
            )

            # Build schema from ingestion_map (same as other sources)
            schema_fields: List[Tuple[str, pa.DataType]] = [
                (self.metadata.ingestion_map.datetime.from_col or 'datetime', pa.timestamp('ns', tz='UTC')),
                (self.metadata.ingestion_map.lat.from_col or 'lat', pa.float64()),
                (self.metadata.ingestion_map.lon.from_col or 'lon', pa.float64()),
            ]

            for var in self.metadata.ingestion_map.canonical_variables + self.metadata.ingestion_map.metadata_variables:
                pa_type = pa.string() if var.dtype == "string" or var.dtype == "object" else pa.from_numpy_dtype(np.dtype(var.dtype))
                schema_fields.append((var.column, pa_type))

            # Stream synthetic data to parquet
            total_rows = await stream_dataframes_to_parquet(
                self.generate_data_chunk(event.start_time, event.end_time),
                path,
                schema_fields
            )

            if total_rows == 0:
                self.logger.warning("No data generated")
                return None

            self.logger.info(f"Generated {total_rows} synthetic rows to {path}")

            return IngestDataCommand(
                id=uuid4(),
                metadata=self.metadata,
                payload_location=path,
                start_time=event.start_time,
                end_time=event.end_time,
            )

        except Exception as e:
            self.logger.exception(e)
            return None