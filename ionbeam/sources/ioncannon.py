import random
import time
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Optional
from uuid import uuid4

import pandas as pd
from pydantic import BaseModel

from ionbeam.observability.metrics import IonbeamMetricsProtocol
from ionbeam.storage.arrow_store import ArrowStore
from ionbeam.utilities.arrow_tools import dataframes_to_record_batches, schema_from_ingestion_map

from ..core.constants import LatitudeColumn, LongitudeColumn, ObservationTimestampColumn
from ..core.handler import BaseHandler
from ..models.models import (
    DatasetMetadata,
    IngestDataCommand,
    IngestionMetadata,
    StartSourceCommand,
)
from .metno.netatmo import netatmo_metadata


class IonCannonConfig(BaseModel):
    # Core parameters
    num_stations: int = 10000
    measurement_frequency: timedelta = timedelta(minutes=5)

    # Geographic bounds for station placement (Central Europe)
    geographic_bounds: dict = {
        "min_lat": 47.0,
        "max_lat": 55.0,
        "min_lon": 5.0,
        "max_lon": 15.0,
    }


class IonCannonSource(BaseHandler[StartSourceCommand, Optional[IngestDataCommand]]):
    """
    IonCannon: A simple configurable load test source that generates synthetic data
    where shape is based on DataIngestionMap configurations.
    """

    def __init__(self, config: IonCannonConfig, metrics: IonbeamMetricsProtocol, arrow_store: ArrowStore):
        super().__init__("IonCannonSource", metrics)
        self._config = config
        self.arrow_store = arrow_store

        self.metadata: IngestionMetadata = IngestionMetadata(
            dataset=DatasetMetadata(
                name="ioncannon_loadtest",
                aggregation_span=timedelta(hours=1),
                description="Synthetic load test data for performance testing",
                source_links=[],
                keywords=["synthetic", "loadtest", "performance"],
            ),
            ingestion_map=netatmo_metadata.ingestion_map,
            version=1,
        )
        self._arrow_schema = schema_from_ingestion_map(self.metadata.ingestion_map)

    def _build_object_key(self, start_time: datetime, end_time: datetime) -> str:
        start_s = start_time.strftime("%Y%m%dT%H%M%SZ")
        end_s = end_time.strftime("%Y%m%dT%H%M%SZ")
        now_s = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"{self.metadata.dataset.name}/{start_s}-{end_s}_{now_s}"

    async def generate_data_chunk(self, start_time: datetime, end_time: datetime) -> AsyncIterator[pd.DataFrame]:
        """Generate synthetic data for the time window"""

        freq = self._config.measurement_frequency
        timestamps = pd.date_range(start_time, end_time, freq=freq, inclusive="left")

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
                    ObservationTimestampColumn: timestamp,
                    LatitudeColumn: lat,
                    LongitudeColumn: lon,
                    "station_id": f"SYNTH_{station_id:04d}",
                }

                # Generate simple random values for all canonical variables
                for var in self.metadata.ingestion_map.canonical_variables:
                    row[var.column] = random.uniform(0, 100)

                rows.append(row)

            if rows:
                yield pd.DataFrame(rows)

    async def _handle(self, event: StartSourceCommand) -> Optional[IngestDataCommand]:
        dataset_name = self.metadata.dataset.name
        request_start = time.perf_counter()
        total_rows = 0

        try:
            object_key = self._build_object_key(event.start_time, event.end_time)

            batch_stream = dataframes_to_record_batches(
                self.generate_data_chunk(event.start_time, event.end_time),
                schema=self._arrow_schema,
                preserve_index=False,
            )

            total_rows = await self.arrow_store.write_record_batches(
                object_key,
                batch_stream,
                schema=self._arrow_schema,
            )

            request_duration = time.perf_counter() - request_start
            self.metrics.sources.observe_fetch_duration(dataset_name, request_duration)
            self.metrics.sources.observe_request_rows(dataset_name, int(total_rows))

            if total_rows == 0:
                self.metrics.sources.record_ingestion_request(dataset_name, "empty")
                self.logger.warning("No data generated")
                return None

            self.metrics.sources.record_ingestion_request(dataset_name, "success")

            self.logger.info("Generated synthetic data", rows=total_rows, key=object_key)

            return IngestDataCommand(
                id=uuid4(),
                metadata=self.metadata,
                payload_location=object_key,
                start_time=event.start_time,
                end_time=event.end_time,
            )

        except Exception as exc:
            request_duration = time.perf_counter() - request_start
            self.metrics.sources.observe_fetch_duration(dataset_name, request_duration)
            self.metrics.sources.observe_request_rows(dataset_name, int(total_rows))
            self.metrics.sources.record_ingestion_request(dataset_name, "error")
            self.logger.exception("IonCannon failed to generate data", error=str(exc))
            return None
