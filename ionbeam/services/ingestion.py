import asyncio
import time

import pandas as pd
import pyarrow as pa
from pydantic import BaseModel

from ionbeam.observability.metrics import IonbeamMetricsProtocol
from ionbeam.storage.arrow_store import ArrowStore

from ..core.constants import LatitudeColumn, LongitudeColumn, ObservationTimestampColumn
from ..core.handler import BaseHandler
from ..models.models import DataAvailableEvent, IngestDataCommand
from ..storage.timeseries import TimeSeriesDatabase
from ..utilities.dataframe_tools import coerce_types


class IngestionConfig(BaseModel):
    batch_size: int = 65536


class IngestionService(BaseHandler[IngestDataCommand, DataAvailableEvent]):
    def __init__(
        self,
        config: IngestionConfig,
        timeseries_db: TimeSeriesDatabase,
        metrics: IonbeamMetricsProtocol,
        arrow_store: ArrowStore,
    ):
        super().__init__("IngestionService", metrics)
        self.config = config
        self.timeseries_db = timeseries_db
        self.arrow_store = arrow_store

    async def _handle(self, event: IngestDataCommand) -> DataAvailableEvent:
        self.logger.debug("Ingestion config", batch_size=self.config.batch_size)

        dataset_name = event.metadata.dataset.name
        ingest_started = time.perf_counter()

        ingestion_map = event.metadata.ingestion_map
        time_col = ingestion_map.datetime.from_col or ObservationTimestampColumn
        lat_col = ingestion_map.lat.from_col or LatitudeColumn
        lon_col = ingestion_map.lon.from_col or LongitudeColumn

        # Read Arrow batches from object store
        batch_stream = self.arrow_store.read_record_batches(
            event.payload_location,
            batch_size=self.config.batch_size
        )
        
        total_points = 0
        batch_num = 0
        actual_start_time = None
        actual_end_time = None
        
        async for batch in batch_stream:
            await asyncio.sleep(0)  # keep event loop responsive
            
            # Convert Arrow batch to pandas for processing
            df_chunk = batch.to_pandas(types_mapper={pa.string(): pd.StringDtype(storage="python")}.get)
            df_chunk = coerce_types(df_chunk, ingestion_map)

            for col in (time_col, lat_col, lon_col):
                if col not in df_chunk.columns:
                    raise ValueError(f"Required axis column '{col}' missing in batch {batch_num + 1}")

            canonical_vars = []
            for var in ingestion_map.canonical_variables:
                if var.column in df_chunk.columns:
                    canonical_vars.append((var.column, var.to_canonical_name(), var.dtype))

            metadata_vars = [m.column for m in ingestion_map.metadata_variables if m.column in df_chunk.columns]

            keep_cols = [time_col, lat_col, lon_col] + [src for (src, _, _) in canonical_vars] + metadata_vars
            df_chunk = df_chunk[keep_cols]

            rename_map = {src: field for (src, field, _) in canonical_vars}
            if rename_map:
                df_chunk.rename(columns=rename_map, inplace=True)

            if lat_col != LatitudeColumn:
                df_chunk.rename(columns={lat_col: LatitudeColumn}, inplace=True)
            if lon_col != LongitudeColumn:
                df_chunk.rename(columns={lon_col: LongitudeColumn}, inplace=True)

            df_chunk[time_col] = pd.to_datetime(df_chunk[time_col], utc=True, errors="coerce") # TODO - is this needed?
            df_chunk.dropna(subset=[time_col], inplace=True)
            df_chunk.sort_values([time_col], kind="mergesort", inplace=True) # this is only sorting a chunk - not the full DF but that' doesn't matter as we only sort to speed up writes to influxdb

            field_cols = [LatitudeColumn, LongitudeColumn] + [f for (_, f, _) in canonical_vars]
            df_chunk.dropna(subset=field_cols, how="all", inplace=True)

            n_points = len(df_chunk)
            if n_points == 0:
                self.logger.info("No points to write in batch; skipping", batch=batch_num + 1)
                batch_num += 1
                continue

            # Track actual time bounds from the data
            chunk_times = df_chunk[time_col]
            chunk_min = chunk_times.min()
            chunk_max = chunk_times.max()
            
            if actual_start_time is None or chunk_min < actual_start_time:
                actual_start_time = chunk_min
            if actual_end_time is None or chunk_max > actual_end_time:
                actual_end_time = chunk_max

            self.logger.info("Writing batch", batch=batch_num + 1, points=n_points)
            await self.timeseries_db.write_dataframe(
                record=df_chunk,
                measurement_name=event.metadata.dataset.name,
                tag_columns=metadata_vars,
                timestamp_column=time_col,
            )
            total_points += n_points
            batch_num += 1

        self.metrics.ingestion.observe_dataset_points(dataset_name, total_points)
        self.metrics.ingestion.observe_dataset_duration(dataset_name, time.perf_counter() - ingest_started)
        
        # Use actual data time bounds if available, otherwise fall back to command times
        final_start_time = actual_start_time if actual_start_time is not None else event.start_time
        final_end_time = actual_end_time if actual_end_time is not None else event.end_time
        
        # Log if there's a discrepancy between declared and actual time bounds
        if actual_start_time is not None and actual_end_time is not None:
            if final_start_time != event.start_time or final_end_time != event.end_time:
                self.logger.warning(
                    "Actual data time bounds differ from command",
                    dataset=dataset_name,
                    command_start=event.start_time.isoformat(),
                    command_end=event.end_time.isoformat(),
                    actual_start=final_start_time.isoformat(),
                    actual_end=final_end_time.isoformat(),
                )
        
        return DataAvailableEvent(
            id=event.id,
            metadata=event.metadata,
            start_time=final_start_time,
            end_time=final_end_time,
        )
