"""
Dataset Builder

Consumes prioritized window build requests from the coordinator's queue and builds
datasets in the background. Intended to be run as long-lived worker(s).
"""

import asyncio
import pathlib
import time
import uuid
from datetime import timedelta
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import structlog
from faststream.rabbit import RabbitBroker
from pydantic import BaseModel

from ionbeam.models.models import DataSetAvailableEvent, IngestionMetadata
from ionbeam.observability.metrics import IonbeamMetricsProtocol
from ionbeam.utilities.dataframe_tools import coerce_types
from ionbeam.utilities.parquet_tools import stream_dataframes_to_parquet

from ..core.constants import LatitudeColumn, LongitudeColumn, ObservationTimestampColumn
from ..services.models import Window
from ..storage.ingestion_record_store import IngestionRecordStore
from ..storage.ordered_queue import OrderedQueue
from ..storage.timeseries import TimeSeriesDatabase
from .dataset_coordinator import WindowStateManager


class DatasetBuilderConfig(BaseModel):
    queue_key: str = "dataset_queue"
    poll_interval_seconds: float = 3.0
    data_path: pathlib.Path = pathlib.Path("data/aggregated")
    delete_after_export: Optional[bool] = False
    concurrency: int = 2


class DatasetBuilder:
    """Background worker that builds datasets from queued windows."""
    
    def __init__(
        self,
        config: DatasetBuilderConfig,
        record_store: IngestionRecordStore,
        queue: OrderedQueue,
        timeseries_db: TimeSeriesDatabase,
        metrics: IonbeamMetricsProtocol,
        broker: Optional[RabbitBroker] = None,
    ) -> None:
        self.config = config
        self.state = WindowStateManager(record_store, queue)
        self.timeseries_db = timeseries_db
        self.broker = broker
        self.metrics = metrics
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._inflight: set[asyncio.Task] = set()
        self.logger = structlog.get_logger(__name__)
    
    async def build_window(self, window: Window, priority: int) -> None:
        """Build a single window."""
        self.metrics.builders.build_started(window.dataset)
        start_time = time.perf_counter()
        
        try:
            desired = await self.state.get_desired_events(window)
            observed_hash = await self.state.get_observed_hash(window)
            
            if observed_hash == desired.hash:
                self.logger.info("Window up-to-date", window=window.dataset_key)
                self.metrics.builders.build_succeeded(window.dataset)
                return
            
            records = await self.state.get_ingestion_records(window.dataset)
            if not records:
                self.logger.warning("No metadata available", window=window.dataset_key)
                await self.state.enqueue_window(window, priority)
                self.metrics.builders.requeued(window.dataset, "missing_metadata")
                return
            
            latest = max(records, key=lambda r: (r.end_time, r.start_time))
            metadata = latest.metadata
            
            dataset_event = await self._build_dataset_file(window, metadata)
            
            if dataset_event:
                await self._publish_event(dataset_event)
            
            await self.state.set_observed_hash(window, desired.hash)
            
            self.metrics.builders.build_succeeded(window.dataset)
            self.logger.info(
                "Built window successfully",
                dataset=window.dataset,
                window_start=window.start.isoformat(),
                window_end=window.end.isoformat(),
            )
            
        except Exception:
            self.logger.exception("Build failed", window=window.dataset_key)
            await self.state.enqueue_window(window, priority)
            self.metrics.builders.requeued(window.dataset, "exception")
            self.metrics.builders.build_failed(window.dataset)
        finally:
            duration = time.perf_counter() - start_time
            self.metrics.builders.observe_build_duration(window.dataset, duration)
    
    async def _build_dataset_file(
        self,
        window: Window,
        metadata: IngestionMetadata,
    ) -> Optional[DataSetAvailableEvent]:
        """Fetch data from time series DB using streaming, and create dataset event."""
        
        self.config.data_path.mkdir(parents=True, exist_ok=True)
        dataset_filename = f"{window.dataset}_{window.start.strftime('%Y%m%dT%H%M%S')}_{window.window_id.split('_')[1]}.parquet"
        dataset_path = self.config.data_path / dataset_filename
        self.logger.info("Creating dataset file", path=str(dataset_path))
        
        schema_fields: List[Tuple[str, pa.DataType]] = [
            (ObservationTimestampColumn, pa.timestamp("ns", tz="UTC")),
            (LatitudeColumn, pa.float64()),
            (LongitudeColumn, pa.float64()),
        ]
        for var in metadata.ingestion_map.canonical_variables + metadata.ingestion_map.metadata_variables:
            pa_type = (
                pa.string() if var.dtype == "string" or var.dtype == "object" else pa.from_numpy_dtype(np.dtype(var.dtype))
            )
            schema_fields.append((var.to_canonical_name(), pa_type))
        
        async def processed_dataframe_stream():
            """Stream and process data from time series database."""
            
            def _to_df(df_long: pd.DataFrame) -> pd.DataFrame:
                if df_long is None or df_long.empty:
                    return pd.DataFrame()
                
                df = df_long.drop(columns=["result", "table", "_start", "_stop"], errors="ignore")
                df = df.rename(columns={"_time": ObservationTimestampColumn, "_measurement": "source"})
                
                if df.empty:
                    return df
                
                helper = {ObservationTimestampColumn, "source", "_field", "_value"}
                tag_cols = [c for c in df.columns if not c.startswith("_") and c not in helper]
                
                index_cols = [ObservationTimestampColumn, "source"] + tag_cols
                df_wide = (
                    df.pivot_table(
                        index=index_cols,
                        columns="_field",
                        values="_value",
                        aggfunc="last",
                    )
                    .reset_index()
                )
                df_wide.columns = [c if not isinstance(c, tuple) else c[-1] for c in df_wide.columns]
                
                df_wide = coerce_types(df_wide, metadata.ingestion_map, True)
                return df_wide
            
            async for df_long in self.timeseries_db.query_measurement_data(
                measurement=window.dataset,
                start_time=window.start,
                end_time=window.end,
                slice_duration=timedelta(minutes=15),
            ):
                df_wide = _to_df(df_long)
                if df_wide is not None and not df_wide.empty:
                    yield df_wide
        
        total_rows = await stream_dataframes_to_parquet(
            processed_dataframe_stream(),
            dataset_path,
            schema_fields,
        )
        self.metrics.builders.observe_rows_exported(window.dataset, int(total_rows))
        
        if total_rows == 0:
            self.logger.info(
                "No data found in time series DB",
                window_start=window.start.isoformat(),
                window_end=window.end.isoformat()
            )
            return None
        
        self.logger.info("Wrote parquet file", rows=total_rows, path=str(dataset_path))
        
        return DataSetAvailableEvent(
            id=uuid.uuid4(),
            metadata=metadata.dataset,
            dataset_location=dataset_path,
            start_time=window.start,
            end_time=window.end,
        )
    
    async def _publish_event(self, event: DataSetAvailableEvent) -> None:
        """Publish DataSetAvailableEvent to the dataset fanout exchange if broker configured."""
        dataset_name = str(event.metadata.name)
        if not self.broker:
            self.metrics.builders.publish(dataset_name, "skipped")
            self.logger.warning("RabbitBroker not configured; dataset event not published")
            return
        try:
            await self.broker.publish(event, exchange="ionbeam.dataset.available")
            self.metrics.builders.publish(dataset_name, "success")
            self.logger.info(
                "Published dataset available event",
                dataset=dataset_name,
                start_time=event.start_time.isoformat(),
                end_time=event.end_time.isoformat(),
            )
        except Exception:
            self.metrics.builders.publish(dataset_name, "failure")
            self.logger.exception("Failed to publish dataset available event")
    
    async def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="DatasetBuilderWorker")
            self.logger.info("DatasetBuilder started")
    
    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._inflight:
            await asyncio.gather(*self._inflight, return_exceptions=True)
            self._inflight.clear()
        self.logger.info("DatasetBuilder stopped")
    
    async def __aenter__(self):
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()
    
    async def _run(self) -> None:
        """Main worker loop."""
        try:
            while not self._stop.is_set():
                while len(self._inflight) < max(1, self.config.concurrency):
                    item = await self.state.queue.dequeue_highest_priority()
                    if not item:
                        break
                    
                    window, priority = item
                    task = asyncio.create_task(self.build_window(window, priority))
                    self._track_task(task)
                
                if not self._inflight:
                    await asyncio.sleep(self.config.poll_interval_seconds)
                else:
                    await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass
        except Exception:
            self.logger.exception("DatasetBuilder worker loop crashed")
        finally:
            if self._inflight:
                await asyncio.gather(*self._inflight, return_exceptions=True)
                self._inflight.clear()
    
    def _track_task(self, task: asyncio.Task) -> None:
        self._inflight.add(task)
        task.add_done_callback(lambda _: self._inflight.discard(task))
