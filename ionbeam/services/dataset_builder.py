"""
Dataset Builder

Consumes prioritized window build requests from the coordinator's queue and builds
datasets in the background. Intended to be run as long-lived worker(s).

Queue model:
- Stored as a JSON mapping in EventStore under `queue_key`
  { "{dataset}:{window_id}": score, ... }
- The worker picks the dataset key with the highest score (TODO ZPOPMAX),
  removes it from the queue, acquires a lock, and builds.

Observed state:
- After a successful build, the worker writes the observed state for the window:
  key: {dataset}:{window_id}:state
  value: {"event_ids_hash": "...", "version": 1, "timestamp": "..."}

Desired state:
- Latest set of event IDs contributing to the window:
  key: {dataset}:{window_id}:event_ids
  value: JSON list of event IDs

Locks (best-effort, cooperative):
- key: lock:{dataset}:{window_id}
- Implemented by writing a timestamp with TTL; (TODO atmoic redis).
"""

import asyncio
import json
import pathlib
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import structlog
from faststream.rabbit import RabbitBroker
from isodate import duration_isoformat
from pydantic import BaseModel

from ionbeam.models.models import DataSetAvailableEvent, IngestionMetadata
from ionbeam.utilities.dataframe_tools import coerce_types
from ionbeam.utilities.parquet_tools import stream_dataframes_to_parquet

from ..core.constants import LatitudeColumn, LongitudeColumn, MaximumCachePeriod, ObservationTimestampColumn
from ..services.models import IngestionRecord, LockInfo, WindowBuildState, WindowRef
from ..storage.event_store import EventStore
from ..storage.timeseries import TimeSeriesDatabase


class DatasetBuilderConfig(BaseModel):
    queue_key: str = "dataset_queue"
    poll_interval_seconds: float = 3.0
    lock_ttl_seconds: int = 15 * 60  # 15 minutes
    data_path: pathlib.Path = pathlib.Path("data/aggregated")
    delete_after_export: Optional[bool] = False
    concurrency: int = 2  # Number of windows to build concurrently per worker


class DatasetBuilder:
    """
    Background worker that continuously builds datasets from the coordinator's queue.
    """

    def __init__(
        self,
        config: DatasetBuilderConfig,
        event_store: EventStore,
        timeseries_db: TimeSeriesDatabase,
        broker: Optional[RabbitBroker] = None,
    ) -> None:
        self.config = config
        self.event_store = event_store
        self.timeseries_db = timeseries_db
        self.broker = broker
        self._stop = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._inflight: set[asyncio.Task] = set()
        self.logger = structlog.get_logger(__name__)

    async def _process_window_data(
        self,
        dataset_name: str,
        window_start: datetime,
        window_end: datetime,
        aggregation: timedelta,
        metadata: IngestionMetadata,
    ) -> Optional[DataSetAvailableEvent]:
        """Fetch data from time series DB using streaming, and create dataset event."""
        
        # Generate dataset filename
        self.config.data_path.mkdir(parents=True, exist_ok=True)
        dataset_filename = f"{dataset_name}_{window_start.strftime('%Y%m%dT%H%M%S')}_{duration_isoformat(aggregation)}.parquet"
        dataset_path = self.config.data_path / dataset_filename
        self.logger.info("Creating dataset file", path=str(dataset_path))
        
        # Build schema from ingestion_map
        schema_fields: List[Tuple[str, pa.DataType]] = [
            (ObservationTimestampColumn, pa.timestamp('ns', tz='UTC')),
            (LatitudeColumn, pa.float64()),
            (LongitudeColumn, pa.float64()),
        ]
        for var in metadata.ingestion_map.canonical_variables + metadata.ingestion_map.metadata_variables:
            pa_type = pa.string() if var.dtype == "string" or var.dtype == "object" else pa.from_numpy_dtype(np.dtype(var.dtype))
            schema_fields.append((var.to_canonical_name(), pa_type))

        async def processed_dataframe_stream():
            """Stream and process data from time series database."""
            
            def _to_df(df_long: pd.DataFrame) -> pd.DataFrame:
                if df_long is None or df_long.empty:
                    return pd.DataFrame()

                # Keep long-form columns + tags; normalize
                df = df_long.drop(columns=["result", "table", "_start", "_stop"], errors="ignore")
                df = df.rename(columns={"_time": ObservationTimestampColumn, "_measurement": "source"})

                if df.empty:
                    return df

                # Derive tag columns automatically: non-underscore, not helper columns
                helper = {ObservationTimestampColumn, "source", "_field", "_value"}
                tag_cols = [c for c in df.columns if not c.startswith("_") and c not in helper]

                index_cols = [ObservationTimestampColumn, "source"] + tag_cols
                df_wide = (
                    df.pivot_table(
                        index=index_cols,
                        columns="_field",
                        values="_value",
                        aggfunc="last"  # de-dup within the same ts/tags/field if needed
                    )
                    .reset_index()
                )
                df_wide.columns = [c if not isinstance(c, tuple) else c[-1] for c in df_wide.columns]

                # Type coercion (your existing cleaning)
                df_wide = coerce_types(df_wide, metadata.ingestion_map, True)
                return df_wide

            async for df_long in self.timeseries_db.query_measurement_data(
                measurement=dataset_name,
                start_time=window_start,
                end_time=window_end,
                slice_duration=timedelta(minutes=15)
            ):
                df_wide = _to_df(df_long)
                if df_wide is not None and not df_wide.empty:
                    yield df_wide

        # Stream data to parquet file
        total_rows = await stream_dataframes_to_parquet(
            processed_dataframe_stream(),
            dataset_path,
            schema_fields
        )

        if total_rows == 0:
            self.logger.info(f"No data found in time series DB for window: {window_start} to {window_end}")
            # TODO this could overwrite already created dataset files with empty parquets...
            return None

        self.logger.info("Wrote parquet file", rows=total_rows, path=str(dataset_path))

        return DataSetAvailableEvent(
            id=uuid.uuid4(),
            metadata=metadata.dataset,
            dataset_location=dataset_path,
            start_time=window_start,
            end_time=window_end,
        )

    async def _publish_dataset_event(self, event: DataSetAvailableEvent) -> None:
        """Publish DataSetAvailableEvent to the dataset fanout exchange if broker configured."""
        if not self.broker:
            self.logger.warning("RabbitBroker not configured; dataset event not published")
            return
        try:
            await self.broker.publish(event, exchange="ionbeam.dataset.available")
            self.logger.info(
                "Published dataset available event",
                dataset=str(event.metadata.name),
                start_time=event.start_time.isoformat(),
                end_time=event.end_time.isoformat(),
            )
        except Exception:
            self.logger.exception("Failed to publish dataset available event")

    # Lifecycle ---------------------------------------------------------------

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
        # Wait for in-flight builds to finish
        if self._inflight:
            await asyncio.gather(*self._inflight, return_exceptions=True)
            self._inflight.clear()
        self.logger.info("DatasetBuilder stopped")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()

    # Queue helpers -----------------------------------------------------------

    async def _get_queue(self) -> Dict[str, int]:
        raw = await self.event_store.get_event(self.config.queue_key)
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except Exception:
            self.logger.exception("Failed to parse queue mapping; resetting", key=self.config.queue_key)
            return {}

    async def _set_queue(self, queue: Dict[str, int]) -> None:
        await self.event_store.store_event(self.config.queue_key, json.dumps(queue))

    async def _pop_highest_priority(self) -> Optional[Tuple[str, int]]:
        """
        Emulates Redis ZPOPMAX: returns (dataset_key, score) with highest score and removes it from the queue.
        Not atomic; best-effort for single-worker usage.
        TODO: Use Redis ZPOPMAX for atomic pop when EventStore supports it.
        """
        queue = await self._get_queue()
        if not queue:
            return None
        dataset_key = max(queue, key=lambda k: queue[k])
        score = queue.pop(dataset_key)
        await self._set_queue(queue)
        return dataset_key, score

    async def _requeue(self, dataset_key: str, score: int) -> None:
        queue = await self._get_queue()
        queue[dataset_key] = score
        await self._set_queue(queue)

    # Parsing and state helpers ----------------------------------------------
    def _get_window_id(self, window_start: datetime, aggregation: timedelta) -> str:
        """Generate consistent window identifier."""
        return f"{window_start.isoformat()}_{duration_isoformat(aggregation)}"
    async def _record_window_events(
            self, 
            dataset_name: str, 
            window_start: datetime, 
            aggregation: timedelta,
            events: List[IngestionRecord]
        ):
            """Record which events were used to process this window."""
            window_id = self._get_window_id(window_start, aggregation)
            window_end = window_start + aggregation
            
            # Get events that overlap with this window
            window_events = [
                event for event in events 
                if event.start_time < window_end and event.end_time > window_start
            ]
            event_ids = [str(event.id) for event in window_events]
            
            # Store the event IDs used for this window
            events_key = f"window_events:{dataset_name}:{window_id}"
            await self.event_store.store_event(
                events_key,
                json.dumps(event_ids),
                ttl=int(MaximumCachePeriod.total_seconds())
            )
            
            self.logger.info("Recorded window events", window_id=window_id, event_ids=event_ids)


    async def _acquire_lock(self, dataset: str, window_id: str) -> bool:
        """
        Cooperative lock using EventStore. Not atomic across processes.
        TODO: Replace with Redis SET NX + TTL for atomicity across workers.
        """
        key = f"lock:{dataset}:{window_id}"
        existing = await self.event_store.get_event(key)
        if existing:
            return False
        await self.event_store.store_event(
            key,
            LockInfo(locked_at=datetime.now(timezone.utc)).model_dump_json(),
            ttl=self.config.lock_ttl_seconds,
        )
        return True

    async def _release_lock(self, dataset: str, window_id: str) -> None:
        key = f"lock:{dataset}:{window_id}"
        # TODO: No explicit delete API; store with very short TTL to expire.
        await self.event_store.store_event(
            key,
            LockInfo(released_at=datetime.now(timezone.utc)).model_dump_json(),
            ttl=1,
        )

    async def _get_window_event_ids_aggregate(self, dataset: str, window_id: str) -> List[str]:
        key = f"{dataset}:{window_id}:event_ids"
        data = await self.event_store.get_event(key)
        if not data:
            return []
        try:
            ids = json.loads(data)
            return [str(i) for i in ids]
        except Exception:
            self.logger.exception("Failed to parse desired event_ids", key=key)
            return []

    async def _get_latest_build_state(self, dataset: str, window_id: str) -> Optional[str]:
        key = f"{dataset}:{window_id}:state"
        data = await self.event_store.get_event(key)
        if not data:
            return None
        try:
            state = WindowBuildState.model_validate_json(data)
            return state.event_ids_hash
        except Exception:
            self.logger.exception("Failed to parse observed state", key=key)
            return None

    async def _set_observed_hash(self, dataset: str, window_id: str, event_ids_hash: str) -> None:
        key = f"{dataset}:{window_id}:state"
        state = WindowBuildState(event_ids_hash=event_ids_hash, timestamp=datetime.now(timezone.utc))
        await self.event_store.store_event(key, state.model_dump_json(), ttl=int(MaximumCachePeriod.total_seconds()))

    def _hash_ids(self, ids: List[str]) -> str:
        import hashlib

        joined = ",".join(sorted(ids))
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    async def _load_ingestion_records(self, dataset: str) -> List[IngestionRecord]:
        vals = await self.event_store.get_events(f"ingestion_events:{dataset}:*")
        if not vals:
            return []
        try:
            return [IngestionRecord.model_validate_json(v) for v in vals]
        except Exception:
            self.logger.exception("Failed to parse ingestion records", dataset=dataset)
            return []

    async def _select_latest_metadata(self, dataset: str):
        records = await self._load_ingestion_records(dataset)
        if not records:
            return None, []
        records.sort(key=lambda r: (r.end_time, r.start_time))
        latest = records[-1]
        return latest.metadata, records

    # Build one window ---------------------------------------------------------

    async def _build_dataset(self, dataset_key: str, score: int) -> None:
        parsed = WindowRef.from_dataset_key(dataset_key)
        if not parsed:
            return

        # Acquire lock
        locked = await self._acquire_lock(parsed.dataset, parsed.window_id)
        if not locked:
            self.logger.info("Window already locked, skipping", dataset_key=dataset_key)
            return

        try:
            # Compare desired vs observed
            desired_event_ids = await self._get_window_event_ids_aggregate(parsed.dataset, parsed.window_id)
            desired_hash = self._hash_ids(desired_event_ids)
            observed_hash = await self._get_latest_build_state(parsed.dataset, parsed.window_id)
            if observed_hash == desired_hash:
                self.logger.info("Observed state up-to-date; skipping build", dataset_key=dataset_key)
                return

            # Load metadata and events
            metadata, all_records = await self._select_latest_metadata(parsed.dataset)
            if metadata is None:
                self.logger.warning("No metadata available; requeueing window", dataset_key=dataset_key)
                await self._requeue(dataset_key, score)
                return

            # Compute window end
            window_end = parsed.window_start + parsed.aggregation

            # Perform the build using the existing aggregation routine
            dataset_event = await self._process_window_data(
                parsed.dataset,
                parsed.window_start,
                window_end,
                parsed.aggregation,
                metadata,
            )

            if not dataset_event:
                # No data available; mark observed state to avoid repeated empty rebuilds.
                self.logger.info("No data written for window; but marked observed state as satisfied", dataset_key=dataset_key)

            # Record contributing events for compatibility with legacy keys
            try:
                await self._record_window_events( 
                    parsed.dataset,
                    parsed.window_start,
                    parsed.aggregation,
                    all_records,
                )
            except Exception:
                # Non-fatal: proceed even if compatibility recording fails
                self.logger.exception("Failed to record legacy window events", dataset_key=dataset_key)

            # Write observed state so coordinator sees window as satisfied
            await self._set_observed_hash(parsed.dataset, parsed.window_id, desired_hash)

            # Publish dataset available event for downstream consumers
            try:
                if dataset_event:
                    await self._publish_dataset_event(dataset_event)
                else:
                    self.logger.debug("No dataset_event to publish")
            except Exception:
                self.logger.exception("Error while publishing dataset available event", dataset_key=dataset_key)

            self.logger.info(
                "Built window and updated observed state",
                dataset=parsed.dataset,
                window_id=parsed.window_id,
                window_start=parsed.window_start.isoformat(),
                window_end=window_end.isoformat(),
            )

        except Exception:
            # On failure, requeue to try again later
            self.logger.exception("Build failed; requeueing", dataset_key=dataset_key)
            await self._requeue(dataset_key, score)
        finally:
            await self._release_lock(parsed.dataset, parsed.window_id)

    # Main worker loop ---------------------------------------------------------

    async def _run(self) -> None:
        try:
            while not self._stop.is_set():
                # Fill up to concurrency with new builds
                started_any = False
                while len(self._inflight) < max(1, self.config.concurrency):
                    popped = await self._pop_highest_priority()
                    if not popped:
                        break
                    dataset_key, score = popped
                    started_any = True
                    task = asyncio.create_task(self._build_dataset(dataset_key, score))
                    self._inflight.add(task)
                    task.add_done_callback(self._inflight.discard)

                if not started_any and not self._inflight:
                    # Idle: nothing to do
                    await asyncio.sleep(self.config.poll_interval_seconds)
                else:
                    # Allow in-flight tasks to progress
                    await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            # Graceful shutdown
            pass
        except Exception:
            self.logger.exception("DatasetBuilder worker loop crashed")
        finally:
            # Drain in-flight tasks before exit
            if self._inflight:
                await asyncio.gather(*self._inflight, return_exceptions=True)
                self._inflight.clear()
