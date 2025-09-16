"""
this is complicated and a bit unwieldly - this should provide good background:

This service implements a data aggregation process that handles out-of-order events, overlapping time spans, data gaps,
and the need to reprocess windows when new data arrives.

---

- Data is aggregated into fixed-duration windows (e.g., 1-hour periods).
    Windows are aligned to aggregation boundaries (e.g., 10:00-11:00, 11:00-12:00).

- Each ingestion event represents a time span of available data (start_time to end_time).
Events can:
    - Span multiple windows (e.g., 10:30-11:30 spans two 1-hour windows)
    - Arrive out of order (later events may fill gaps in earlier time period windows)
    - Overlap with other events (multiple events covering the same time range)

- Windows are only processed when they have complete data coverage
    from available events, with no gaps that would compromise data quality.

---

When a DataAvailableEvent arrives:
    - Store the event metadata in Redis for tracking
    - Load all existing events for the dataset
    - Analyze the overall time span and identify any data gaps

For each potential aggregation window:
    - Check if window is within subject-to-change cutoff (prevents processing recent data)
    - Validate complete coverage (events must span the entire window)
    - Verify no data gaps exist within the window
    - Determine if reprocessing is needed (new events affecting this window)

When a window should be processed:
    - Query InfluxDB for raw data within the window timeframe
    - Transform and aggregate the data
    - Store the aggregated dataset in File system
    - Emit a DataSetAvailableEvent for downstream consumers
    - Record which events contributed to this window in redis
    - Clean up processed InfluxDB data (but preserve event metadata)

- Redis keys used:
    - ingestion_events:{dataset}:{event_id}: Individual event metadata
    - window_events:{dataset}:{window_id}: List of event IDs used to process each window
"""

import json
import pathlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
from isodate import duration_isoformat
from pydantic import BaseModel

from ..core.constants import LatitudeColumn, LongitudeColumn, ObservationTimestampColumn
from ..core.handler import BaseHandler
from ..models.models import DataAvailableEvent, DataSetAvailableEvent, IngestionMetadata
from ..services.models import IngestionRecord
from ..storage.event_store import EventStore
from ..storage.timeseries import TimeSeriesDatabase
from ..utilities.dataframe_tools import coerce_types
from ..utilities.parquet_tools import stream_dataframes_to_parquet


@dataclass
class EventAggregateAnalysis:
    events: List[IngestionRecord]
    overall_start: Optional[datetime]
    overall_end: Optional[datetime]
    gaps: List[Tuple[datetime, datetime]]


class DatasetAggregatorConfig(BaseModel):
    delete_after_export: Optional[bool] = False
    data_path: pathlib.Path # aggregated data output
    page_size: int = 500  # Number of records to fetch per page


class DatasetAggregatorService(BaseHandler[DataAvailableEvent, AsyncIterator[DataSetAvailableEvent]]):
    def __init__(self, config: DatasetAggregatorConfig, event_store: EventStore, timeseries_db: TimeSeriesDatabase):
        super().__init__("DatasetAggregatorService")
        self.config = config
        self.event_store = event_store
        self.timeseries_db = timeseries_db

    def _align_to_aggregation(self, ts: datetime, aggregation: timedelta) -> datetime:
        """Align timestamp to aggregation boundary"""
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        delta = ts - epoch
        aligned_seconds = (delta.total_seconds() // aggregation.total_seconds()) * aggregation.total_seconds()
        return epoch + timedelta(seconds=aligned_seconds)

    def _get_window_id(self, window_start: datetime, aggregation: timedelta) -> str:
        """Generate consistent window identifier."""
        return f"{window_start.isoformat()}_{duration_isoformat(aggregation)}"

    async def _should_reprocess_window(
        self, 
        dataset_name: str, 
        window_start: datetime, 
        window_end: datetime,
        aggregation: timedelta,
        current_events: List[IngestionRecord]
    ) -> bool:
        """Check if window should be reprocessed due to new events."""
        window_id = self._get_window_id(window_start, aggregation)
        
        # Get events that were used in the last processing of this window
        last_events_key = f"window_events:{dataset_name}:{window_id}"
        last_event_ids_data = await self.event_store.get_event(last_events_key)
        
        if not last_event_ids_data:
            # Never processed before
            return True
        
        last_event_ids = set(json.loads(last_event_ids_data))
        
        # Get current events that overlap with this window
        current_window_events = [
            event for event in current_events 
            if event.start_time < window_end and event.end_time > window_start
        ]
        current_event_ids = {str(event.id) for event in current_window_events}
        
        # Reprocess if we have different events than last time
        if current_event_ids != last_event_ids:
            self.logger.info(f"Window {window_id} needs reprocessing - event set changed")
            self.logger.info(f"Previous events: {last_event_ids}")
            self.logger.info(f"Current events: {current_event_ids}")
            return True
        
        return False

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
        await self.event_store.store_event(events_key, json.dumps(event_ids))
        
        self.logger.info(f"Recorded events for window {window_id}: {event_ids}")

    async def _load_and_parse_data_events(self, dataset_name: str) -> EventAggregateAnalysis:
        """Load events from event store and calculate overall data span and gaps.
            - Events may arrive out of order and can backfill older periods.
            - Gaps are detected against the merged coverage (rolling max end).
            - Only gaps strictly larger than the threshold are recorded.
            - All datetimes are assumed to be timezone-aware UTC.
            
            At the moment due to implementation details any 'new' data will overwrite old data - 
            in future we may want to include a correlation-id which allows us to differentiate old from new data.
            If that is ever implemented, this logic will need to be updated as it only concerns itself with event start and end date, no the time (or id) at which the event arrived.
        """
        event_data_list = await self.event_store.get_events(f"ingestion_events:{dataset_name}:*")

        if not event_data_list:
            self.logger.warning(f"No previous events found for dataset: {dataset_name}")
            return EventAggregateAnalysis(events=[], overall_start=None, overall_end=None, gaps=[])

        events = [IngestionRecord.model_validate_json(event_data) for event_data in event_data_list]
        if not events:
            self.logger.warning(f"No valid events found for dataset: {dataset_name}")
            return EventAggregateAnalysis(events=[], overall_start=None, overall_end=None, gaps=[])

        # ordering first by start_time, then end_time
        events.sort(key=lambda e: (e.start_time, e.end_time))

        overall_start = events[0].start_time
        overall_end = max(e.end_time for e in events)
        self.logger.info(f"Total data span for {dataset_name}: {overall_start} to {overall_end}")

        gaps: List[Tuple[datetime, datetime]] = []
        # Only consider gaps larger than 1 second to avoid flagging normal timing variations - TODO - is this needed?
        min_gap_threshold = timedelta(seconds=1)

        # Rolling coverage end: track the furthest end covered so far
        coverage_end = events[0].end_time

        for e in events[1:]:
            if e.start_time > coverage_end:
                # There is uncovered time between coverage_end and this event's start
                gap_duration = e.start_time - coverage_end
                if gap_duration > min_gap_threshold:
                    gaps.append((coverage_end, e.start_time))
                    self.logger.warning(
                        f"Data gap detected in {dataset_name}: {coverage_end} to {e.start_time} "
                        f"(duration: {gap_duration})"
                    )
                # Advance coverage to this event (no overlap)
                coverage_end = e.end_time
            else:
                # Overlap or contiguity: extend coverage to the furthest end seen
                if e.end_time > coverage_end:
                    coverage_end = e.end_time

        return EventAggregateAnalysis(
            events=events,
            overall_start=overall_start,
            overall_end=overall_end,
            gaps=gaps,
        )

    def _should_process_window(
        self,
        window_start: datetime,
        window_end: datetime,
        stc_cutoff: datetime,
        gaps: List[Tuple[datetime, datetime]],
        events: List[IngestionRecord],
    ) -> bool:
        """Check if window should be processed - validates cutoff, gaps, and coverage"""
        if window_end >= stc_cutoff:
            self.logger.info(f"Skipping window ending at {window_end} due to subject-to-change cutoff.")
            return False

        if any(gap_start < window_end and gap_end > window_start for gap_start, gap_end in gaps):
            self.logger.warning(f"Skipping window {window_start} to {window_end} - contains data gaps")
            return False

        # Get events overlapping with window
        events_in_window = [event for event in events if event.start_time < window_end and event.end_time > window_start]

        if not events_in_window:
            self.logger.warning(f"Skipping window {window_start} to {window_end} - no events found")
            return False

        # Check if window is fully covered
        earliest_start = min(event.start_time for event in events_in_window)
        latest_end = max(event.end_time for event in events_in_window)

        if earliest_start > window_start or latest_end < window_end:
            self.logger.warning(f"Skipping window {window_start} to {window_end} - incomplete coverage (data: {earliest_start} to {latest_end})")
            return False

        return True

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
        self.logger.info(f"Creating dataset: {dataset_path}")
        
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

                # Ensure datetime dtype
                if ObservationTimestampColumn in df.columns and not pd.api.types.is_datetime64_any_dtype(df[ObservationTimestampColumn]):
                    df[ObservationTimestampColumn] = pd.to_datetime(df[ObservationTimestampColumn], utc=True, errors="coerce")

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
                # Flatten any MultiIndex columns from pivot_table
                if isinstance(df_wide.columns, pd.MultiIndex):
                    df_wide.columns = [c if not isinstance(c, tuple) else c[-1] for c in df_wide.columns]
                else:
                    df_wide.columns = [c if not isinstance(c, tuple) else c[-1] for c in df_wide.columns]

                # Type coercion (your existing cleaning)
                df_wide = coerce_types(df_wide, metadata.ingestion_map, True)
                return df_wide

            async for df_long in self.timeseries_db.query_measurement_data(
                measurement=dataset_name,
                start_time=window_start,
                end_time=window_end,
                slice_duration=timedelta(minutes=10)
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

        self.logger.info(f"Exported {total_rows} rows to filesystem: {dataset_path}")

        return DataSetAvailableEvent(
            id=uuid.uuid4(),
            metadata=metadata.dataset,
            dataset_location=dataset_path,
            start_time=window_start,
            end_time=window_end,
        )

    async def _cleanup_processed_data(
        self, dataset_name: str, window_start: datetime, window_end: datetime, events: List[IngestionRecord]
    ) -> List[IngestionRecord]:
        """Clean up time series data after successful export. Events are preserved for future reprocessing."""
        # Delete from time series database only
        if self.config.delete_after_export:
            await self.timeseries_db.delete_measurement_data(
                measurement=dataset_name,
                start_time=window_start,
                end_time=window_end
            )
            self.logger.info(f"Deleted data from time series DB for window: {window_start} to {window_end}")

        # Return all events unchanged - no deletion during aggregation
        # Events will be cleaned up by a separate maintenance process
        return events

    async def _handle(self, event: DataAvailableEvent) -> AsyncIterator[DataSetAvailableEvent]: # type: ignore
        """Main aggregation logic
            - This will check all dataset windows based on the events stored in redis, regardless of if the current event is relevant to that window or not
            - TODO: define TTLs on the redis keys to ensure we tidy up old event data (not currently implemented)"""
        metadata = event.metadata
        dataset_name = metadata.dataset.name
        self.logger.info(f"Aggregating dataset: {dataset_name}")

        # Save current ingestion event to event store for aggregation tracking
        event_key = f"ingestion_events:{metadata.dataset.name}:{event.id}"
        ingestion_event = IngestionRecord(id=event.id, metadata=metadata, start_time=event.start_time, end_time=event.end_time)
        await self.event_store.store_event(event_key, ingestion_event.model_dump_json())

        # Load all events and analyze data span/gaps
        parsed_events = await self._load_and_parse_data_events(dataset_name)
        if not parsed_events.events or parsed_events.overall_start is None or parsed_events.overall_end is None:
            return

        # Setup aggregation parameters
        now = datetime.now(timezone.utc)
        stc_cutoff = now - metadata.dataset.subject_to_change_window
        aggregation_span = metadata.dataset.aggregation_span
        start_time = self._align_to_aggregation(parsed_events.overall_start, aggregation_span)
        self.logger.info(f"Aligned start time for aggregation: {start_time}")

        # Process each aggregation window
        window_start = start_time
        while window_start < parsed_events.overall_end:
            window_end = window_start + aggregation_span

            # Check basic processing conditions (coverage, gaps, STC)
            if not self._should_process_window(window_start, window_end, stc_cutoff, parsed_events.gaps, parsed_events.events):
                window_start = window_end
                continue

            # Check if we need to reprocess due to new/changed events
            if not await self._should_reprocess_window(dataset_name, window_start, window_end, aggregation_span, parsed_events.events):
                self.logger.info(f"Skipping window {window_start} to {window_end} - no new events")
                window_start = window_end
                continue

            self.logger.info(f"Processing/reprocessing window for {dataset_name}: {window_start} to {window_end}")

            # Process window data
            dataset_event = await self._process_window_data(
                dataset_name, window_start, window_end, aggregation_span, metadata
            )

            if dataset_event:
                # Record which events were used and cleanup time series data only
                await self._record_window_events(dataset_name, window_start, aggregation_span, parsed_events.events)
                parsed_events.events = await self._cleanup_processed_data(
                    dataset_name, window_start, window_end, parsed_events.events
                )
                yield dataset_event

            window_start = window_end
