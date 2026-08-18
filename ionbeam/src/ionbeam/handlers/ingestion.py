# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Awaitable, Callable, Optional, Tuple
from uuid import UUID, uuid5

import numpy as np
import pandas as pd
import pyarrow as pa
import structlog
from ionbeam_client.models import DataAvailableEvent, IngestionMetadata, WindowRecord

from ionbeam.datasets import DatasetBuildConfig, DatasetRegistry
from ionbeam.handlers.canonicalize import CanonicalBatch, canonicalize
from ionbeam.handlers.schema_contract import verify_stream_schema
from ionbeam.provenance import align_to_aggregation
from ionbeam.observability import IngestionMetrics
from ionbeam.observability.timing import async_timer
from ionbeam.storage.fingerprints import row_fingerprints
from ionbeam.storage.coordination_store import CoordinationStore
from ionbeam.storage.lateness_histogram import bucket_counts
from ionbeam.storage.timeseries import RECORD_ID_COLUMN, TimeSeriesDatabase

DataAvailablePublisher = Callable[[DataAvailableEvent], Awaitable[None]]

# margin past a window's seal before its stored-content set is forgotten;
# keeps a row deemed live against the seal from racing a just-expired set
_FILTER_EXPIRY_MARGIN = timedelta(hours=1)


def _filter_expiry(window_start_s: int, seal_delay_s: int) -> datetime:
    return (
        datetime.fromtimestamp(window_start_s + seal_delay_s, tz=timezone.utc)
        + _FILTER_EXPIRY_MARGIN
    )


class CoverageCheckpoints:
    """Coverage checkpoints for one ingestion command, published while the stream is
    still open so windows can build without waiting for it to end.

    Checkpoints chain contiguously (each starts where the previous ended) so
    coverage analysis never sees a false gap between them. A checkpoint fires when
    the watermark of written data crosses an aggregation-window boundary; data older
    than the checkpointed range widens the next checkpoint downward, so the fresh
    record id spans the affected windows and forces their rebuild.
    """

    def __init__(self, start: datetime, aggregation: timedelta):
        self._aggregation = aggregation
        self._checkpointed_through = start
        self._pending_low: Optional[datetime] = None
        self._watermark: Optional[datetime] = None

    def advance(
        self, batch_start: datetime, batch_end: datetime
    ) -> Optional[Tuple[datetime, datetime]]:
        """Record a written batch's time bounds; return the checkpoint now due, if any."""
        if self._pending_low is None or batch_start < self._pending_low:
            self._pending_low = batch_start
        if self._watermark is None or batch_end > self._watermark:
            self._watermark = batch_end

        boundary = align_to_aggregation(self._checkpointed_through, self._aggregation)
        if align_to_aggregation(self._watermark, self._aggregation) <= boundary:
            return None

        checkpoint = (min(self._checkpointed_through, self._pending_low), self._watermark)
        self._checkpointed_through = self._watermark
        self._pending_low = None
        return checkpoint

    def final(self, declared_end: datetime) -> Tuple[datetime, datetime]:
        """The tail checkpoint: everything not yet checkpointed, through the
        declared end — widened if data ran past it."""
        start = self._checkpointed_through
        if self._pending_low is not None:
            start = min(start, self._pending_low)
        end = declared_end
        if self._watermark is not None:
            end = max(end, self._watermark)
        return start, end


class Ingestion:
    def __init__(
        self,
        timeseries_db: TimeSeriesDatabase,
        ingestion_metrics: IngestionMetrics,
        record_store: CoordinationStore,
        registry: DatasetRegistry,
        retention: timedelta = timedelta(days=7),
    ):
        self.timeseries_db = timeseries_db
        self._metrics = ingestion_metrics
        self._record_store = record_store
        self._registry = registry
        self._retention = retention
        self.logger = structlog.get_logger("Ingestion")

    async def _record_lateness(
        self, dataset: str, written: pa.Table, suppressed: int, timestamp_column: str
    ) -> None:
        """Record each stored row's arrival lateness (now minus its observation
        time). The rows a ``dedup_ingestion`` dataset suppresses never reach
        the histogram: a re-fetched span cannot inflate the p95. Rows of an
        already-sealed window are skipped, since no build can use them."""
        times = written.column(timestamp_column).to_pandas()
        now = pd.Timestamp.now(tz="UTC")
        lateness_s = (now - times).dt.total_seconds().to_numpy()

        production = self._registry.get(dataset)
        span_s = int(production.aggregation_span.total_seconds())
        seal_delay_s = int(production.seal_delay(self._retention).total_seconds())
        window_start_s = times.astype("int64").to_numpy() // 1_000_000_000 // span_s * span_s
        live = window_start_s + seal_delay_s > int(now.timestamp())

        self._metrics.record_lateness_samples(
            dataset,
            new=int(live.sum()),
            duplicate=suppressed,
            sealed=int((~live).sum()),
        )

        recorded = lateness_s[live]
        await self._record_store.record_lateness(
            dataset,
            bucket_counts(recorded[recorded >= 0]),
            int(self._retention.total_seconds() // 3600),
        )

    async def ingest(
        self,
        ingestion_id: UUID,
        metadata: IngestionMetadata,
        start_time: datetime,
        end_time: datetime,
        batches: AsyncIterator[pa.RecordBatch],
        on_data_available: DataAvailablePublisher,
    ) -> DataAvailableEvent:
        dataset_name = metadata.name

        total_points = 0
        batch_num = 0
        production = self._registry.get(dataset_name)
        checkpoints = CoverageCheckpoints(start_time, production.aggregation_span)
        claim_num = 0

        # Deterministic ids: a retried command re-checkpoints, re-tags, and
        # re-claims under the same ids, so replays read as the same data.
        # Each claim's windows get their own record ids: the id every row of
        # that window is tagged with, and the id the claim publishes for the
        # coordinator to fold into that window's desired set.
        claim_records: dict[int, UUID] = {}

        def record_id_for(window_start_s: int) -> UUID:
            return claim_records.setdefault(
                window_start_s,
                uuid5(ingestion_id, f"record-{claim_num}-{window_start_s}"),
            )

        def claim_event(claim_start: datetime, claim_end: datetime) -> DataAvailableEvent:
            return DataAvailableEvent(
                id=uuid5(ingestion_id, f"claim-{claim_num}"),
                metadata=metadata,
                start_time=claim_start,
                end_time=claim_end,
                arrived_at=datetime.now(timezone.utc),
                records=[
                    WindowRecord(
                        id=record_id,
                        window_start=datetime.fromtimestamp(ws, tz=timezone.utc),
                    )
                    for ws, record_id in sorted(claim_records.items())
                ],
            )

        stream_verified = False

        async for batch in batches:
            if not stream_verified:
                verify_stream_schema(batch.schema, metadata)
                stream_verified = True
            self._record_batch_observability(dataset_name, batch, metadata)
            # CPU-bound; kept off the Flight server's event loop
            canonical = await asyncio.to_thread(
                canonicalize, batch, metadata.dataset_schema
            )
            if canonical is None:
                self.logger.info(
                    "No points to write in batch; skipping", batch=batch_num + 1
                )
                batch_num += 1
                continue

            n_points = canonical.table.num_rows
            self.logger.info("Writing batch", batch=batch_num + 1, points=n_points)

            written = await self._write_tagged(
                dataset_name, canonical, production, record_id_for
            )

            total_points += n_points
            batch_num += 1
            self._metrics.record_batch_processed(dataset_name)

            await self._record_lateness(
                dataset_name,
                written,
                canonical.table.num_rows - written.num_rows,
                canonical.timestamp_column,
            )

            checkpoint = checkpoints.advance(canonical.start_time, canonical.end_time)
            if checkpoint is not None:
                checkpoint_start, checkpoint_end = checkpoint
                event = claim_event(checkpoint_start, checkpoint_end)
                claim_num += 1
                claim_records = {}
                self.logger.info(
                    "Publishing coverage claim",
                    dataset=dataset_name,
                    claim_start=checkpoint_start.isoformat(),
                    claim_end=checkpoint_end.isoformat(),
                    records=len(event.records),
                )
                await on_data_available(event)

        self._metrics.observe_data_points(dataset_name, total_points)

        final_start_time, final_end_time = checkpoints.final(end_time)

        if final_end_time > end_time:
            self.logger.warning(
                "Data ran past the declared window end",
                dataset=dataset_name,
                declared_end=end_time.isoformat(),
                actual_end=final_end_time.isoformat(),
            )

        event = claim_event(final_start_time, final_end_time)
        await on_data_available(event)
        self._metrics.record_success(dataset_name)
        return event

    async def _write_tagged(
        self,
        dataset: str,
        canonical: CanonicalBatch,
        production: DatasetBuildConfig,
        record_id_for: Callable[[int], UUID],
    ) -> pa.Table:
        """Write the batch's rows, each tagged with its window's record id —
        the provenance a build selects by, and what lets the claim name the
        windows that received rows. ``dedup_ingestion`` datasets write only
        content not already stored, decided against exact per-window sets
        marked once the write succeeds. A crash in between re-stores a
        duplicate, which the build collapse discards.
        Returns the rows actually written."""
        table = canonical.table
        novel_fingerprints: dict[int, set[bytes]] = {}
        if production.dedup_ingestion:
            table, novel_fingerprints = await self._novel_rows(
                dataset, canonical, production
            )

        if table.num_rows:
            span_s = int(production.aggregation_span.total_seconds())
            times = table.column(canonical.timestamp_column).to_pandas()
            window_starts = (
                times.astype("int64").to_numpy() // 1_000_000_000 // span_s * span_s
            )
            tagged = table.append_column(
                RECORD_ID_COLUMN,
                pa.array(
                    [str(record_id_for(int(ws))) for ws in window_starts],
                    type=pa.string(),
                ),
            )
            async with async_timer(
                lambda d: self._metrics.observe_duration(dataset, d)
            ):
                await self.timeseries_db.write(
                    table=tagged,
                    measurement=dataset,
                    tag_columns=canonical.tag_columns + [RECORD_ID_COLUMN],
                    timestamp_column=canonical.timestamp_column,
                )

        seal_delay_s = int(production.seal_delay(self._retention).total_seconds())
        for window_start, digests in novel_fingerprints.items():
            await self._record_store.mark_content_stored(
                dataset,
                window_start,
                sorted(digests),
                _filter_expiry(window_start, seal_delay_s),
            )
        return table

    async def _novel_rows(
        self, dataset: str, canonical: CanonicalBatch, production: DatasetBuildConfig
    ) -> tuple[pa.Table, dict[int, set[bytes]]]:
        """The batch's rows whose full content is not already stored, with their
        fingerprints grouped by aggregation window for post-write marking."""
        table = canonical.table
        fingerprints = await asyncio.to_thread(row_fingerprints, table)
        span_s = int(production.aggregation_span.total_seconds())
        times = table.column(canonical.timestamp_column).to_pandas()
        window_starts = (
            times.astype("int64").to_numpy() // 1_000_000_000 // span_s * span_s
        )

        novel = np.zeros(len(fingerprints), dtype=bool)
        novel_fingerprints: dict[int, set[bytes]] = {}
        for ws in np.unique(window_starts):
            rows = np.flatnonzero(window_starts == ws)
            stored = await self._record_store.stored_content(
                dataset, int(ws), [fingerprints[i] for i in rows]
            )
            fresh = rows[~stored]
            novel[fresh] = True
            if fresh.size:
                novel_fingerprints[int(ws)] = {fingerprints[i] for i in fresh}

        return table.filter(pa.array(novel)), novel_fingerprints

    def _record_batch_observability(
        self, dataset_name: str, batch: pa.RecordBatch, metadata: IngestionMetadata
    ) -> None:
        for column in metadata.dataset_schema.canonical_columns:
            self._metrics.record_null_values(
                dataset_name, column, batch.column(column).null_count
            )

        time_name = metadata.dataset_schema.time.name
        parsed = pd.to_datetime(
            batch.column(time_name).to_pandas(), utc=True, errors="coerce"
        )
        self._metrics.record_dropped_time_rows(
            dataset_name, time_name, int(parsed.isna().sum())
        )
