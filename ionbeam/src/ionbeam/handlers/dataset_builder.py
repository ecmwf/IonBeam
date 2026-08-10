# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import structlog
from ionbeam_client.canonical_stream import canonical_arrow_schema
from ionbeam_client.geo import geospatial_projection
from ionbeam_client.models import IngestionMetadata
from ionbeam_client.schema_metadata import BUILD
from pydantic import BaseModel, ValidationError

from ionbeam import __version__
from ionbeam.builds import (
    build_file_key,
    manifest_key,
    next_version,
    stored_builds,
)
from ionbeam.datasets import DatasetRegistry
from ionbeam.messaging.event_bus import DataSetAvailableEvent
from ionbeam.provenance import (
    ManifestBuild,
    ManifestRecord,
    RecordSet,
    IngestionRecord,
    Window,
    WindowBuildState,
    WindowManifest,
)
from ionbeam.observability import BuilderMetrics
from ionbeam.storage.arrow_store import ArrowStore
from ionbeam.storage.build_queue import BuildQueue
from ionbeam.storage.coordination_store import CoordinationStore
from ionbeam.storage.timeseries import RECORD_ID_COLUMN, TimeSeriesDatabase


def collapse_revisions(
    table: pa.Table, identity_columns: list[str], rank: pa.Array
) -> pa.Table:
    """Collapse a record-scoped read to one row per identity — the row from the
    highest-precedence (latest-arrived) record, ``rank`` giving each row its
    record's precedence. Applies the store's (tags, time) upsert deterministically
    over an explicit record set: a correction replaces the whole row, its columns
    coming from the correcting record.

    The result is sorted by ``identity_columns`` in the order given; any order
    groups an identity's revisions adjacently, so a caller that leads with the
    time column gets a time-ascending result and needs no second sort."""
    if table.num_rows == 0:
        return table
    combined = table.append_column("__rank", rank).append_column(
        "__row", pa.array(np.arange(table.num_rows, dtype=np.int64))
    )
    ordered = combined.sort_by(
        [(column, "ascending") for column in identity_columns]
        + [("__rank", "descending"), ("__row", "descending")]
    )

    # A row survives iff its identity differs from the previous (higher-
    # precedence) row's. equal() is null when either side is null; null tags
    # compare equal to each other via the "both null" fallback.
    same_as_previous = None
    for column in identity_columns:
        values = ordered.column(column).combine_chunks()
        current, previous = values.slice(1), values.slice(0, len(values) - 1)
        equal = pc.coalesce(
            pc.equal(current, previous),
            pc.and_kleene(pc.is_null(current), pc.is_null(previous)),
        )
        same_as_previous = (
            equal if same_as_previous is None else pc.and_(same_as_previous, equal)
        )

    keep = np.concatenate(
        ([True], ~same_as_previous.to_numpy(zero_copy_only=False))
    )
    return ordered.filter(pa.array(keep)).drop_columns(["__rank", "__row"])


@dataclass
class _ChunkStats:
    """Totals a chunked pass accumulates as it streams."""

    rows: int = 0
    peak_chunk_rows: int = 0
    drain_s: float = 0.0
    collapse_s: float = 0.0
    drained: set[str] = field(default_factory=set)


class IncompleteRecordSet(Exception):
    """The window's desired record set cannot be composed from the hot store:
    an expired registration leaves arrival order unknowable, or a record's
    rows are gone. Publishing anyway would be silent data loss; the build
    defers instead."""

    def __init__(self, reason: str, detail: str):
        super().__init__(detail)
        self.reason = reason


class DatasetBuilderConfig(BaseModel):
    enabled: bool = True
    poll_interval_seconds: float = 3.0
    concurrency: int = 1
    # A window is read one chunk at a time, bounding a build's memory by the rows
    # a chunk spans rather than the whole window. ``concurrency`` multiplies it.
    chunk_span: timedelta = timedelta(minutes=5)
    retention: timedelta = timedelta(days=7)  # the hot-store horizon and final floor
    # A failing build re-enqueues with exponential backoff, then parks on the
    # dead-letter set after max_build_attempts. 12 attempts at a 600s cap spans
    # ~3.5h, covering a multi-hour store outage.
    max_build_attempts: int = 12
    retry_backoff_base_seconds: float = 5.0
    retry_backoff_max_seconds: float = 600.0


class DatasetBuilder:
    def __init__(
        self,
        config: DatasetBuilderConfig,
        record_store: CoordinationStore,
        queue: BuildQueue,
        timeseries_db: TimeSeriesDatabase,
        builder_metrics: BuilderMetrics,
        arrow_store: ArrowStore,
        event_publisher: Callable[[DataSetAvailableEvent], Awaitable[None]],
        registry: DatasetRegistry,
    ) -> None:
        self.config = config
        self.record_store = record_store
        self.queue = queue
        self.timeseries_db = timeseries_db
        self.arrow_store = arrow_store
        self.event_publisher = event_publisher
        self._registry = registry
        self._metrics = builder_metrics
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._inflight: set[asyncio.Task] = set()
        # Per-replica failed-build counter, keyed by window; drives backoff + cap.
        self._attempts: dict[str, int] = {}
        self.logger = structlog.get_logger(__name__)

    def _is_final(self, window: Window) -> bool:
        """A window is final once the hot-store retention has passed: its build
        is immutable, and no further revisions will be published."""
        return datetime.now(timezone.utc) >= window.end + self.config.retention

    def _next_claim_floor(self, window: Window) -> datetime:
        return datetime.now(timezone.utc) + self._registry.get(
            window.dataset
        ).rebuild_debounce

    async def _settle(self, window: Window) -> None:
        await self.queue.complete(window, self._next_claim_floor(window))
        self._attempts.pop(window.dataset_key, None)
        self._metrics.build_succeeded(window.dataset)

    async def build_window(self, window: Window) -> None:
        self._metrics.build_started(window.dataset)
        start_time = time.perf_counter()

        try:
            desired = RecordSet.from_list(
                await self.record_store.get_desired_record_ids(window)
            )
            state = await self.record_store.get_window_state(window)

            if state and state.record_ids_hash == desired.hash:
                self.logger.info("Window up-to-date", window=window.dataset_key)
                await self._settle(window)
                return

            if not desired.ids:
                # a queued window can outlive its desired set's TTL
                self.logger.info("No records for window", window=window.dataset_key)
                await self._settle(window)
                return

            records = await self.record_store.get_ingestion_records(
                window.dataset, desired.ids
            )
            if not records:
                self.logger.warning("No metadata available", window=window.dataset_key)
                await self._defer_window(window, "missing_metadata")
                return

            latest = max(records, key=lambda r: (r.end_time, r.start_time))
            metadata = latest.metadata

            is_final = self._is_final(window)
            version = next_version(
                state, await stored_builds(self.arrow_store, window)
            )
            dataset_event, total_rows = await self._build_dataset_files(
                window, records, metadata, desired, is_final, version
            )

            await self.event_publisher(dataset_event)
            self._metrics.record_dataset_published(window.dataset, "success")

            await self.record_store.set_window_state(
                window,
                WindowBuildState(
                    record_ids_hash=desired.hash,
                    version=version,
                    timestamp=datetime.now(timezone.utc),
                    total_rows=total_rows,
                ),
            )
            await self._settle(window)
            self.logger.info(
                "Built window successfully",
                dataset=window.dataset,
                window_start=window.start.isoformat(),
                window_end=window.end.isoformat(),
                hash=desired.hash[:8],
            )

        except IncompleteRecordSet as err:
            self.logger.error(
                "Window record set cannot be composed; deferring",
                window=window.dataset_key,
                reason=err.reason,
                detail=str(err),
            )
            self._metrics.build_failed(window.dataset)
            await self._defer_window(window, err.reason)
        except Exception:
            await self._handle_build_failure(window)
        finally:
            duration = time.perf_counter() - start_time
            self._metrics.observe_build_duration(window.dataset, duration)

    async def _handle_build_failure(self, window: Window) -> None:
        """A build raised — almost always a timed-out or overloaded query. Defers
        with backoff, or drops past the cap."""
        self.logger.exception("Build failed", window=window.dataset_key)
        self._metrics.build_failed(window.dataset)
        await self._defer_window(window, "exception")

    async def _defer_window(self, window: Window, reason: str) -> None:
        """Release the lease and reschedule with exponential backoff. The queue
        holds the delay, keeping a failing window from blocking a worker slot;
        drops the window after ``max_build_attempts``."""
        key = window.dataset_key
        attempts = self._attempts.get(key, 0) + 1
        self._attempts[key] = attempts

        if attempts >= self.config.max_build_attempts:
            self._attempts.pop(key, None)
            await self.queue.park(window, self._next_claim_floor(window))
            self._metrics.requeued(window.dataset, "parked")
            self.logger.error(
                "Parking window after repeated failures",
                window=key,
                reason=reason,
                attempts=attempts,
            )
            return

        backoff = min(
            self.config.retry_backoff_max_seconds,
            self.config.retry_backoff_base_seconds * (2 ** (attempts - 1)),
        )
        await self.queue.requeue(
            window, datetime.now(timezone.utc) + timedelta(seconds=backoff)
        )
        self._metrics.requeued(window.dataset, reason)

    def _record_precedence(
        self, records: list[IngestionRecord], desired: RecordSet
    ) -> list[str] | None:
        """The desired record ids in precedence order (arrival, then id),
        lowest first — or None when a desired record's registration has
        expired, leaving its arrival unknowable. That build can no longer
        order corrections deterministically, so it defers rather than guess."""
        arrivals = {str(record.id): record.arrived_at for record in records}
        if any(record_id not in arrivals for record_id in desired.ids):
            return None
        return sorted(desired.ids, key=lambda record_id: (arrivals[record_id], record_id))

    async def _build_dataset_files(
        self,
        window: Window,
        records: list[IngestionRecord],
        metadata: IngestionMetadata,
        desired: RecordSet,
        is_final: bool,
        version: int,
    ) -> tuple[DataSetAvailableEvent, int]:
        desired_hash = desired.hash
        dataset_metadata = self._registry.get(window.dataset).output_metadata(window.dataset)

        self.logger.info(
            "Creating dataset",
            window=window.dataset_key,
            version=version,
            hash=desired_hash[:8],
        )

        schema = canonical_arrow_schema(metadata, dataset_metadata)
        geo_schema, geo_transform = geospatial_projection(schema)

        time_column = metadata.dataset_schema.time.name

        def shape_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
            # A window may lack declared columns entirely — restore them as typed
            # nulls so every build carries the full canonical schema.
            columns = dict(zip(batch.schema.names, batch.columns))
            arrays = [
                columns[field.name].cast(field.type)
                if field.name in columns
                else pa.nulls(batch.num_rows, type=field.type)
                for field in schema
            ]
            return pa.RecordBatch.from_arrays(arrays, schema=schema)

        def project(batch: pa.RecordBatch) -> pa.RecordBatch:
            shaped = shape_batch(batch)
            return geo_transform(shaped) if geo_transform else shaped

        # Reads exactly the desired records' rows, so the stamped record-set
        # hash names what the artifact holds.
        ordered_ids = self._record_precedence(records, desired)
        if ordered_ids is None:
            raise IncompleteRecordSet(
                "expired_records", f"desired records unregistered for {window.dataset_key}"
            )

        # Time leads the identity so each collapsed chunk comes out
        # time-ascending, making the concatenated window sorted by construction.
        identity = [time_column] + [tag.name for tag in metadata.dataset_schema.tags]

        def collapse_and_project(raw_batches: list[pa.RecordBatch]) -> list[pa.RecordBatch]:
            """A record-scoped read holds every record's revision of the same
            identity; collapse to the latest-arrived before shaping."""
            raw = pa.Table.from_batches(raw_batches)
            rank = pc.index_in(
                raw.column(RECORD_ID_COLUMN),
                value_set=pa.array(ordered_ids, type=pa.string()),
            )
            collapsed = collapse_revisions(raw, identity, rank)
            return [project(batch) for batch in collapsed.to_batches(max_chunksize=131072)]

        # An identity carries its timestamp, so every revision of a row shares a
        # chunk: collapsing chunk-by-chunk gives the same result as collapsing the
        # whole window, while holding only one chunk's rows at a time.
        chunk_starts: list[datetime] = []
        chunk_start = window.start
        while chunk_start < window.end:
            chunk_starts.append(chunk_start)
            chunk_start = min(chunk_start + self.config.chunk_span, window.end)

        async def collapsed_chunks(stats: _ChunkStats):
            """Every chunk's collapsed, projected batches, in window order."""
            for chunk_start in chunk_starts:
                chunk_end = min(chunk_start + self.config.chunk_span, window.end)

                drain_start = time.perf_counter()
                collected = [
                    batch
                    async for batch in self.timeseries_db.query_measurement_data(
                        measurement=window.dataset,
                        start_time=chunk_start,
                        end_time=chunk_end,
                        timestamp_column=time_column,
                        record_ids=ordered_ids,
                    )
                ]  # raw: the collapse needs record_id, which shaping drops
                stats.drain_s += time.perf_counter() - drain_start
                if not collected:
                    continue

                stats.peak_chunk_rows = max(
                    stats.peak_chunk_rows, sum(batch.num_rows for batch in collected)
                )
                stats.drained.update(
                    record_id
                    for batch in collected
                    for record_id in pc.unique(batch.column(RECORD_ID_COLUMN)).to_pylist()
                )

                collapse_start = time.perf_counter()
                projected = await asyncio.to_thread(collapse_and_project, collected)
                stats.collapse_s += time.perf_counter() - collapse_start
                del collected

                for batch in projected:
                    stats.rows += batch.num_rows
                    yield batch

            if not stats.drained.issuperset(ordered_ids):
                # Every desired record put rows in this window when it was made.
                # Rows the tag filter cannot reach mean the hot store lost or
                # expired them.
                missing = len(set(ordered_ids) - stats.drained)
                raise IncompleteRecordSet(
                    "missing_record_rows",
                    f"{missing} desired records have no reachable rows in {window.dataset_key}",
                )

        # The footer stamps the row count, so the window is counted before it is
        # written. Both passes hold one chunk at a time.
        counted = _ChunkStats()
        async for _ in collapsed_chunks(counted):
            pass
        total_rows = counted.rows
        self._metrics.observe_rows_exported(window.dataset, total_rows)

        locations = [build_file_key(window, version, desired_hash)]

        build = ManifestBuild(
            version=version,
            built_at=datetime.now(timezone.utc),
            record_ids_hash=desired_hash,
            schema_hash=metadata.schema_hash(),
            total_rows=total_rows,
            is_final=is_final,
            ionbeam_version=__version__,
            locations=locations,
            records=self._manifest_records(records, desired),
        )
        build_schema = geo_schema.with_metadata(
            {
                **(geo_schema.metadata or {}),
                BUILD.encode(): build.model_dump_json().encode(),
            }
        )

        write_start = time.perf_counter()
        stats = _ChunkStats()
        await self.arrow_store.write_record_batches(
            locations[0],
            collapsed_chunks(stats),
            schema=build_schema,
            sorted_by=time_column,
        )
        write_s = time.perf_counter() - write_start

        self.logger.info(
            "Build stage timings",
            window=window.dataset_key,
            drain_s=round(stats.drain_s, 2),
            collapse_s=round(stats.collapse_s, 2),
            write_s=round(write_s, 2),
            rows=total_rows,
            chunks=len(chunk_starts),
            peak_chunk_rows=stats.peak_chunk_rows,
            files=len(locations),
        )

        await self._write_manifest(window, metadata, build)

        event = DataSetAvailableEvent(
            id=uuid.uuid4(),
            metadata=dataset_metadata,
            dataset_locations=locations,
            start_time=window.start,
            end_time=window.end,
            version=version,
            is_final=is_final,
        )
        return event, int(total_rows)

    @staticmethod
    def _manifest_records(
        records: list[IngestionRecord], desired: RecordSet
    ) -> list[ManifestRecord]:
        # every desired id resolved; _record_precedence defers the build if
        # any registration has expired
        by_id = {str(record.id): record for record in records}
        return [
            ManifestRecord(
                id=record_id,
                start_time=by_id[record_id].start_time,
                end_time=by_id[record_id].end_time,
                arrived_at=by_id[record_id].arrived_at,
            )
            for record_id in sorted(desired.ids)
        ]

    async def _write_manifest(
        self, window: Window, metadata: IngestionMetadata, build: ManifestBuild
    ) -> None:
        """Append this build to the window's history sidecar under the
        dataset's ``_manifests/`` prefix (``_``-prefixed so dataset discovery
        skips it); each entry names its build's exact file set. Written after
        the files, so a crash in between leaves the history a build behind."""
        key = manifest_key(window)

        builds: list[ManifestBuild] = []
        existing = await self.arrow_store.read_json(key)
        if existing is not None:
            try:
                builds = WindowManifest.model_validate_json(existing).builds
            except ValidationError:
                self.logger.warning("Discarding unreadable window manifest", key=key)

        manifest = WindowManifest(
            dataset=window.dataset,
            window_start=window.start,
            window_end=window.end,
            aggregation=window.aggregation,
            ingestion_metadata=metadata,
            builds=[*builds, build],
        )
        await self.arrow_store.write_json(key, manifest.model_dump_json())

    async def start(self) -> None:
        if not self.config.enabled:
            self.logger.info("DatasetBuilder disabled; window building will not run")
            return
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
        failures = 0
        try:
            while not self._stop.is_set():
                try:
                    while len(self._inflight) < max(1, self.config.concurrency):
                        window = await self.queue.claim_due()
                        if not window:
                            break

                        task = asyncio.create_task(self.build_window(window))
                        self._track_task(task)
                    failures = 0
                except Exception:
                    # The queue is this loop's only external boundary (Valkey).
                    # An outage idles the builder; it never kills it.
                    failures += 1
                    backoff = min(
                        self.config.retry_backoff_max_seconds,
                        self.config.retry_backoff_base_seconds * 2 ** (failures - 1),
                    )
                    self.logger.exception(
                        "Builder dequeue failed; retrying", backoff_s=backoff
                    )
                    await asyncio.sleep(backoff)
                    continue

                if not self._inflight:
                    await asyncio.sleep(self.config.poll_interval_seconds)
                else:
                    await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            pass
        finally:
            if self._inflight:
                await asyncio.gather(*self._inflight, return_exceptions=True)
                self._inflight.clear()

    def _track_task(self, task: asyncio.Task) -> None:
        self._inflight.add(task)
        task.add_done_callback(lambda _: self._inflight.discard(task))
