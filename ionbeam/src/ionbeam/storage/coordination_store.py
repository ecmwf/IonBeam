# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Sequence, Tuple

import numpy as np
import redis.asyncio as redis
import structlog
from pydantic import ValidationError

from ..provenance import (
    CoverageClaim,
    IngestionRecord,
    RegisteredDatasetMetadata,
    Window,
    WindowBuildState,
)
from .lateness_histogram import percentile_seconds

logger = structlog.get_logger(__name__)


class CoordinationStore(ABC):
    """Interface for storing ingestion records, registered schemas, and window state."""

    @abstractmethod
    async def save_registered_metadata(self, record: RegisteredDatasetMetadata) -> None:
        """Persist registered dataset metadata and its schema hash."""
        pass

    @abstractmethod
    async def get_registered_metadata(self, dataset: str) -> Optional[RegisteredDatasetMetadata]:
        """Return the registered metadata for a dataset, if any."""
        pass

    @abstractmethod
    async def save_ingestion_record(self, record: IngestionRecord) -> None:
        """Save an ingestion record."""
        pass

    @abstractmethod
    async def get_ingestion_records(
        self, dataset: str, record_ids: Sequence[str]
    ) -> List[IngestionRecord]:
        """The named records. A build folds a known record set, so the read is
        keyed: a dataset's whole population spans the retention horizon and is
        far larger than any one window's."""
        pass

    @abstractmethod
    async def save_coverage_claim(self, dataset: str, claim: CoverageClaim) -> None:
        """Save a coverage claim — the contiguous range an ingestion swept."""
        pass

    @abstractmethod
    async def get_coverage_spans(
        self, dataset: str, start: datetime, end: datetime
    ) -> List[Tuple[datetime, datetime]]:
        """The swept intervals overlapping ``[start, end]``. A build decision is
        a question about one window, so coverage is read over that window rather
        than over the dataset's whole history."""
        pass

    @abstractmethod
    async def get_desired_record_ids(self, window: Window) -> List[str]:
        """The record ids a window's next build should fold in, sorted."""
        pass

    @abstractmethod
    async def add_desired_record_ids(
        self, window: Window, record_ids: List[str]
    ) -> List[str]:
        """Fold record ids into a window's desired set and return the full set,
        sorted. The union is applied server-side, so concurrent coordinators
        never lose each other's records."""
        pass

    @abstractmethod
    async def get_window_state(self, window: Window) -> Optional[WindowBuildState]:
        """Get the build state for a window."""
        pass

    @abstractmethod
    async def set_window_state(self, window: Window, state: WindowBuildState) -> None:
        """Set the build state for a window."""
        pass

    @abstractmethod
    async def record_lateness(
        self, dataset: str, bucket_counts: dict[int, int], retention_hours: int
    ) -> None:
        """Add a batch of per-datum arrival-lateness observations, already bucketed
        as ``{bucket: count}``, into the dataset's rolling histogram."""
        pass

    @abstractmethod
    async def lateness_percentile(
        self, dataset: str, percentile: float, min_samples: int, retention_hours: int
    ) -> Optional[timedelta]:
        """The measured lateness at ``percentile`` for a dataset, or None until
        ``min_samples`` observations have accumulated."""
        pass

    @abstractmethod
    async def stored_content(
        self, dataset: str, window_start: int, fingerprints: list[bytes]
    ) -> np.ndarray:
        """Which rows of one aggregation window's batch are already persisted in
        the time-series store — the read half of ``dedup_ingestion``. Exact
        membership, never a Bloom filter: a false positive here would drop a real
        row from the store. Returns a bool mask ``(n_rows,)``, True where the
        content is already stored. Does not mark anything; call
        :meth:`mark_content_stored` only after the rows' write has succeeded.
        Every failure mode degrades toward storing a duplicate, never toward
        losing a row."""
        pass

    @abstractmethod
    async def mark_content_stored(
        self,
        dataset: str,
        window_start: int,
        fingerprints: list[bytes],
        expire_at: datetime,
    ) -> None:
        """Record row contents as durably written. The set is forgotten at
        ``expire_at`` (the window's seal time); later duplicates are stored
        again and discarded by build collapse."""
        pass


# Caps items per command, bounding how long one dedup call blocks the shared
# Valkey (which also carries the streams bus), and staying under the server's
# 1M-argument command limit.
_DEDUP_ITEMS_PER_COMMAND = 10_000


class RedisCoordinationStore(CoordinationStore):
    """Redis implementation of CoordinationStore."""

    def __init__(self, client: redis.Redis, retention: timedelta = timedelta(days=7)):
        # Retention must outlive every window the coordination state can still affect.
        self.client = client
        self._ttl = int(retention.total_seconds())

    def _ingestion_record_key(self, dataset: str, record_id: str) -> str:
        return f"ionbeam:ingestion_records:{dataset}:{record_id}"

    def _registered_metadata_key(self, dataset: str) -> str:
        return f"ionbeam:registered_metadata:{dataset}"

    def _desired_records_key(self, window: Window) -> str:
        return f"ionbeam:window:{window.dataset_key}:desired_records"

    def _window_state_key(self, window: Window) -> str:
        return f"ionbeam:window:{window.dataset_key}:state"

    def _lateness_key(self, dataset: str, hour: int) -> str:
        return f"ionbeam:lateness:{dataset}:{hour}"

    def _stored_key(self, dataset: str, window_start: int) -> str:
        return f"ionbeam:delta_stored:{dataset}:{window_start}"

    async def save_registered_metadata(self, record: RegisteredDatasetMetadata) -> None:
        key = self._registered_metadata_key(record.metadata.name)
        await self.client.set(key, record.model_dump_json())

    async def get_registered_metadata(self, dataset: str) -> Optional[RegisteredDatasetMetadata]:
        result = await self.client.get(self._registered_metadata_key(dataset))
        if not result:
            return None
        try:
            return RegisteredDatasetMetadata.model_validate_json(result.decode("utf-8"))
        except ValidationError:
            # Metadata from a prior schema version is treated as unregistered; the
            # next register_dataset overwrites it with the current shape.
            logger.warning(
                "Discarding stale registered metadata failing validation", dataset=dataset
            )
            return None

    def _coverage_key(self, dataset: str) -> str:
        return f"ionbeam:coverage:{dataset}"

    def _coverage_maxspan_key(self, dataset: str) -> str:
        return f"ionbeam:coverage_maxspan:{dataset}"

    def _coverage_claim_key(self, dataset: str, claim_id: str) -> str:
        return f"ionbeam:coverage_claims:{dataset}:{claim_id}"

    async def save_ingestion_record(self, record: IngestionRecord) -> None:
        key = self._ingestion_record_key(record.metadata.name, record.id)
        await self.client.set(key, record.model_dump_json(), ex=self._ttl)

    async def save_coverage_claim(self, dataset: str, claim: CoverageClaim) -> None:
        # Coverage is the union of swept intervals: a claim's identity and
        # arrival never enter a decision, so the span alone is stored and
        # repeated sweeps of one interval collapse to a single member.
        start = claim.start_time.timestamp()
        end = claim.end_time.timestamp()
        floor = datetime.now(timezone.utc).timestamp() - self._ttl
        key = self._coverage_key(dataset)
        maxspan_key = self._coverage_maxspan_key(dataset)

        pipe = self.client.pipeline()
        pipe.zadd(key, {f"{start}|{end}": start})
        pipe.zremrangebyscore(key, "-inf", floor)
        pipe.expire(key, self._ttl)
        # The widest span seen bounds how far back a read must look for an
        # interval that starts before a window and reaches into it. GT keeps it
        # monotonic without a read-modify-write between replicas.
        pipe.zadd(maxspan_key, {"max": end - start}, gt=True)
        pipe.expire(maxspan_key, self._ttl)
        await pipe.execute()

    async def get_coverage_spans(
        self, dataset: str, start: datetime, end: datetime
    ) -> List[Tuple[datetime, datetime]]:
        maxspan = await self.client.zscore(self._coverage_maxspan_key(dataset), "max")
        lo = start.timestamp() - (maxspan or 0.0)
        members = await self.client.zrangebyscore(
            self._coverage_key(dataset), lo, end.timestamp()
        )
        spans: List[Tuple[datetime, datetime]] = []
        for member in members:
            span_start, _, span_end = member.decode("utf-8").partition("|")
            span_start, span_end = float(span_start), float(span_end)
            if span_end < start.timestamp():
                continue
            spans.append(
                (
                    datetime.fromtimestamp(span_start, timezone.utc),
                    datetime.fromtimestamp(span_end, timezone.utc),
                )
            )
        return spans

    async def get_ingestion_records(
        self, dataset: str, record_ids: Sequence[str]
    ) -> List[IngestionRecord]:
        keys = [self._ingestion_record_key(dataset, rid) for rid in record_ids]
        if not keys:
            return []

        values = await self.client.mget(keys)
        records: List[IngestionRecord] = []
        skipped = 0
        for value in values:
            if value is None:
                continue
            try:
                records.append(IngestionRecord.model_validate_json(value.decode("utf-8")))
            except ValidationError:
                # A record written by a prior schema version is skipped rather than
                # failing the read; it ages out via its TTL and is superseded by
                # fresh writes.
                skipped += 1
        if skipped:
            logger.warning(
                "Skipped stale ingestion records failing validation",
                dataset=dataset,
                skipped=skipped,
            )
        return records

    async def get_desired_record_ids(self, window: Window) -> List[str]:
        members = await self.client.smembers(self._desired_records_key(window))
        return sorted(member.decode("utf-8") for member in members)

    async def add_desired_record_ids(
        self, window: Window, record_ids: List[str]
    ) -> List[str]:
        key = self._desired_records_key(window)
        async with self.client.pipeline(transaction=True) as pipe:
            if record_ids:
                pipe.sadd(key, *record_ids)
            pipe.expire(key, self._ttl)
            pipe.smembers(key)
            results = await pipe.execute()
        return sorted(member.decode("utf-8") for member in results[-1])

    async def get_window_state(self, window: Window) -> Optional[WindowBuildState]:
        key = self._window_state_key(window)
        result = await self.client.get(key)
        if not result:
            return None

        try:
            return WindowBuildState.model_validate_json(result.decode("utf-8"))
        except ValidationError:
            # State written by a prior schema version is treated as never-built;
            # the next build rewrites it in the current shape.
            logger.warning(
                "Discarding stale window state failing validation",
                window=window.dataset_key,
            )
            return None

    async def set_window_state(self, window: Window, state: WindowBuildState) -> None:
        key = self._window_state_key(window)
        await self.client.set(key, state.model_dump_json(), ex=self._ttl)

    async def record_lateness(
        self, dataset: str, bucket_counts: dict[int, int], retention_hours: int
    ) -> None:
        # The histogram is sharded into hourly hashes with a rolling TTL: the
        # estimate forgets stale arrival patterns and one-off backfills rather than
        # accumulating forever. HINCRBY is atomic.
        if not bucket_counts:
            return
        hour = int(datetime.now(timezone.utc).timestamp()) // 3600
        key = self._lateness_key(dataset, hour)
        async with self.client.pipeline(transaction=False) as pipe:
            for bucket, count in bucket_counts.items():
                pipe.hincrby(key, str(bucket), count)
            pipe.expire(key, retention_hours * 3600)
            await pipe.execute()

    async def lateness_percentile(
        self, dataset: str, percentile: float, min_samples: int, retention_hours: int
    ) -> Optional[timedelta]:
        now_hour = int(datetime.now(timezone.utc).timestamp()) // 3600
        keys = [self._lateness_key(dataset, now_hour - i) for i in range(retention_hours)]
        async with self.client.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.hgetall(key)
            shards = await pipe.execute()

        counts: dict[int, int] = {}
        for shard in shards:
            for bucket, count in shard.items():
                b = int(bucket)
                counts[b] = counts.get(b, 0) + int(count)

        seconds = percentile_seconds(counts, percentile, min_samples)
        return timedelta(seconds=seconds) if seconds is not None else None

    async def stored_content(
        self, dataset: str, window_start: int, fingerprints: list[bytes]
    ) -> np.ndarray:
        if not fingerprints:
            return np.zeros(0, dtype=bool)

        key = self._stored_key(dataset, window_start)
        stored: list[int] = []
        for i in range(0, len(fingerprints), _DEDUP_ITEMS_PER_COMMAND):
            chunk = fingerprints[i : i + _DEDUP_ITEMS_PER_COMMAND]
            stored.extend(await self.client.smismember(key, chunk))
        return np.array(stored, dtype=bool)

    async def mark_content_stored(
        self,
        dataset: str,
        window_start: int,
        fingerprints: list[bytes],
        expire_at: datetime,
    ) -> None:
        if not fingerprints:
            return

        key = self._stored_key(dataset, window_start)
        async with self.client.pipeline(transaction=False) as pipe:
            for i in range(0, len(fingerprints), _DEDUP_ITEMS_PER_COMMAND):
                pipe.sadd(key, *fingerprints[i : i + _DEDUP_ITEMS_PER_COMMAND])
                if i == 0:
                    pipe.expireat(key, int(expire_at.timestamp()))
            await pipe.execute()
