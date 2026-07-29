# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import numpy as np
import redis.asyncio as redis
import structlog
from pydantic import ValidationError

from ..models import (
    CoverageClaim,
    IngestionRecord,
    RegisteredDatasetMetadata,
    Window,
    WindowBuildState,
)
from .lateness_histogram import percentile_seconds

logger = structlog.get_logger(__name__)


class IngestionRecordStore(ABC):
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
    async def get_ingestion_records(self, dataset: str) -> List[IngestionRecord]:
        """Get all ingestion records for a dataset."""
        pass

    @abstractmethod
    async def save_coverage_claim(self, dataset: str, claim: CoverageClaim) -> None:
        """Save a coverage claim — the contiguous range an ingestion swept."""
        pass

    @abstractmethod
    async def get_coverage_claims(self, dataset: str) -> List[CoverageClaim]:
        """Get all coverage claims for a dataset."""
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
    async def filter_unseen(
        self,
        dataset: str,
        window_start: int,
        fingerprints: list[bytes],
        capacity: int,
        expire_at: datetime,
        now: datetime,
    ) -> np.ndarray:
        """Which rows of one aggregation window's batch are newly seen content.

        ``fingerprints`` are per-row content digests (see :mod:`.fingerprints`)
        of rows whose observation time falls in the window starting at
        ``window_start`` (epoch seconds). Returns a bool mask ``(n_rows,)``, True
        where the row's content has not been seen in that window before; each row
        is marked seen as it is checked, in order, so a duplicate later in the
        same batch reads as seen. ``capacity`` sizes the window's filter at
        creation (the dataset's expected distinct row-contents per window). The
        filter is forgotten at ``expire_at`` — its seal time, past which arrivals
        can no longer affect any build. ``now`` is passed in so adapters stay
        deterministic."""
        pass

    @abstractmethod
    async def stored_content(
        self, dataset: str, window_start: int, fingerprints: list[bytes]
    ) -> np.ndarray:
        """Which rows of one aggregation window's batch are already persisted in
        the time-series store — the read half of ``dedup_ingestion``. Exact
        membership, never a Bloom filter: a false positive here would silently
        drop a real row from the store. Returns a bool mask ``(n_rows,)``, True
        where the content is already stored. Unlike :meth:`filter_unseen` this
        does NOT mark anything — call :meth:`mark_content_stored` only after the
        rows' write has succeeded, so every failure mode degrades toward storing
        a duplicate, never toward losing a row."""
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
        ``expire_at`` (the window's seal time): later duplicates are stored
        again — dead weight the build collapse discards, never loss."""
        pass


# Cap items per BF.INSERT so one dedup call never blocks the shared Valkey
# (which also carries the streams bus) for an unbounded burst, and can never
# approach the server's 1M-argument command limit however large a batch grows.
_DEDUP_ITEMS_PER_COMMAND = 10_000
_DEDUP_ERROR_RATE = "0.01"


class RedisIngestionRecordStore(IngestionRecordStore):
    """Redis implementation of IngestionRecordStore. The dedup filters need the
    server to provide the valkey-bloom ``BF.*`` commands (the valkey-bundle
    image, or Redis with the RedisBloom module)."""

    def __init__(self, client: redis.Redis, retention: timedelta = timedelta(days=7)):
        # coordination state must outlive every window it can still affect
        self.client = client
        self._ttl = int(retention.total_seconds())

    def _ingestion_record_key(self, dataset: str, record_id: str) -> str:
        return f"ingestion_records:{dataset}:{record_id}"

    def _registered_metadata_key(self, dataset: str) -> str:
        return f"registered_metadata:{dataset}"

    def _desired_records_key(self, window: Window) -> str:
        return f"{window.dataset_key}:desired_records"

    def _window_state_key(self, window: Window) -> str:
        return f"{window.dataset_key}:state"

    def _lateness_key(self, dataset: str, hour: int) -> str:
        return f"lateness:{dataset}:{hour}"

    def _dedup_key(self, dataset: str, window_start: int) -> str:
        return f"dedup:{dataset}:{window_start}"

    def _stored_key(self, dataset: str, window_start: int) -> str:
        return f"delta_stored:{dataset}:{window_start}"

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
            # A record from a prior schema version (rolling-deploy cache skew). Treat
            # as unregistered so the next register_dataset overwrites it with the
            # current shape — registration self-heals instead of hard-failing ingest.
            logger.warning(
                "Discarding stale registered metadata failing validation", dataset=dataset
            )
            return None

    def _coverage_claim_key(self, dataset: str, claim_id: str) -> str:
        return f"coverage_claims:{dataset}:{claim_id}"

    async def save_ingestion_record(self, record: IngestionRecord) -> None:
        key = self._ingestion_record_key(record.metadata.name, record.id)
        await self.client.set(key, record.model_dump_json(), ex=self._ttl)

    async def save_coverage_claim(self, dataset: str, claim: CoverageClaim) -> None:
        key = self._coverage_claim_key(dataset, str(claim.id))
        await self.client.set(key, claim.model_dump_json(), ex=self._ttl)

    async def get_coverage_claims(self, dataset: str) -> List[CoverageClaim]:
        pattern = f"coverage_claims:{dataset}:*"
        keys = [key async for key in self.client.scan_iter(match=pattern, count=500)]
        if not keys:
            return []
        values = await self.client.mget(keys)
        return [
            CoverageClaim.model_validate_json(value.decode("utf-8"))
            for value in values
            if value is not None
        ]

    async def get_ingestion_records(self, dataset: str) -> List[IngestionRecord]:
        pattern = f"ingestion_records:{dataset}:*"
        keys = [key async for key in self.client.scan_iter(match=pattern, count=500)]
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
                # A record written by a prior schema version — cache skew across a
                # rolling deploy. Skip it rather than failing every read for the
                # dataset; it ages out via its TTL and is superseded by fresh writes.
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
            # State written by a prior schema version (rolling-deploy cache skew).
            # Treat as never-built: the next build rewrites it in the current shape.
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
        # The histogram is sharded into hourly hashes with a rolling TTL so the
        # estimate forgets stale arrival patterns (and one-off backfills) instead
        # of accumulating forever. HINCRBY is atomic, so replicas share it safely.
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

    async def filter_unseen(
        self,
        dataset: str,
        window_start: int,
        fingerprints: list[bytes],
        capacity: int,
        expire_at: datetime,
        now: datetime,
    ) -> np.ndarray:
        # One valkey-bloom filter per (dataset, aggregation window): a re-delivery
        # carries its original observation time, so it always lands in the filter
        # that remembers it, however late it re-arrives while the window is
        # revisable. The key expires at the window's seal time, when stragglers
        # stop mattering. BF.INSERT is an atomic check-and-add returning 1 per
        # newly added item, so concurrent replicas never double-count a row.
        # CAPACITY applies only when the command creates the filter; a window that
        # outgrows it scales by stacking sub-filters (slower, looser error) rather
        # than erroring.
        if not fingerprints:
            return np.zeros(0, dtype=bool)

        key = self._dedup_key(dataset, window_start)
        added: list[int] = []
        for i in range(0, len(fingerprints), _DEDUP_ITEMS_PER_COMMAND):
            chunk = fingerprints[i : i + _DEDUP_ITEMS_PER_COMMAND]
            added.extend(
                await self.client.execute_command(
                    "BF.INSERT", key,
                    "CAPACITY", capacity,
                    "ERROR", _DEDUP_ERROR_RATE,
                    "ITEMS", *chunk,
                )
            )
        await self.client.expireat(key, int(expire_at.timestamp()))
        return np.array(added, dtype=bool)

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
            pipe.expireat(key, int(expire_at.timestamp()))
            await pipe.execute()
