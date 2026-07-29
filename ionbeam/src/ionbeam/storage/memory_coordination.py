# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import numpy as np

from ..provenance import (
    CoverageClaim,
    IngestionRecord,
    RegisteredDatasetMetadata,
    Window,
    WindowBuildState,
)
from .build_queue import BuildQueue
from .coordination_store import CoordinationStore
from .lateness_histogram import percentile_seconds
from .trigger_claims import TriggerClaims


class InMemoryCoordinationStore(CoordinationStore):
    """In-memory storage for ingestion records and window state, for tests and
    single-process runs. Nothing expires: no local run lives long enough."""

    def __init__(self, retention: timedelta = timedelta(days=7)):
        self._records: dict[str, IngestionRecord] = {}
        self._claims: dict[str, dict[str, CoverageClaim]] = {}
        self._registered_metadata: dict[str, RegisteredDatasetMetadata] = {}
        self._desired_records: dict[str, set[str]] = {}
        self._window_states: dict[str, WindowBuildState] = {}
        self._lateness: dict[str, dict[int, int]] = {}
        self._stored: dict[tuple[str, int], set[bytes]] = {}

    async def save_registered_metadata(self, record: RegisteredDatasetMetadata) -> None:
        self._registered_metadata[record.metadata.name] = record

    async def get_registered_metadata(self, dataset: str) -> Optional[RegisteredDatasetMetadata]:
        return self._registered_metadata.get(dataset)

    async def save_ingestion_record(self, record: IngestionRecord) -> None:
        self._records[f"ingestion_records:{record.metadata.name}:{record.id}"] = record

    async def get_ingestion_records(self, dataset: str) -> List[IngestionRecord]:
        prefix = f"ingestion_records:{dataset}:"
        return [record for key, record in self._records.items() if key.startswith(prefix)]

    async def save_coverage_claim(self, dataset: str, claim: CoverageClaim) -> None:
        self._claims.setdefault(dataset, {})[str(claim.id)] = claim

    async def get_coverage_claims(self, dataset: str) -> List[CoverageClaim]:
        return list(self._claims.get(dataset, {}).values())

    async def get_desired_record_ids(self, window: Window) -> List[str]:
        return sorted(self._desired_records.get(window.dataset_key, set()))

    async def add_desired_record_ids(
        self, window: Window, record_ids: List[str]
    ) -> List[str]:
        desired = self._desired_records.setdefault(window.dataset_key, set())
        desired.update(record_ids)
        return sorted(desired)

    async def get_window_state(self, window: Window) -> Optional[WindowBuildState]:
        return self._window_states.get(window.dataset_key)

    async def set_window_state(self, window: Window, state: WindowBuildState) -> None:
        self._window_states[window.dataset_key] = state

    async def record_lateness(
        self, dataset: str, bucket_counts: dict[int, int], retention_hours: int
    ) -> None:
        hist = self._lateness.setdefault(dataset, {})
        for bucket, count in bucket_counts.items():
            hist[bucket] = hist.get(bucket, 0) + count

    async def lateness_percentile(
        self, dataset: str, percentile: float, min_samples: int, retention_hours: int
    ) -> Optional[timedelta]:
        seconds = percentile_seconds(
            self._lateness.get(dataset, {}), percentile, min_samples
        )
        return timedelta(seconds=seconds) if seconds is not None else None

    async def stored_content(
        self, dataset: str, window_start: int, fingerprints: list[bytes]
    ) -> np.ndarray:
        stored = self._stored.get((dataset, window_start), set())
        return np.array([f in stored for f in fingerprints], dtype=bool)

    async def mark_content_stored(
        self,
        dataset: str,
        window_start: int,
        fingerprints: list[bytes],
        expire_at: datetime,
    ) -> None:
        self._stored.setdefault((dataset, window_start), set()).update(fingerprints)


class InMemoryTriggerClaims(TriggerClaims):
    """In-memory trigger claims. Suitable for single-process deployments."""

    def __init__(self):
        self._claims: dict[str, datetime] = {}

    async def try_claim(self, key: str, ttl: timedelta) -> bool:
        now = datetime.now(timezone.utc)
        expiry = self._claims.get(key)
        if expiry is not None and expiry > now:
            return False
        self._claims[key] = now + ttl
        return True


class InMemoryBuildQueue(BuildQueue):
    """In-memory build schedule. Suitable for single-process deployments.

    Leases carry no expiry: a crash loses the whole schedule anyway, so the
    reclaim path has nothing to recover. They still exist so a window
    re-scheduled mid-build waits for its release, exactly as in Redis."""

    def __init__(self):
        self._scheduled: dict[str, datetime] = {}
        self._leased: set[str] = set()
        self._dead: set[str] = set()
        self._lock = asyncio.Lock()

    async def schedule(self, window: Window, eligible_at: datetime) -> None:
        async with self._lock:
            self._scheduled[window.dataset_key] = eligible_at

    async def claim_due(self) -> Optional[Window]:
        async with self._lock:
            now = datetime.now(timezone.utc)
            due = [
                (at, key)
                for key, at in self._scheduled.items()
                if at <= now and key not in self._leased
            ]
            if not due:
                return None
            _, dataset_key = min(due)
            del self._scheduled[dataset_key]
            self._leased.add(dataset_key)
            return Window.from_dataset_key(dataset_key)

    async def requeue(self, window: Window, eligible_at: datetime) -> None:
        async with self._lock:
            self._leased.discard(window.dataset_key)
            self._scheduled[window.dataset_key] = eligible_at

    async def complete(self, window: Window, next_claim_floor: datetime) -> None:
        async with self._lock:
            key = window.dataset_key
            self._leased.discard(key)
            self._dead.discard(key)
            if key in self._scheduled and self._scheduled[key] < next_claim_floor:
                self._scheduled[key] = next_claim_floor

    async def park(self, window: Window, next_claim_floor: datetime) -> None:
        async with self._lock:
            key = window.dataset_key
            self._leased.discard(key)
            self._dead.add(key)
            if key in self._scheduled and self._scheduled[key] < next_claim_floor:
                self._scheduled[key] = next_claim_floor

    async def dead_lettered(self) -> list[Window]:
        async with self._lock:
            return sorted(
                (Window.from_dataset_key(key) for key in self._dead),
                key=lambda window: window.dataset_key,
            )
