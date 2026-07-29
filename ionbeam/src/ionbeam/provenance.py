# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import FrozenSet, List, Literal, Optional, Tuple
from uuid import UUID

from ionbeam_client.models import IngestionMetadata
from isodate import duration_isoformat, parse_duration
from pydantic import BaseModel


def align_to_aggregation(ts: datetime, aggregation: timedelta) -> datetime:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = ts - epoch
    aligned_seconds = (
        delta.total_seconds() // aggregation.total_seconds()
    ) * aggregation.total_seconds()
    return epoch + timedelta(seconds=aligned_seconds)


class IngestionRecord(BaseModel):
    """One window's row batch: every row it delivered carries its id as the
    record tag, and its span is the window it delivered into."""

    id: UUID
    metadata: IngestionMetadata
    start_time: datetime
    end_time: datetime
    arrived_at: datetime


class CoverageClaim(BaseModel):
    """The contiguous range one ingestion checkpoint swept. Claims answer
    "was this interval checked" — a range under a claim with no rows holds no
    data; a range under no claim is a gap."""

    id: UUID
    start_time: datetime
    end_time: datetime
    arrived_at: datetime


class RegisteredDatasetMetadata(BaseModel):
    metadata: IngestionMetadata
    schema_hash: str
    registered_at: Optional[datetime] = None


class ManifestRecord(BaseModel):
    """One ingestion record folded into a build. The Optional span fields
    only read as None in old manifests, from builds that could fold expired
    records; current builds defer instead."""

    id: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    arrived_at: Optional[datetime] = None


class ManifestBuild(BaseModel):
    """One build of a window, composing exactly its desired records' rows.
    ``locations`` are the exact store keys the build wrote."""

    version: int
    built_at: datetime
    record_ids_hash: str
    schema_hash: str
    total_rows: int
    is_final: bool
    ionbeam_version: str
    locations: List[str] = []
    records: List[ManifestRecord]


class WindowManifest(BaseModel):
    """A window's durable provenance, stored beside its dataset file.

    ``builds`` is the full build history, appended per rebuild; the last entry
    describes the current file. ``ingestion_metadata`` is the declared schema
    of the last build — earlier builds are pinned to theirs by
    ``schema_hash``."""

    dataset: str
    window_start: datetime
    window_end: datetime
    aggregation: timedelta
    ingestion_metadata: IngestionMetadata
    builds: List[ManifestBuild]


class WindowBuildState(BaseModel):
    record_ids_hash: str
    version: int = 1
    timestamp: datetime
    total_rows: int = 0


@dataclass(frozen=True)
class Window:
    dataset: str
    start: datetime
    aggregation: timedelta

    @property
    def end(self) -> datetime:
        return self.start + self.aggregation

    @property
    def window_id(self) -> str:
        return f"{self.start.isoformat()}_{duration_isoformat(self.aggregation)}"

    @property
    def dataset_key(self) -> str:
        return f"{self.dataset}:{self.window_id}"

    def overlaps(self, start: datetime, end: datetime) -> bool:
        return start < self.end and end > self.start

    @classmethod
    def from_dataset_key(cls, dataset_key: str) -> "Window":
        dataset, window_id = dataset_key.split(":", 1)
        start_iso, duration_iso = window_id.rsplit("_", 1)
        ws = datetime.fromisoformat(start_iso)
        dur = parse_duration(duration_iso)
        agg = dur if isinstance(dur, timedelta) else dur.totimedelta()
        return cls(dataset, ws, agg)


@dataclass(frozen=True)
class RecordSet:
    ids: FrozenSet[str]

    @property
    def hash(self) -> str:
        joined = ",".join(sorted(self.ids))
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    @classmethod
    def from_list(cls, ids: List[str]) -> "RecordSet":
        return cls(frozenset(ids))


@dataclass
class CoverageAnalysis:
    claims: List[CoverageClaim]
    overall_start: Optional[datetime]
    overall_end: Optional[datetime]
    gaps: List[Tuple[datetime, datetime]]

    @classmethod
    def of(cls, claims: List[CoverageClaim]) -> "CoverageAnalysis":
        """Overall span and internal gaps of a dataset's coverage claims."""
        if not claims:
            return cls([], None, None, [])

        sorted_claims = sorted(claims, key=lambda e: (e.start_time, e.end_time))
        overall_start = sorted_claims[0].start_time
        overall_end = max(e.end_time for e in sorted_claims)

        gaps = []
        coverage_end = sorted_claims[0].end_time
        min_gap = timedelta(seconds=1)

        for claim in sorted_claims[1:]:
            if claim.start_time > coverage_end:
                if claim.start_time - coverage_end > min_gap:
                    gaps.append((coverage_end, claim.start_time))
            coverage_end = max(coverage_end, claim.end_time)

        return cls(sorted_claims, overall_start, overall_end, gaps)

    def has_gap_in_window(self, window: Window) -> bool:
        return any(
            gap_start < window.end and gap_end > window.start
            for gap_start, gap_end in self.gaps
        )

    def claims_in_window(self, window: Window) -> List[CoverageClaim]:
        return [e for e in self.claims if window.overlaps(e.start_time, e.end_time)]

    def fully_covers(self, window: Window) -> bool:
        covering = self.claims_in_window(window)
        if not covering:
            return False
        earliest = min(e.start_time for e in covering)
        latest = max(e.end_time for e in covering)
        return earliest <= window.start and latest >= window.end
