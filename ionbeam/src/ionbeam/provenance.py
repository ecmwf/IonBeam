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
    read as None in old manifests, from builds that folded expired records.
    Builds now defer expired records rather than folding them."""

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
    """Coverage over some interval, as the spans that were swept. A decision
    reads only the spans overlapping the window it is deciding, so gaps here
    are those inside the analysed interval, not the dataset's whole history."""

    spans: List[Tuple[datetime, datetime]]
    overall_start: Optional[datetime]
    overall_end: Optional[datetime]
    gaps: List[Tuple[datetime, datetime]]

    @classmethod
    def of(cls, spans: List[Tuple[datetime, datetime]]) -> "CoverageAnalysis":
        """Overall span and internal gaps of a set of swept intervals."""
        if not spans:
            return cls([], None, None, [])

        ordered = sorted(spans)
        overall_start = ordered[0][0]
        overall_end = max(end for _, end in ordered)

        gaps = []
        coverage_end = ordered[0][1]
        min_gap = timedelta(seconds=1)

        for span_start, span_end in ordered[1:]:
            if span_start > coverage_end and span_start - coverage_end > min_gap:
                gaps.append((coverage_end, span_start))
            coverage_end = max(coverage_end, span_end)

        return cls(ordered, overall_start, overall_end, gaps)

    def has_gap_in_window(self, window: Window) -> bool:
        return any(
            gap_start < window.end and gap_end > window.start
            for gap_start, gap_end in self.gaps
        )

    def spans_in_window(self, window: Window) -> List[Tuple[datetime, datetime]]:
        return [span for span in self.spans if window.overlaps(*span)]

    def fully_covers(self, window: Window) -> bool:
        covering = self.spans_in_window(window)
        if not covering:
            return False
        earliest = min(start for start, _ in covering)
        latest = max(end for _, end in covering)
        return earliest <= window.start and latest >= window.end
