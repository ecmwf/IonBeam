import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import FrozenSet, List, Optional, Tuple
from uuid import UUID

from ionbeam_client.models import IngestionMetadata
from isodate import duration_isoformat, parse_duration
from pydantic import BaseModel


class IngestionRecord(BaseModel):
    id: UUID
    metadata: IngestionMetadata
    start_time: datetime
    end_time: datetime


class WindowBuildState(BaseModel):
    event_ids_hash: str
    version: int = 1
    timestamp: datetime


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
class EventSet:
    ids: FrozenSet[str]

    @property
    def hash(self) -> str:
        joined = ",".join(sorted(self.ids))
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    def union(self, other: "EventSet") -> "EventSet":
        return EventSet(self.ids | other.ids)

    @classmethod
    def from_list(cls, ids: List[str]) -> "EventSet":
        return cls(frozenset(ids))

    @classmethod
    def from_records(cls, records: List[IngestionRecord]) -> "EventSet":
        return cls(frozenset(str(r.id) for r in records))


@dataclass
class CoverageAnalysis:
    events: List[IngestionRecord]
    overall_start: Optional[datetime]
    overall_end: Optional[datetime]
    gaps: List[Tuple[datetime, datetime]]

    def has_gap_in_window(self, window: Window) -> bool:
        return any(
            gap_start < window.end and gap_end > window.start
            for gap_start, gap_end in self.gaps
        )

    def events_in_window(self, window: Window) -> List[IngestionRecord]:
        return [e for e in self.events if window.overlaps(e.start_time, e.end_time)]

    def fully_covers(self, window: Window) -> bool:
        covering = self.events_in_window(window)
        if not covering:
            return False
        earliest = min(e.start_time for e in covering)
        latest = max(e.end_time for e in covering)
        return earliest <= window.start and latest >= window.end
