from datetime import datetime, timedelta
from typing import Optional, FrozenSet, List, Tuple
from uuid import UUID
import hashlib

from isodate import duration_isoformat, parse_duration
from pydantic import BaseModel
from dataclasses import dataclass

from ..models.models import IngestionMetadata


class IngestionRecord(BaseModel):
    id: UUID
    metadata: IngestionMetadata
    start_time: datetime
    end_time: datetime


class WindowBuildState(BaseModel):
    event_ids_hash: str
    version: int = 1
    timestamp: datetime


class LockInfo(BaseModel):
    locked_at: Optional[datetime] = None
    released_at: Optional[datetime] = None


@dataclass(frozen=True)
class Window:
    """Immutable window specification with all derived properties."""
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
    """Immutable set of event IDs with deterministic hash."""
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
    """Result of analyzing event coverage for a dataset."""
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
        return [
            e for e in self.events 
            if window.overlaps(e.start_time, e.end_time)
        ]
    
    def fully_covers(self, window: Window) -> bool:
        covering = self.events_in_window(window)
        if not covering:
            return False
        earliest = min(e.start_time for e in covering)
        latest = max(e.end_time for e in covering)
        return earliest <= window.start and latest >= window.end


class WindowRef(BaseModel):
    """
    Reference to a dataset aggregation window and its event key encoding.

    Dataset key format: "{dataset}:{window_id}"
    Window ID format: "{window_start_iso}_{duration_iso}"
    """
    dataset: str
    window_start: datetime
    aggregation: timedelta

    @property
    def window_id(self) -> str:
        return f"{self.window_start.isoformat()}_{duration_isoformat(self.aggregation)}"

    @property
    def dataset_key(self) -> str:
        return f"{self.dataset}:{self.window_id}"

    @classmethod
    def from_window_id(cls, dataset: str, window_id: str) -> "WindowRef":
        start_iso, duration_iso = window_id.rsplit("_", 1)
        ws = datetime.fromisoformat(start_iso)
        dur = parse_duration(duration_iso)
        agg = dur if isinstance(dur, timedelta) else dur.totimedelta()
        return cls(dataset=dataset, window_start=ws, aggregation=agg)

    @classmethod
    def from_dataset_key(cls, dataset_key: str) -> "WindowRef":
        dataset, window_id = dataset_key.split(":", 1)
        return cls.from_window_id(dataset, window_id)
