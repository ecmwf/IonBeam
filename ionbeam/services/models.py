from datetime import datetime, timedelta
from typing import Optional
from uuid import UUID

from isodate import duration_isoformat, parse_duration
from pydantic import BaseModel

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
