from dataclasses import dataclass, field
from datetime import datetime

from pydantic import BaseModel


class MeteoTrackerConfig(BaseModel):
    base_url: str = "https://app.meteotracker.com/api/"
    token_endpoint: str = "https://app.meteotracker.com/auth/login/api"
    refresh_endpoint: str = "https://app.meteotracker.com/auth/refreshtoken"
    timeout: int = 30
    max_retries: int = 3
    headers: dict[str, str] | None = None
    max_queries: int = 500
    username: str | None = None
    password: str | None = None
    author_patterns: dict[str, list[str]] = {
        "Bologna": ["bologna_living_lab_.+"],
        "Barcelona": ["barcelona_living_lab_.+", "Barcelona_living_lab_.+"],
        "Genoa": ["CIMA I-Change", "genova_living_lab_.+"],
        "Amsterdam": ["Amsterdam_living_lab_ICHANGE", "Gert-Jan Steeneveld"],
        "Ouagadougou": ["llwa_living_lab_.+"],
        "Dublin": ["Dublin LL"],
        "Jerusalem": ["jerusalem_living_lab_.+"],
    }


SessionId = str
Location = str
Username = str


@dataclass
class MT_Session:
    """Represents a single MeteoTracker trip"""

    id: SessionId
    n_points: int
    offset_tz: str
    start_time: datetime
    author: str
    raw_json: dict = field(repr=False)
    end_time: datetime | None
    columns: list[str]
    living_lab: str | None = None

    def __init__(self, **d):
        self.id = SessionId(d["_id"])
        self.n_points = int(d["nPoints"])
        self.offset_tz = d["offsetTZ"]
        self.start_time = datetime.fromisoformat(d["startTime"])
        self.end_time = datetime.fromisoformat(d["endTime"]) if "endTime" in d else None
        self.columns = [k for k in d if isinstance(d[k], dict) and "avgVal" in d[k]]
        self.author = d["by"]
        self.raw_json = d
