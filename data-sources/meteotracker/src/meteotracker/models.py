# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

from pydantic_settings import BaseSettings, SettingsConfigDict


class MeteoTrackerConfig(BaseSettings):
    # Non-secret fields come from the config file; credentials (username,
    # password) come from METEOTRACKER_* env vars — a k8s Secret via envFrom,
    # since the Helm chart never renders secrets into the ConfigMap.
    model_config = SettingsConfigDict(env_prefix="METEOTRACKER_", extra="ignore")

    base_url: str = "https://app.meteotracker.com/api/"
    token_endpoint: str = "https://app.meteotracker.com/auth/login/api"
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


class MT_Session:
    """Represents a single MeteoTracker trip"""

    id: SessionId
    n_points: int
    start_time: datetime
    author: str
    end_time: datetime | None
    columns: list[str]

    def __init__(self, **d):
        self.id = SessionId(d["_id"])
        self.n_points = int(d["nPoints"])
        self.start_time = datetime.fromisoformat(d["startTime"])
        self.end_time = datetime.fromisoformat(d["endTime"]) if "endTime" in d else None
        self.columns = [k for k in d if isinstance(d[k], dict) and "avgVal" in d[k]]
        self.author = d["by"]
