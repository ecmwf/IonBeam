# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from dataclasses import dataclass
from datetime import timedelta

from pydantic import BaseModel, Field


class AcronetConfig(BaseModel):
    """Configuration for the Acronet data source."""

    base_url: str = "https://webdrops.cimafoundation.org/app/"
    token_endpoint: str = "https://testauth.cimafoundation.org/auth/realms/webdrops/protocol/openid-connect/token"
    username: str | None = None
    password: str | None = None
    client_id: str = "webdrops"
    client_secret: str | None = None
    timeout_seconds: float = 30.0
    max_retries: int = 3
    aggregation_minutes: int = 60
    maximum_request_size: timedelta = Field(default=timedelta(days=2))
    station_group: str = "ComuneLive%IChange"
    geo_window: tuple[float, float, float, float] = (6.0, 36.0, 18.6, 47.5)
    headers: dict[str, str] | None = None
    verify_ssl: bool = True


@dataclass(frozen=True)
class SensorCatalogEntry:
    sensor_id: str
    station_name: str
    station_id: str
    latitude: float
    longitude: float
    unit: str | None
    sensor_class: str
