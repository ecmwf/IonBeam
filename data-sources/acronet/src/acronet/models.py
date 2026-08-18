# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from datetime import timedelta

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AcronetConfig(BaseSettings):
    """Configuration for the Acronet data source.

    Non-secret fields come from the config file; credentials (username,
    password, client_secret) come from ACRONET_* env vars — a k8s Secret via
    envFrom, since the Helm chart never renders secrets into the ConfigMap.
    """

    model_config = SettingsConfigDict(env_prefix="ACRONET_", extra="ignore")

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
