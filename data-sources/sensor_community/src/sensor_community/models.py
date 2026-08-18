# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel


class SensorCommunityConfig(BaseModel):
    base_url: str = "https://archive.sensor.community"
    live_url: str = "https://data.sensor.community/static/v1/data.json"
    poll_interval_seconds: int = 60
    timeout_seconds: int = 60
    concurrency: int = 10
