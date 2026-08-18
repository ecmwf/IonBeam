# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timedelta
from uuid import UUID, uuid5

from pydantic import BaseModel, Field, field_validator

_SCHEDULE_NAMESPACE = UUID("6b3f5a2e-9d41-4c8a-b7e0-1f2a3c4d5e6f")


class SourceSchedule(BaseModel):
    """Configuration for a single source schedule.

    Defines when and how often a data source should be triggered,
    along with the time window for each trigger.

    The ``id`` is derived from the schedule's content, so every replica parsing
    the same configuration computes the same identity — the basis for
    exactly-once firing across replicas.
    """

    source_name: str = Field(..., description="Name of the data source to trigger")
    window_size: timedelta = Field(
        ..., description="Size of the time window to fetch (> 0)"
    )
    trigger_interval: timedelta = Field(
        ..., description="How often to trigger the source (> 0)"
    )
    window_lag: timedelta = Field(
        default=timedelta(0), description="Lag offset before window end (>= 0)"
    )

    @property
    def id(self) -> UUID:
        content = (
            f"{self.source_name}|{self.window_size}"
            f"|{self.trigger_interval}|{self.window_lag}"
        )
        return uuid5(_SCHEDULE_NAMESPACE, content)

    @field_validator("window_size")
    @classmethod
    def _validate_window_size(cls, v: timedelta) -> timedelta:
        if v.total_seconds() <= 0:
            raise ValueError("window_size must be > 0")
        return v

    @field_validator("trigger_interval")
    @classmethod
    def _validate_trigger_interval(cls, v: timedelta) -> timedelta:
        if v.total_seconds() <= 0:
            raise ValueError("trigger_interval must be > 0")
        return v

    @field_validator("window_lag")
    @classmethod
    def _validate_window_lag(cls, v: timedelta) -> timedelta:
        if v.total_seconds() < 0:
            raise ValueError("window_lag must be >= 0")
        return v

    def get_window_bounds(self, trigger_time: datetime) -> tuple[datetime, datetime]:
        end = trigger_time - self.window_lag
        start = end - self.window_size
        return start, end
