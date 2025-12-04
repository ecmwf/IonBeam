# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

from datetime import timedelta
from typing import List, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


class SourceSchedule(BaseModel):
    """Configuration for a single source schedule.

    Defines when and how often a data source should be triggered,
    along with the time window for each trigger.
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
    id: UUID = Field(
        default_factory=uuid4, description="Unique identifier for this schedule"
    )

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

    def get_window_bounds(self, trigger_time) -> tuple:
        end = trigger_time - self.window_lag
        start = end - self.window_size
        return start, end


class SchedulerConfig(BaseModel):
    windows: Optional[List[SourceSchedule]] = Field(
        default=None, description="List of source schedules to run"
    )
    enabled: bool = Field(default=True, description="Whether the scheduler is enabled")
