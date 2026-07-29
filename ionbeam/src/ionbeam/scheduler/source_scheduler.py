# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
import math
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable
from uuid import UUID, uuid5

import structlog

from .models import SourceSchedule

logger = structlog.get_logger(__name__)

TriggerSource = Callable[[str, datetime, datetime, UUID], Awaitable[None]]
TriggerClaim = Callable[[str, timedelta], Awaitable[bool]]


def next_aligned_time(current: datetime, interval: timedelta) -> datetime:
    sec = interval.total_seconds()
    next_epoch = (math.floor(current.timestamp() / sec) + 1) * sec
    return datetime.fromtimestamp(next_epoch, tz=timezone.utc)


class SourceScheduler:
    """Triggers data sources on wall-clock-aligned intervals, exactly once
    across any number of replicas.

    Every replica runs every schedule and computes identical boundaries, window
    bounds, and trigger ids — all derived from the schedule content and the
    boundary, never from local state. Before firing, a replica must win the
    claim for (schedule, boundary); losers skip. Any replica alive is enough to
    keep triggers flowing, with no leader and no failover.

    A driving adapter: it only calls the trigger and claim ports it is given.
    """

    def __init__(
        self,
        schedules: list[SourceSchedule],
        trigger: TriggerSource,
        claim: TriggerClaim,
    ):
        self._schedules = schedules
        self._trigger = trigger
        self._claim = claim
        self._tasks: dict[UUID, asyncio.Task] = {}

    async def start(self) -> None:
        if self._tasks:
            return
        for schedule in self._schedules:
            if schedule.id in self._tasks:
                raise ValueError(f"Duplicate schedule {schedule.id}")
            logger.info(
                "Starting schedule",
                source=schedule.source_name,
                schedule_id=str(schedule.id),
                trigger_interval=str(schedule.trigger_interval),
                window_size=str(schedule.window_size),
            )
            self._tasks[schedule.id] = asyncio.create_task(
                self._run_schedule(schedule),
                name=f"schedule-{schedule.source_name}-{schedule.id}",
            )

    async def stop(self) -> None:
        if not self._tasks:
            return
        logger.info("Stopping scheduler", active_schedules=len(self._tasks))
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()

    async def _run_schedule(self, schedule: SourceSchedule) -> None:
        while True:
            now = datetime.now(timezone.utc)
            boundary = next_aligned_time(now, schedule.trigger_interval)
            await asyncio.sleep((boundary - now).total_seconds())
            await self._fire(schedule, boundary)

    async def _fire(self, schedule: SourceSchedule, boundary: datetime) -> None:
        claim_key = f"trigger:{schedule.id}:{boundary.isoformat()}"
        start, end = schedule.get_window_bounds(boundary)
        try:
            if not await self._claim(claim_key, schedule.trigger_interval * 2):
                return
            command_id = uuid5(schedule.id, boundary.isoformat())
            await self._trigger(schedule.source_name, start, end, command_id)
            logger.info(
                "Triggered source",
                source=schedule.source_name,
                start=start.isoformat(),
                end=end.isoformat(),
                command_id=str(command_id),
            )
        except Exception:
            logger.exception("Failed to trigger source", source=schedule.source_name)
