# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import asyncio
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID, uuid4

import structlog
from faststream.rabbit import RabbitBroker
from ionbeam_client.models import StartSourceCommand

from .models import SchedulerConfig, SourceSchedule

logger = structlog.get_logger(__name__)


def _sanitize_segment(seg: str) -> str:
    clean = re.compile(r"[^A-Za-z0-9._-]+").sub("_", seg.strip())
    return clean or "unknown"


class SourceScheduler:
    """Schedules sliding window triggers for data sources, aligned to wall-clock boundaries.

    The scheduler maintains multiple schedules, each triggering a data source at regular
    intervals with configurable time windows. Triggers are aligned to UTC clock boundaries
    for predictable execution times.
    """

    def __init__(self, config: SchedulerConfig, broker: RabbitBroker):
        self._tasks: dict[UUID, asyncio.Task] = {}
        self._broker = broker
        self.config = config
        self.source_schedules: list[SourceSchedule] = list(config.windows or [])
        self._publishers: dict[str, object] = {}
        self._running = False

    def _routing_key_for(self, source_name: str) -> str:
        return f"ionbeam.source.{_sanitize_segment(source_name)}.start"

    def _publisher_for(self, source_name: str):
        pub = self._publishers.get(source_name)
        if pub is None:
            pub = self._broker.publisher(self._routing_key_for(source_name))
            self._publishers[source_name] = pub
        return pub

    async def trigger_source(
        self,
        source_name: str,
        start_time: datetime,
        end_time: datetime,
        id: Optional[UUID] = None,
    ) -> Optional[StartSourceCommand]:
        try:
            command = StartSourceCommand(
                id=id or uuid4(),
                source_name=source_name,
                start_time=start_time,
                end_time=end_time,
            )
            publisher = self._publisher_for(source_name)
            await publisher.publish(command)

            logger.info(
                "Triggered source",
                source=source_name,
                start=start_time.isoformat(),
                end=end_time.isoformat(),
                command_id=str(command.id),
            )
            return command
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Failed to trigger source", source=source_name)
            return None

    async def _start_source(
        self, window: SourceSchedule
    ) -> Optional[StartSourceCommand]:
        now = datetime.now(timezone.utc)
        window_start, window_end = window.get_window_bounds(now)

        try:
            result = await self.trigger_source(
                window.source_name, window_start, window_end
            )
            if result:
                logger.info(
                    "Published start command",
                    window_id=str(window.id),
                    event_time=now.isoformat(),
                )
            return result
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Failed to publish start command for window",
                source=window.source_name,
                window_id=str(window.id),
            )
            return None

    def _add_window(
        self, window: SourceSchedule, now: Optional[datetime] = None
    ) -> None:
        if window.id in self._tasks:
            raise ValueError(f"Window {window.id} already exists")

        start_time = now or datetime.now(timezone.utc)
        task_name = f"schedule-{window.source_name}-{window.id}"
        task = asyncio.create_task(
            self._schedule_window(window, start_time), name=task_name
        )
        self._tasks[window.id] = task

    def start(self) -> None:
        if not self.config.enabled:
            logger.info("Scheduler is disabled, not starting schedules")
            return

        if self._running:
            logger.warning("Scheduler already running")
            return

        self._running = True

        for window in self.source_schedules:
            logger.info(
                "Starting schedule for window",
                source=window.source_name,
                window_id=str(window.id),
                trigger_interval=str(window.trigger_interval),
                window_size=str(window.window_size),
            )
            self._add_window(window)

    async def stop(self) -> None:
        if not self._running:
            return

        logger.info("Stopping scheduler", active_schedules=len(self._tasks))

        for task in self._tasks.values():
            task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)

        self._tasks.clear()
        self._running = False
        logger.info("Scheduler stopped")

    async def _schedule_window(
        self, window: SourceSchedule, start_time: datetime
    ) -> None:
        try:
            while True:
                try:
                    now = datetime.now(timezone.utc)
                    next_trigger = self._next_aligned_time(now, window.trigger_interval)

                    sleep_time = (next_trigger - now).total_seconds()
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)

                    await self._start_source(window)

                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Error while scheduling window",
                        source=window.source_name,
                        window_id=str(window.id),
                    )
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info(
                "Window schedule cancelled",
                source=window.source_name,
                window_id=str(window.id),
            )

    def _next_aligned_time(self, current: datetime, interval: timedelta) -> datetime:
        sec = interval.total_seconds()
        if sec <= 0:
            raise ValueError("trigger_interval must be > 0")
        epoch = current.timestamp()
        next_epoch = (math.floor(epoch / sec) + 1) * sec
        return datetime.fromtimestamp(next_epoch, tz=timezone.utc)

    async def __aenter__(self):
        self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()
