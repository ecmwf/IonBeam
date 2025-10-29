import asyncio
import math
import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from uuid import UUID, uuid4

import structlog
from faststream.rabbit import RabbitBroker
from pydantic import BaseModel, Field, field_validator

from ..models.models import StartSourceCommand

logger = structlog.get_logger(__name__)

def _sanitize_segment(seg: str) -> str:
    clean = re.compile(r"[^A-Za-z0-9._-]+").sub("_", seg.strip())
    return clean or "unknown"

class SourceSchedule(BaseModel):
    source_name: str
    window_size: timedelta = Field(..., description="Window size (> 0)")
    trigger_interval: timedelta = Field(..., description="Trigger interval (> 0)")
    window_lag: timedelta = Field(default=timedelta(0), description="Lag offset (>= 0)")
    id: UUID = Field(default_factory=uuid4)

    @field_validator("window_size")
    @classmethod
    def _validate_window_size(cls, v: timedelta):
        if v.total_seconds() <= 0:
            raise ValueError("window_size must be > 0")
        return v

    @field_validator("trigger_interval")
    @classmethod
    def _validate_trigger_interval(cls, v: timedelta):
        if v.total_seconds() <= 0:
            raise ValueError("trigger_interval must be > 0")
        return v

    @field_validator("window_lag")
    @classmethod
    def _validate_window_lag(cls, v: timedelta):
        if v.total_seconds() < 0:
            raise ValueError("window_lag must be >= 0")
        return v

    def get_window_bounds(self, trigger_time: datetime) -> tuple[datetime, datetime]:
        end = trigger_time - self.window_lag
        start = end - self.window_size
        return start, end

class SchedulerConfig(BaseModel):
    windows: Optional[List[SourceSchedule]] = None
class SourceScheduler:
    """Schedules sliding window triggers, aligned to wall-clock boundaries."""

    def __init__(self, config: SchedulerConfig, broker: RabbitBroker):
        self._tasks: dict[UUID, asyncio.Task] = {}
        self._broker = broker
        self.source_schedules: List[SourceSchedule] = list(config.windows or [])
        # Internal publisher cache
        self._publishers: dict[str, object] = {}

    def _routing_key_for(self, source_name: str) -> str:
        return f"ionbeam.source.{_sanitize_segment(source_name)}.start"

    def _publisher_for(self, source_name: str):
        pub = self._publishers.get(source_name)
        if pub is None:
            pub = self._broker.publisher(self._routing_key_for(source_name))
            self._publishers[source_name] = pub
        return pub

    async def trigger_source(self, source_name: str, start_time: datetime, end_time: datetime, id: Optional[UUID] = None) -> Optional[StartSourceCommand]:
        """
        Trigger a source manually without scheduling.
        This is the core business logic for starting sources.
        """
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
            )
            return command
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Failed to trigger source", source=source_name)
            return None

    async def _startSource(self, window: SourceSchedule) -> Optional[StartSourceCommand]:
        """
        Compute the window and publish a StartSourceCommand.
        Returns the command on success, None on failure.
        """
        now = datetime.now(timezone.utc)
        window_start, window_end = window.get_window_bounds(now)

        try:
            result = await self.trigger_source(window.source_name, window_start, window_end)
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

    def _add_window(self, window: SourceSchedule, now: datetime | None = None) -> None:
        """Add a window to schedule."""
        if window.id in self._tasks:
            raise ValueError(f"Window {window.id} already exists")

        start_time = now or datetime.now(timezone.utc)
        task_name = f"schedule-{window.source_name}-{window.id}"
        task = asyncio.create_task(self._schedule_window(window, start_time), name=task_name)
        self._tasks[window.id] = task

    def start(self) -> None:
        for window in self.source_schedules:
            logger.info("Starting schedule for window", source=window.source_name, window_id=str(window.id))
            self._add_window(window)

    async def stop(self) -> None:
        """Stop all scheduling tasks and wait for them to finish."""
        for task in self._tasks.values():
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()

    async def _schedule_window(self, window: SourceSchedule, start_time: datetime) -> None:
        """
        Schedule a single window with strict wall-clock alignment.
        Fires at the next boundary of 'trigger_interval' (e.g., every :00, :05, :10 ...).
        """
        try:
            while True:
                try:
                    now = datetime.now(timezone.utc)
                    next_trigger = self._next_aligned_time(now, window.trigger_interval)

                    sleep_time = (next_trigger - now).total_seconds()
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)

                    await self._startSource(window)

                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Error while scheduling window", source=window.source_name, window_id=str(window.id))
                    # Small backoff then resume aligned to next boundary
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("Window schedule cancelled", source=window.source_name, window_id=str(window.id))

    def _next_aligned_time(self, current: datetime, interval: timedelta) -> datetime:
        """
        Get the next UTC time aligned to an interval boundary
        """
        sec = interval.total_seconds()
        if sec <= 0:
            raise ValueError("trigger_interval must be > 0")
        epoch = current.timestamp()  # float seconds since epoch
        next_epoch = (math.floor(epoch / sec) + 1) * sec
        return datetime.fromtimestamp(next_epoch, tz=timezone.utc)

    async def __aenter__(self):
        self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()
