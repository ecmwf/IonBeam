import time
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Generic, TypeVar

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars

from ..observability.metrics import IonbeamMetricsProtocol

TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")


class BaseHandler(ABC, Generic[TInput, TOutput]):
    def __init__(self, name: str, metrics: IonbeamMetricsProtocol | None):
        self.name = name
        self.logger: structlog.BoundLogger = structlog.get_logger(self.name)
        self.metrics = metrics

    @abstractmethod
    async def _handle(self, event: TInput) -> TOutput:
        ...

    async def handle(self, event: TInput) -> TOutput:
        async with self._execution_context(event):
            return await self._handle(event)

    @asynccontextmanager
    async def _execution_context(self, event: TInput):
        correlation_id = getattr(event, "id", None)
        input_type = type(event).__name__

        bind_contextvars(
            correlation_id=str(correlation_id) if correlation_id else "-",
            handler=self.name,
            input_type=input_type,
        )
        self.logger.info("Handler started")
        
        started = time.perf_counter()
        try:
            yield
            elapsed_seconds = time.perf_counter() - started
            elapsed_ms = int(elapsed_seconds * 1000)
            if(self.metrics):
                self.metrics.handlers.record_run(self.name, "success")
                self.metrics.handlers.observe_duration(self.name, elapsed_seconds)
            self.logger.info("Handler completed", elapsed_ms=elapsed_ms)
        except Exception as exc:
            elapsed_seconds = time.perf_counter() - started
            elapsed_ms = int(elapsed_seconds * 1000)
            if(self.metrics):
                self.metrics.handlers.record_run(self.name, "error")
                self.metrics.handlers.observe_duration(self.name, elapsed_seconds)
            self.logger.exception("Handler failed", error=str(exc), elapsed_ms=elapsed_ms)
            raise
        finally:
            clear_contextvars()
