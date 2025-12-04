# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import time
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Generic, TypeVar

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars

TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")


class BaseHandler(ABC, Generic[TInput, TOutput]):
    def __init__(self, name: str) -> None:
        self.name = name
        self.logger: structlog.BoundLogger = structlog.get_logger(self.name)

    @abstractmethod
    async def _handle(self, event: TInput) -> TOutput: ...

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
            self.logger.info("Handler completed", elapsed_ms=elapsed_ms)
        except Exception as exc:
            elapsed_seconds = time.perf_counter() - started
            elapsed_ms = int(elapsed_seconds * 1000)
            self.logger.exception(
                "Handler failed", error=str(exc), elapsed_ms=elapsed_ms
            )
            raise
        finally:
            clear_contextvars()
