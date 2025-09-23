import time
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Generic, Optional, TypeVar

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars

TInput = TypeVar('TInput')
TOutput = TypeVar('TOutput')

class BaseHandler(ABC, Generic[TInput, TOutput]):
    def __init__(self, name: Optional[str] = None):
        self.name = name or self.__class__.__name__
        self.logger: structlog.BoundLogger = structlog.get_logger(self.name)
    
    @abstractmethod
    async def _handle(self, event: TInput) -> TOutput:
        pass
    
    async def handle(self, event: TInput) -> TOutput:
        async with self._execution_context(event):
            return await self._handle(event)
    
    @asynccontextmanager
    async def _execution_context(self, event: TInput):
        correlation_id = getattr(event, 'id', None)
        
        bind_contextvars(
            correlation_id=str(correlation_id) if correlation_id else "-",
            handler=self.name,
            input_type=type(event).__name__,
        )
        self.logger.info("Handler started")
        t0 = time.perf_counter()
        try:
            yield
            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            self.logger.info("Handler completed", elapsed_ms=elapsed_ms)
        except Exception as e:
            elapsed_ms = int((time.perf_counter() - t0) * 1000)
            self.logger.exception("Handler failed", error=str(e), elapsed_ms=elapsed_ms)
            raise
        finally:
            clear_contextvars()
