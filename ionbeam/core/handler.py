import logging
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import AsyncIterator, Generic, Optional, TypeVar

TInput = TypeVar('TInput')
TOutput = TypeVar('TOutput')

class BaseHandler(ABC, Generic[TInput, TOutput]):
    def __init__(self, name: Optional[str] = None):
        self.name = name or self.__class__.__name__
        self.logger = logging.getLogger(self.name)
    
    @abstractmethod
    async def _handle(self, event: TInput) -> TOutput:
        pass
    
    async def handle(self, event: TInput) -> TOutput:
        async with self._execution_context(event):
            result = self._handle(event)
            if isinstance(result, AsyncIterator): # TODO - bit hacky, but handles AsyncIterator correctly
                return result # type: ignore
            return await result
    
    @asynccontextmanager
    async def _execution_context(self, event: TInput):
        correlation_id = getattr(event, 'id', None)
        
        self.logger.info(
            f"Starting {self.name}",
            extra={
                "correlation_id": str(correlation_id) if correlation_id else None,
                "handler": self.name,
                "input_type": type(event).__name__
            }
        )
        
        # TODO: Start OTEL span, emit start metrics
        try:
            yield
            self.logger.info(f"Completed {self.name}")
        except Exception as e:
            self.logger.error(f"Failed {self.name}: {e}")
            raise
