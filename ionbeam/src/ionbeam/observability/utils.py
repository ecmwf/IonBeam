from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable
import time


@asynccontextmanager
async def async_timer(observe_fn: Callable[[float], None]) -> AsyncIterator[None]:
    start = time.perf_counter()
    try:
        yield
    finally:
        duration = time.perf_counter() - start
        observe_fn(duration)


class Timer:
    def __init__(self, observe_fn: Callable[[float], None]):
        self._observe_fn = observe_fn
        self._start = None

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._start is not None:
            duration = time.perf_counter() - self._start
            self._observe_fn(duration)
