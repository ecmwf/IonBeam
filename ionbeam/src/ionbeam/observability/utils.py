# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

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
