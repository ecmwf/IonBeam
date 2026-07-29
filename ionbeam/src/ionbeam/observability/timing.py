# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

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
