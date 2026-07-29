# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
import signal
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from typing import Callable, Optional

import structlog

from .client import IonbeamClient
from .config import IonbeamClientConfig

logger = structlog.get_logger(__name__)

Setup = Callable[
    [IonbeamClient, asyncio.Event], Optional[AbstractAsyncContextManager]
]


async def liveness(port: int) -> asyncio.Server:
    """Bare HTTP 200 endpoint for a kubelet livenessProbe. Must serve from the
    event loop doing the work: a wedged loop stops answering and the kubelet
    restarts the pod."""

    async def answer(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            await reader.readuntil(b"\r\n\r\n")
        except (asyncio.IncompleteReadError, ConnectionError):
            writer.close()
            return
        writer.write(
            b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
        )
        await writer.drain()
        writer.close()

    return await asyncio.start_server(answer, port=port)


async def run_source(name: str, config: dict, setup: Setup) -> None:
    """Run one data-source process around a connected :class:`IonbeamClient`.

    Builds the client from the config's ``ionbeam`` section, wires
    SIGINT/SIGTERM to a shutdown event, and calls ``setup(client, shutdown)``
    before connecting so trigger/export handlers can register. ``setup`` may
    return an async context manager (an MQTT driver, a poll loop) to hold open
    alongside the connected client until shutdown. Serves a :func:`liveness`
    endpoint on ``liveness_port`` (default 8080) for the whole run.
    """
    client = IonbeamClient(IonbeamClientConfig(**config.get("ionbeam", {})))

    shutdown = asyncio.Event()

    def request_shutdown(signum, frame):
        logger.info("Received shutdown signal", signal=signum)
        shutdown.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    session = setup(client, shutdown)

    logger.info("Starting data source", source=name)
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(
            await liveness(config.get("liveness_port", 8080))
        )
        await stack.enter_async_context(client)
        if session is not None:
            await stack.enter_async_context(session)
        logger.info("Data source running", source=name)
        await shutdown.wait()
        logger.info("Shutting down", source=name)
    logger.info("Data source stopped", source=name)
