# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
import socket
import time

from ionbeam_client.runner import liveness

PROBE_REQUEST = b"GET /healthz HTTP/1.1\r\nHost: probe\r\nConnection: close\r\n\r\n"


def probe(port: int, timeout: float) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.sendall(PROBE_REQUEST)
        try:
            return sock.recv(1024)
        except TimeoutError:
            return b""


async def start() -> tuple[asyncio.Server, int]:
    server = await liveness(0)
    ipv4 = next(s for s in server.sockets if s.family == socket.AF_INET)
    return server, ipv4.getsockname()[1]


async def test_answers_200_while_the_loop_is_free():
    server, port = await start()
    async with server:
        response = await asyncio.get_running_loop().run_in_executor(
            None, probe, port, 2.0
        )
    assert response.startswith(b"HTTP/1.1 200 OK")


async def test_cannot_answer_while_the_loop_is_wedged():
    server, port = await start()
    async with server:
        # no await between submitting the probe and blocking, so the loop
        # cannot accept a connection until after the probe's timeout
        pending = asyncio.get_running_loop().run_in_executor(None, probe, port, 1.0)
        time.sleep(2.0)
        assert await pending == b""
