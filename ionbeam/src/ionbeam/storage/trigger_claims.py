# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Exactly-once claims for scheduler firings across replicas.

Every replica computes the same claim key for the same (schedule, boundary);
whichever claims it first publishes the trigger, the rest skip.
"""

from abc import ABC, abstractmethod
from datetime import timedelta

import redis.asyncio as redis


class TriggerClaims(ABC):
    @abstractmethod
    async def try_claim(self, key: str, ttl: timedelta) -> bool:
        """True if this caller won the claim; False if it was already taken."""


class RedisTriggerClaims(TriggerClaims):
    def __init__(self, client: redis.Redis):
        self._client = client

    async def try_claim(self, key: str, ttl: timedelta) -> bool:
        return bool(
            await self._client.set(
                f"ionbeam:claim:{key}",
                b"1",
                nx=True,
                px=int(ttl.total_seconds() * 1000),
            )
        )
