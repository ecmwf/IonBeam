# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

import redis.asyncio as redis

from ..provenance import Window


class BuildQueue(ABC):
    """Every window awaiting a build, each carrying the time it becomes worth
    building. The builder only ever sees windows whose time has come; the
    coordinator's readiness and debounce delays live in the schedule itself."""

    @abstractmethod
    async def schedule(self, window: Window, eligible_at: datetime) -> None:
        """Add a window, or move an already-scheduled window to a new
        eligibility time.

        Never touches leases: a window scheduled while its build is in flight
        stays queued and becomes claimable when that lease is released. A
        record arriving mid-build cannot start a concurrent second build."""

    @abstractmethod
    async def claim_due(self) -> Optional[Window]:
        """Lease the earliest-eligible due window whose lease is not already
        held, or None when nothing is claimable.

        A claimed window whose builder is killed mid-build is reclaimed once
        its lease expires and handed out again."""

    @abstractmethod
    async def requeue(self, window: Window, eligible_at: datetime) -> None:
        """Release the caller's lease and schedule the window in one step —
        the path a failed build takes back to the queue."""

    @abstractmethod
    async def complete(self, window: Window, next_claim_floor: datetime) -> None:
        """Release a window's lease once handled. A window re-scheduled during
        the build stays queued but cannot be claimed before ``next_claim_floor``;
        a later eligibility still stands. Clears the window from the
        dead-letter set."""

    @abstractmethod
    async def park(self, window: Window, next_claim_floor: datetime) -> None:
        """Give up on a window after exhausted retries: release as
        :meth:`complete` and record it on the dead-letter set. A later claim
        that changes the window's content retries it as normal."""

    @abstractmethod
    async def dead_lettered(self) -> list[Window]:
        """The windows the builder has given up on."""


# Atomically claims the earliest due window into the lease set under a deadline,
# skipping windows whose lease is currently held (re-scheduled mid-build).
_CLAIM_SCRIPT = """
local offset = 0
while true do
  local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', offset, 100)
  if #due == 0 then return nil end
  for _, member in ipairs(due) do
    if not redis.call('ZSCORE', KEYS[2], member) then
      redis.call('ZREM', KEYS[1], member)
      redis.call('ZADD', KEYS[2], ARGV[2], member)
      return member
    end
  end
  offset = offset + #due
end
"""

# Returns every lease past its deadline to the queue, due immediately.
_RECLAIM_SCRIPT = """
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1])
for _, member in ipairs(expired) do
  redis.call('ZADD', KEYS[1], ARGV[1], member)
  redis.call('ZREM', KEYS[2], member)
end
return #expired
"""


class RedisBuildQueue(BuildQueue):
    def __init__(
        self,
        client: redis.Redis,
        queue_key: str = "ionbeam:dataset_queue",
        lease_ttl: float = 900.0,
    ):
        self.client = client
        self.queue_key = queue_key
        self.lease_key = f"{queue_key}:leased"
        self.dead_key = f"{queue_key}:dead"
        self._keys = [self.queue_key, self.lease_key]
        self.lease_ttl = lease_ttl
        self._claim = client.register_script(_CLAIM_SCRIPT)
        self._reclaim = client.register_script(_RECLAIM_SCRIPT)

    async def schedule(self, window: Window, eligible_at: datetime) -> None:
        await self.client.zadd(
            self.queue_key, {window.dataset_key: eligible_at.timestamp()}
        )

    async def requeue(self, window: Window, eligible_at: datetime) -> None:
        async with self.client.pipeline(transaction=True) as pipe:
            pipe.zadd(self.queue_key, {window.dataset_key: eligible_at.timestamp()})
            pipe.zrem(self.lease_key, window.dataset_key)
            await pipe.execute()

    async def claim_due(self) -> Optional[Window]:
        now = time.time()
        await self._reclaim(keys=self._keys, args=[now])
        claimed = await self._claim(keys=self._keys, args=[now, now + self.lease_ttl])
        if not claimed:
            return None
        return Window.from_dataset_key(claimed.decode("utf-8"))

    async def complete(self, window: Window, next_claim_floor: datetime) -> None:
        async with self.client.pipeline(transaction=True) as pipe:
            pipe.zadd(
                self.queue_key,
                {window.dataset_key: next_claim_floor.timestamp()},
                gt=True,
                xx=True,
            )
            pipe.zrem(self.lease_key, window.dataset_key)
            pipe.srem(self.dead_key, window.dataset_key)
            await pipe.execute()

    async def park(self, window: Window, next_claim_floor: datetime) -> None:
        async with self.client.pipeline(transaction=True) as pipe:
            pipe.zadd(
                self.queue_key,
                {window.dataset_key: next_claim_floor.timestamp()},
                gt=True,
                xx=True,
            )
            pipe.zrem(self.lease_key, window.dataset_key)
            pipe.sadd(self.dead_key, window.dataset_key)
            await pipe.execute()

    async def dead_lettered(self) -> list[Window]:
        members = await self.client.smembers(self.dead_key)
        return sorted(
            (Window.from_dataset_key(member.decode("utf-8")) for member in members),
            key=lambda window: window.dataset_key,
        )
