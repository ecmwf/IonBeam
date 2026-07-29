# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import asyncio
from datetime import timedelta

from ionbeam.scheduler import SourceSchedule, SourceScheduler
from ionbeam.storage.memory_coordination import InMemoryTriggerClaims


def _schedule() -> SourceSchedule:
    return SourceSchedule(
        source_name="live_source",
        window_size=timedelta(hours=1),
        trigger_interval=timedelta(milliseconds=100),
        window_lag=timedelta(minutes=5),
    )


def test_schedule_identity_is_deterministic_across_replicas():
    assert _schedule().id == _schedule().id


async def test_replicas_fire_each_boundary_exactly_once():
    """Three replicas share the claim store, as three ionbeam pods share Valkey:
    every boundary must produce exactly one trigger, and the trigger must be
    byte-identical no matter which replica won it."""
    claims = InMemoryTriggerClaims()
    fired = []

    async def trigger(source_name, start, end, command_id):
        fired.append((command_id, source_name, start, end))

    replicas = [
        SourceScheduler([_schedule()], trigger, claims.try_claim) for _ in range(3)
    ]
    for replica in replicas:
        await replica.start()
    await asyncio.sleep(0.55)
    for replica in replicas:
        await replica.stop()

    assert len(fired) >= 3, "schedules kept firing while replicas were up"
    command_ids = [command_id for command_id, *_ in fired]
    assert len(command_ids) == len(set(command_ids)), "a boundary fired twice"
    for _, source_name, start, end in fired:
        assert source_name == "live_source"
        assert end - start == timedelta(hours=1)


async def test_a_lone_replica_keeps_firing():
    claims = InMemoryTriggerClaims()
    fired = []

    async def trigger(source_name, start, end, command_id):
        fired.append(command_id)

    scheduler = SourceScheduler([_schedule()], trigger, claims.try_claim)
    await scheduler.start()
    await asyncio.sleep(0.35)
    await scheduler.stop()

    assert len(fired) >= 2
