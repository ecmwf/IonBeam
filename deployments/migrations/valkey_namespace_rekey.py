# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

# /// script
# requires-python = ">=3.11"
# dependencies = ["redis"]
# ///
"""One-off Valkey re-key: move the coordination keys under the ``ionbeam:``
namespace the streams bus and trigger claims already use.

    ingestion_records:* registered_metadata:* coverage_claims:*
    lateness:* delta_stored:* dataset_queue*        ->  ionbeam:<same>
    <dataset>:<window>:state|:desired_records       ->  ionbeam:window:<same>

RENAME preserves TTLs. Run with the core and admin scaled to zero so no
writer races the rename; a re-run is a no-op. Keys already namespaced, and
everything else (streams, claims), are untouched.

Usage:
    uv run valkey_namespace_rekey.py [redis://host:6379/0]
"""

import sys

import redis

_PREFIXES = (
    "ingestion_records:",
    "registered_metadata:",
    "coverage_claims:",
    "lateness:",
    "delta_stored:",
    "dataset_queue",
)
_WINDOW_SUFFIXES = (":state", ":desired_records")


def _new_key(key: str) -> str | None:
    if key.startswith("ionbeam:"):
        return None
    if key.startswith(_PREFIXES):
        return f"ionbeam:{key}"
    if key.endswith(_WINDOW_SUFFIXES):
        return f"ionbeam:window:{key}"
    return None


def main() -> None:
    url = sys.argv[1] if len(sys.argv) > 1 else "redis://localhost:6379/0"
    client = redis.Redis.from_url(url)
    renamed = collided = 0
    for raw in client.scan_iter(count=500):
        key = raw.decode("utf-8")
        new = _new_key(key)
        if new is None:
            continue
        if client.renamenx(key, new):
            renamed += 1
        else:
            collided += 1
            print(f"kept {key}: {new} already exists")
    print(f"done: renamed {renamed}, collisions {collided}")


if __name__ == "__main__":
    main()
