# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""A log-bucketed histogram of data-arrival lateness.

Kept per dataset so the coordinator can set each source's build gate from the
p95 of how late its data actually arrives, rather than a hand-tuned constant.
Bucket ``i`` covers ``[BASE**i, BASE**(i+1))`` seconds, giving a bounded
relative error (~9% at BASE=1.2) with a few dozen integer counters per source.
The read-back returns the bucket ceiling, so the estimate errs toward waiting
slightly longer — the safe direction for data completeness."""

import math

import numpy as np

BASE = 1.2
# BASE**(MAX_BUCKET + 1) ≈ 10 days; anything later shares the top bucket
MAX_BUCKET = 74

_LOG_BASE = math.log(BASE)


def bucket_for(seconds: float) -> int:
    if seconds <= 1.0:
        return 0
    return min(MAX_BUCKET, int(math.log(seconds) / _LOG_BASE))


def bucket_counts(seconds: np.ndarray) -> dict[int, int]:
    """Bucket a whole batch of lateness observations at once, as ``{bucket: count}``.

    The per-element mapping is identical to ``bucket_for``; non-finite values are
    dropped. Recording every datum's lateness (not one sample per batch) is what
    lets the histogram see the late-arriving tail, so the p95 reflects when a
    window's late tail actually lands rather than how fresh its newest point is."""
    arr = np.asarray(seconds, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return {}
    buckets = np.zeros(arr.shape, dtype=np.int64)
    over = arr > 1.0
    buckets[over] = np.minimum(MAX_BUCKET, (np.log(arr[over]) / _LOG_BASE).astype(np.int64))
    values, counts = np.unique(buckets, return_counts=True)
    return {int(v): int(c) for v, c in zip(values, counts)}


def bucket_ceiling(bucket: int) -> float:
    return BASE ** (bucket + 1)


def percentile_seconds(
    counts: dict[int, int], percentile: float, min_samples: int
) -> float | None:
    """The lateness at the given percentile, or None until ``min_samples`` land.

    Returns the ceiling of the bucket the percentile falls in — a conservative
    over-estimate of the true value, which biases the build gate toward
    completeness over latency."""
    total = sum(counts.values())
    if total < min_samples:
        return None

    target = percentile * total
    cumulative = 0
    for bucket in sorted(counts):
        cumulative += counts[bucket]
        if cumulative >= target:
            return bucket_ceiling(bucket)
    return bucket_ceiling(max(counts))
