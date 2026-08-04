# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""The log-bucketed lateness histogram: bucketing is monotone and the read-back
is a conservative over-estimate of the requested percentile."""

from collections import Counter

import numpy as np
from ionbeam.storage.lateness_histogram import (
    MAX_BUCKET,
    bucket_ceiling,
    bucket_counts,
    bucket_for,
    percentile_seconds,
)


def _histogram(*latencies_s: float) -> dict[int, int]:
    return dict(Counter(bucket_for(s) for s in latencies_s))


def test_batch_bucketing_agrees_with_per_datum_bucketing():
    """The vectorised batch bucketer agrees with bucket_for element-by-element,
    so recording a whole batch is identical to recording each datum in turn."""
    seconds = np.array([0.0, 0.5, 1.0, 5.0, 60.0, 3600.0, 6 * 3600.0, 6 * 3600.0])
    assert bucket_counts(seconds) == _histogram(*seconds)


def test_non_finite_and_empty_latencies_bucket_to_nothing():
    assert bucket_counts(np.array([])) == {}
    assert bucket_counts(np.array([np.nan, np.inf, 3600.0])) == _histogram(3600.0)


def test_bucketing_is_monotone_and_clamps_at_both_ends():
    """A longer delay never lands in an earlier bucket, and delays outside the
    representable range saturate rather than wrap."""
    buckets = [bucket_for(s) for s in range(1, 100_000, 137)]
    assert buckets == sorted(buckets)
    assert [bucket_for(s) for s in (0.0, 0.5, 1.0)] == [0, 0, 0]
    assert bucket_for(10_000_000) == MAX_BUCKET


def test_a_histogram_below_min_samples_is_unknown():
    assert percentile_seconds(_histogram(*[60.0] * 10), 0.95, min_samples=50) is None
    assert percentile_seconds({}, 0.95, min_samples=1) is None


def test_percentile_over_estimates_toward_completeness():
    # 950 arrivals at ~1 min, 50 late at ~1 h; p95 sits at the last of the
    # fast bucket, and the estimate is its ceiling, always at or above p95.
    counts = _histogram(*([60.0] * 950 + [3600.0] * 50))
    p95 = percentile_seconds(counts, 0.95, min_samples=100)
    assert p95 is not None
    assert 60.0 <= p95 <= bucket_ceiling(bucket_for(60.0))
    # pushing the percentile into the late tail returns a much longer wait
    p99 = percentile_seconds(counts, 0.99, min_samples=100)
    assert p99 > p95 * 10
