# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""The log-bucketed lateness histogram: bucketing is monotone and the read-back
is a conservative over-estimate of the requested percentile."""

from collections import Counter

import numpy as np
import pytest
from ionbeam.storage.lateness_histogram import (
    MAX_BUCKET,
    bucket_ceiling,
    bucket_counts,
    bucket_for,
    percentile_seconds,
)


def _histogram(*latencies_s: float) -> dict[int, int]:
    return dict(Counter(bucket_for(s) for s in latencies_s))


def test_bucket_counts_matches_per_element_bucket_for():
    """The vectorised batch bucketer agrees with bucket_for element-by-element,
    so recording a whole batch is identical to recording each datum in turn."""
    seconds = np.array([0.0, 0.5, 1.0, 5.0, 60.0, 3600.0, 6 * 3600.0, 6 * 3600.0])
    assert bucket_counts(seconds) == _histogram(*seconds)


def test_bucket_counts_drops_non_finite_and_empty():
    assert bucket_counts(np.array([])) == {}
    assert bucket_counts(np.array([np.nan, np.inf, 3600.0])) == _histogram(3600.0)


@pytest.mark.parametrize(
    "seconds, bucket",
    [(0.0, 0), (0.5, 0), (1.0, 0), (10_000_000, MAX_BUCKET)],
)
def test_bucketing_clamps_at_both_ends(seconds, bucket):
    assert bucket_for(seconds) == bucket


def test_bucketing_is_monotone_nondecreasing():
    buckets = [bucket_for(s) for s in range(1, 100_000, 137)]
    assert buckets == sorted(buckets)


def test_below_min_samples_is_unknown():
    assert percentile_seconds(_histogram(*[60.0] * 10), 0.95, min_samples=50) is None


def test_percentile_over_estimates_toward_completeness():
    # 950 arrivals at ~1 min, 50 stragglers at ~1 h; p95 sits at the last of the
    # fast bucket, and the estimate is its ceiling — never an under-estimate.
    counts = _histogram(*([60.0] * 950 + [3600.0] * 50))
    p95 = percentile_seconds(counts, 0.95, min_samples=100)
    assert p95 is not None
    assert 60.0 <= p95 <= bucket_ceiling(bucket_for(60.0))
    # pushing the percentile into the straggler tail returns a much longer wait
    p99 = percentile_seconds(counts, 0.99, min_samples=100)
    assert p99 > p95 * 10


def test_empty_histogram_is_unknown():
    assert percentile_seconds({}, 0.95, min_samples=1) is None
