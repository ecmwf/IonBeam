# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Per-dataset build configuration: registry defaults, overrides, and the
day-nesting rule for aggregation spans."""

from datetime import timedelta

import pytest
from ionbeam.datasets import DatasetBuildConfig, DatasetRegistry
from pydantic import ValidationError


def test_registry_entry_inherits_defaults_and_overrides_per_key():
    registry = DatasetRegistry.from_config(
        {
            "defaults": {"aggregation_span": "PT1H", "rebuild_debounce": "PT10M"},
            "registry": {"netatmo": {"dedup_ingestion": True}},
        }
    )

    netatmo = registry.get("netatmo")
    assert netatmo.aggregation_span == timedelta(hours=1)
    assert netatmo.rebuild_debounce == timedelta(minutes=10)
    assert netatmo.dedup_ingestion is True


def test_unconfigured_dataset_falls_back_to_defaults():
    registry = DatasetRegistry.from_config(
        {"defaults": {"aggregation_span": "PT1H"}, "registry": {}}
    )

    fallback = registry.get("anything")
    assert fallback.aggregation_span == timedelta(hours=1)
    assert fallback.seal_delay(timedelta(days=7)) == timedelta(days=7, hours=1)

def test_aggregation_span_must_nest_windows_within_a_day():
    # epoch-aligned windows of a day-dividing span never cross midnight; a
    # build's ib_day partition holds exactly that day's rows
    for span in (timedelta(minutes=10), timedelta(hours=6), timedelta(days=1)):
        DatasetBuildConfig(aggregation_span=span)
    for span in (
        timedelta(0),
        timedelta(hours=7),          # drifts across midnight
        timedelta(days=2),           # wider than a partition
        timedelta(seconds=1.5),      # not whole seconds
    ):
        with pytest.raises(ValidationError):
            DatasetBuildConfig(aggregation_span=span)
