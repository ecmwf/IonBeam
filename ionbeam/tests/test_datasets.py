# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from datetime import timedelta

import pytest
from ionbeam.datasets import DatasetProductionConfig, DatasetRegistry
from pydantic import ValidationError


def test_registry_entry_inherits_defaults_and_overrides_per_key():
    registry = DatasetRegistry.from_config(
        {
            "defaults": {"aggregation_span": "PT1H", "rebuild_debounce": "PT10M"},
            "registry": {"netatmo": {"finalize_after": "PT48H"}},
        }
    )

    netatmo = registry.get("netatmo")
    # inherited from defaults — a per-key override must not reset these
    assert netatmo.aggregation_span == timedelta(hours=1)
    assert netatmo.rebuild_debounce == timedelta(minutes=10)
    # the override itself
    assert netatmo.finalize_after == timedelta(hours=48)


def test_unconfigured_dataset_falls_back_to_defaults():
    registry = DatasetRegistry.from_config(
        {"defaults": {"aggregation_span": "PT1H"}, "registry": {}}
    )

    fallback = registry.get("anything")
    assert fallback.aggregation_span == timedelta(hours=1)
    # no explicit finalize_after ⇒ the delay falls to the hot-store retention
    assert fallback.finalize_delay(timedelta(days=7)) == timedelta(days=7)

def test_aggregation_span_must_be_positive():
    DatasetProductionConfig(aggregation_span=timedelta(minutes=10))
    with pytest.raises(ValidationError):
        DatasetProductionConfig(aggregation_span=timedelta(0))
