# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""The provider's window-key parsing against the builder's key format.

The builder (ionbeam.builds.build_file_key) and this provider share the key
format without a shared import; these tests pin the round-trip so drift
fails here instead of silently disabling window pruning.

pygeoapi is not a dev dependency — the provider runs inside the pygeoapi
image — so its modules are stubbed just enough to import the provider.
"""

import importlib.util
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from ionbeam.builds import build_file_key
from ionbeam.provenance import Window


def _load_provider():
    base = types.ModuleType("pygeoapi.provider.base")
    base.BaseProvider = type("BaseProvider", (), {})
    base.ProviderConnectionError = type("ProviderConnectionError", (Exception,), {})
    base.ProviderItemNotFoundError = type("ProviderItemNotFoundError", (Exception,), {})
    parquet = types.ModuleType("pygeoapi.provider.parquet")
    parquet.ParquetProvider = type("ParquetProvider", (base.BaseProvider,), {})
    provider_pkg = types.ModuleType("pygeoapi.provider")
    provider_pkg.base, provider_pkg.parquet = base, parquet
    pygeoapi = types.ModuleType("pygeoapi")
    pygeoapi.provider = provider_pkg

    sys.modules.setdefault("pygeoapi", pygeoapi)
    sys.modules.setdefault("pygeoapi.provider", provider_pkg)
    sys.modules.setdefault("pygeoapi.provider.base", base)
    sys.modules.setdefault("pygeoapi.provider.parquet", parquet)

    path = Path(__file__).parent.parent / "ionbeam_parquet.py"
    spec = importlib.util.spec_from_file_location("ionbeam_parquet", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


provider = _load_provider()

START = datetime(2026, 7, 17, tzinfo=timezone.utc)

SPANS = [
    timedelta(minutes=30),
    timedelta(hours=1),
    timedelta(hours=6),
    timedelta(days=1),
    timedelta(days=2),
    timedelta(days=1, hours=6),
]


def _file(window, version, record_hash="3f9c2a1b"):
    return f"{build_file_key(window, version, record_hash)}.parquet"


@pytest.mark.parametrize("span", SPANS, ids=str)
def test_every_build_file_prunes_to_its_window_interval(span):
    window = Window("netatmo", START, span)
    key = _file(window, 3)
    assert provider._window_bounds(key) == (window.start, window.start + span), key


def test_latest_builds_serves_one_version_per_window():
    window = Window("netatmo", START, timedelta(days=1))
    other = Window("netatmo", START + timedelta(days=1), timedelta(days=1))
    v1 = _file(window, 1, "aaaa1111")
    v2 = _file(window, 2, "bbbb2222")
    other_v1 = _file(other, 1)

    assert provider._latest_builds([v1, v2, other_v1]) == sorted([v2, other_v1])


def test_windows_sharing_a_dataset_stay_distinct():
    first = Window("netatmo", START, timedelta(minutes=30))
    second = Window("netatmo", START + timedelta(minutes=30), timedelta(minutes=30))
    day = Window("netatmo", START, timedelta(days=1))  # same stamp, other span
    picked = provider._latest_builds(
        [_file(first, 2, "bbbb2222"), _file(first, 1, "aaaa1111"),
         _file(second, 1), _file(day, 1)]
    )
    assert picked == sorted([_file(first, 2, "bbbb2222"), _file(second, 1), _file(day, 1)])


def test_an_unparseable_id_is_not_found_without_scanning():
    instance = object.__new__(provider.IonbeamParquetProvider)
    instance.time_field = "time"
    # no ds/fs on the instance: any scan attempt would AttributeError,
    # so raising not-found proves nothing was scanned
    with pytest.raises(provider.ProviderItemNotFoundError):
        instance.get("not-a-canonical-id")
    with pytest.raises(provider.ProviderItemNotFoundError):
        instance.get("20269999T990000-41c3100d85287e3d")


def test_unparsed_keys_stay_their_own_window():
    assert provider._latest_builds(["netatmo/oddball.parquet"]) == [
        "netatmo/oddball.parquet"
    ]
    assert provider._window_bounds("netatmo/oddball.parquet") is None


PREFIX = "ionbeam/datasets/netatmo"


class _DirectoryFS:
    """In-memory store listed one directory at a time, like the provider
    reads S3: {directory: [(name, size)]}."""

    def __init__(self, dirs):
        self.dirs = dirs

    def get_file_info(self, selector):
        import pyarrow.fs as pafs

        entries = self.dirs.get(selector.base_dir)
        if entries is None:
            if getattr(selector, "allow_not_found", False):
                return []
            raise FileNotFoundError(selector.base_dir)
        return [
            pafs.FileInfo(f"{selector.base_dir}/{name}",
                          type=pafs.FileType.File, size=size)
            for name, size in entries
        ]


def test_every_window_in_the_manifest_registry_is_served():
    fs = _DirectoryFS({
        PREFIX: [],
        f"{PREFIX}/_manifests": [
            ("20260728T120000_PT1H.json", 1),
            ("20260729T060000_PT1H.json", 1),
        ],
        f"{PREFIX}/ib_year=2026/ib_month=07/ib_day=28": [
            ("20260728T120000_PT1H-v1-aaaa1111.parquet", 1)],
        f"{PREFIX}/ib_year=2026/ib_month=07/ib_day=29": [
            ("20260729T060000_PT1H-v2-bbbb2222.parquet", 1)],
    })

    assert provider._list_build_files(fs, PREFIX) == [
        f"{PREFIX}/ib_year=2026/ib_month=07/ib_day=28/20260728T120000_PT1H-v1-aaaa1111.parquet",
        f"{PREFIX}/ib_year=2026/ib_month=07/ib_day=29/20260729T060000_PT1H-v2-bbbb2222.parquet",
    ]


def test_flat_legacy_files_and_empty_objects():
    fs = _DirectoryFS({
        PREFIX: [
            ("20260724T200000_PT1H-v1-cccc3333.parquet", 1),  # pre-day-dir build
            ("20260724T210000_PT1H-v1-dddd4444.parquet", 0),  # unfinished write
        ],
    })

    assert provider._list_build_files(fs, PREFIX) == [
        f"{PREFIX}/20260724T200000_PT1H-v1-cccc3333.parquet"
    ]


SCHEMA_NAMES = ["time", "lat", "lon", "air_temperature", "ib_geometry", "ib_id"]


def _scanned_columns(monkeypatch, **kwargs):
    """The column projection reaching the inherited scanner for a given read.

    The inherited provider hard-codes 'geometry' as the geometry column name
    (select list, WKB decode, and a silent null-fill when absent); the store's
    column is ib_geometry. These tests pin the rename that keeps features
    positioned."""
    captured = {}
    monkeypatch.setattr(
        provider.ParquetProvider,
        "_read_parquet",
        lambda self, return_scanner=False, **kw: captured.update(kw),
        raising=False,
    )
    instance = object.__new__(provider.IonbeamParquetProvider)
    instance.ds = types.SimpleNamespace(
        schema=types.SimpleNamespace(names=SCHEMA_NAMES)
    )
    instance._read_parquet(**kwargs)
    return {name: str(expr) for name, expr in captured["columns"].items()}


def test_ib_geometry_is_served_as_geometry(monkeypatch):
    columns = _scanned_columns(monkeypatch, columns=list(SCHEMA_NAMES))
    assert columns["geometry"] == "ib_geometry"
    assert "ib_geometry" not in columns
    assert columns["air_temperature"] == "air_temperature"


def test_a_requested_geometry_column_reads_ib_geometry(monkeypatch):
    # The provider appends the literal 'geometry' to explicit property
    # selections; an id lookup passes no columns at all and decodes
    # row['geometry'] from the full scan.
    explicit = _scanned_columns(monkeypatch, columns=["ib_id", "geometry"])
    assert explicit == {"ib_id": "ib_id", "geometry": "ib_geometry"}

    full = _scanned_columns(monkeypatch)
    assert full["geometry"] == "ib_geometry"
