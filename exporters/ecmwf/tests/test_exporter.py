# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import json
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Generator
from uuid import uuid4

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight
import pyodc
import pytest

from ionbeam_client.arrow_tools import canonical_arrow_schema
from ionbeam_client.models import (
    CfSemantics,
    Coordinate,
    DatasetSchema,
    DataSetAvailableEvent,
    DatasetMetadata,
    IngestionMetadata,
    Tag,
    TimeCoordinate,
    Variable,
    geographic_point_coordinates,
)

from ecmwf import (
    ODBExporter,
    ODBExporterConfig,
    ReportIdentity,
    VarNoMapping,
)
from ecmwf.exporter import analysis_time

# Bare quantities: the declarations in the fixtures carry level/cell_method/
# period, and matching must be agnostic to that source flavor.
TEST_VARIABLE_MAP = [
    VarNoMapping(varno=39, unit="K", mapped_from=[CfSemantics(standard_name="air_temperature")]),
    VarNoMapping(varno=112, unit="m s-1", mapped_from=[CfSemantics(standard_name="wind_speed")]),
    VarNoMapping(
        varno=108,
        unit="Pa",
        mapped_from=[CfSemantics(standard_name="air_pressure_at_mean_sea_level")],
    ),
]

CYCLE = timedelta(hours=6)

# <dataset>/<YYYYMMDD>/<start stamp>_<span>-v<version>-<hash>
_BUILD = re.compile(
    r"^(?P<dataset>[^/]+)/(?:\d{8}/)?(?P<window>\d{8}T\d{6}_[^-/]+)"
    r"-v(?P<version>\d+)-[0-9a-f]+$"
)


def _window_start(name: str) -> datetime:
    return datetime.strptime(name.split("_", 1)[0], "%Y%m%dT%H%M%S").replace(
        tzinfo=timezone.utc
    )


class _Chunk:
    def __init__(self, data: pa.RecordBatch):
        self.data = data


class _Reader:
    def __init__(self, batches):
        self._batches = batches
        self.schema = batches[0].schema if batches else pa.schema([])

    def __iter__(self):
        return (_Chunk(batch) for batch in self._batches)

    def cancel(self):
        pass


class FakeFlight:
    """Mirrors the server: dataset_range resolves current builds in range;
    do_get streams their batches from the mock store."""

    def __init__(self, store):
        self._store = store

    def get_flight_info(self, descriptor):
        cmd = json.loads(descriptor.command)
        assert cmd["op"] == "dataset_range"
        start = datetime.fromisoformat(cmd["start"])
        end = datetime.fromisoformat(cmd["end"])
        current: dict[str, tuple[int, str]] = {}
        for key in self._store._storage:
            match = _BUILD.match(key)
            if match is None or match["dataset"] != cmd["dataset"]:
                continue
            if not (start <= _window_start(match["window"]) < end):
                continue
            version = int(match["version"])
            name = match["window"]
            if name not in current or version > current[name][0]:
                current[name] = (version, key)
        locations = [key for _, (_, key) in sorted(current.items())]
        if not locations:
            raise flight.FlightServerError("no builds in range")
        ticket = flight.Ticket(
            json.dumps({"op": "dataset", "locations": locations}).encode()
        )
        return SimpleNamespace(endpoints=[SimpleNamespace(ticket=ticket)])

    def do_get(self, ticket):
        locations = json.loads(ticket.ticket)["locations"]
        batches = [
            batch
            for location in locations
            for batch in self._store._storage.get(location, [])
        ]
        return _Reader(batches)


@pytest.fixture
def connection(mock_arrow_store) -> FakeFlight:
    return FakeFlight(mock_arrow_store)


@pytest.fixture
def temp_data_path() -> Generator[Path, None, None]:
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def sample_ingestion_metadata() -> IngestionMetadata:
    return IngestionMetadata(
        name="test",
        dataset_schema=DatasetSchema(
            time=TimeCoordinate(),
            coordinates=geographic_point_coordinates(),
            variables=[
                Variable(
                    name="air_temperature",
                    semantics=CfSemantics(standard_name="air_temperature",
                                          level=1.5, cell_method="mean", period="PT1M"),
                    unit="K",
                ),
                Variable(
                    name="air_temperature_status_flag",
                    semantics=CfSemantics(standard_name="status_flag"),
                    unit="1",
                    ancillary_of=["air_temperature"],
                ),
                Variable(
                    name="wind_speed",
                    semantics=CfSemantics(standard_name="wind_speed",
                                          level=10.0, cell_method="mean", period="PT10M"),
                    unit="m s-1",
                ),
                Variable(
                    name="air_pressure_at_mean_sea_level",
                    semantics=CfSemantics(standard_name="air_pressure_at_mean_sea_level",
                                          level=1.0, cell_method="mean", period="PT1M"),
                    unit="Pa",
                ),
            ],
            tags=[Tag(name="station_id")],
        ),
    )


T0 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _sample_df() -> pd.DataFrame:
    """Two stations: A carries three mapped values (temperature, wind,
    pressure), B only wind — four mapped datums per window build."""
    return pd.DataFrame(
        {
            "time": pd.to_datetime(
                ["2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z"], utc=True
            ),
            "lat": [50.7, 51.7],
            "lon": [7.1, 7.2],
            "station_id": ["A", "B"],
            "air_temperature": [285.45, np.nan],
            "air_temperature_status_flag": [0.0, np.nan],
            "wind_speed": [5.5, 3.2],
            "air_pressure_at_mean_sea_level": [101240.0, np.nan],
        }
    )


def _build_key(start: datetime, *, dataset: str = "test", span: str = "PT1H",
               version: int = 1, digest: str = "deadbeef") -> str:
    return f"{dataset}/{start:%Y%m%d}/{start:%Y%m%dT%H%M%S}_{span}-v{version}-{digest}"


@pytest.fixture
def write_build(arrow_store_writer, sample_ingestion_metadata):
    """Publish a window's current build under a layout-conformant key, so the
    exporter's cycle resolution finds it. Returns the key."""
    schema = canonical_arrow_schema(sample_ingestion_metadata)

    async def _write(start: datetime, df: pd.DataFrame | None = None, **key_kwargs) -> str:
        key = _build_key(start, **key_kwargs)
        await arrow_store_writer(
            key, df if df is not None else _sample_df(), schema=schema
        )
        return key

    return _write


@pytest.fixture
async def sample_build(write_build) -> str:
    """A single window build at T0 (the 00Z window of the 06Z cycle)."""
    return await write_build(T0)


@pytest.fixture
def odb_exporter(temp_data_path: Path) -> ODBExporter:
    output_path = temp_data_path / "output"
    output_path.mkdir(parents=True, exist_ok=True)

    # zero quiesce: cycles here are far past their cutoff, so the event that
    # pokes a cycle also builds it, observable immediately
    config = ODBExporterConfig(
        output_path=output_path, assembly_quiesce=timedelta(0)
    )
    return ODBExporter(config, variable_map=TEST_VARIABLE_MAP)


def _event(
    start: datetime, end: datetime, locations=("ignored",), dataset: str = "test"
) -> DataSetAvailableEvent:
    # dataset_locations rides along but the exporter ignores it: membership is
    # resolved from the store at build time, not taken from the event.
    return DataSetAvailableEvent(
        id=uuid4(),
        metadata=DatasetMetadata(name=dataset, description="Test dataset"),
        dataset_locations=list(locations),
        start_time=start,
        end_time=end,
    )


def _cycle_file(root: Path, analysis: str = "20250101_06") -> Path:
    return root / "output" / f"test_{analysis}.odb"


def _seen_stamp(root: Path, analysis: str = "20250101_06") -> Path:
    return root / "output" / "cycles" / "test" / f"{analysis}.seen"


def test_analysis_time_is_the_next_cycle_boundary():
    assert analysis_time(T0, CYCLE) == T0 + timedelta(hours=6)
    assert analysis_time(T0 + timedelta(hours=5), CYCLE) == T0 + timedelta(hours=6)
    # a window opening exactly on a boundary feeds the *next* cycle
    assert analysis_time(T0 + timedelta(hours=6), CYCLE) == T0 + timedelta(hours=12)
    # crossing midnight rolls the analysis date
    assert analysis_time(T0 + timedelta(hours=18), CYCLE) == datetime(
        2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc
    )


class TestODBExporter:
    async def test_exporter_creates_cycle_odb_with_correct_mapping(
        self,
        odb_exporter: ODBExporter,
        connection: FakeFlight,
        sample_build: str,
        temp_data_path: Path,
    ) -> None:
        odb_exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

        cycle_file = _cycle_file(temp_data_path)
        assert cycle_file.exists() and cycle_file.stat().st_size > 0

        odb_df = pyodc.read_odb(cycle_file, single=True)

        # one row per (input row x mapped varno with a value)
        assert len(odb_df) == 4
        assert set(odb_df["varno@body"]) == {39, 108, 112}
        assert set(odb_df["statid@hdr"]) == {"A", "B"}
        assert set(odb_df["source@hdr"]) == {"test"}

        # crowd-AWS report identity per the ODB governance tables
        assert set(odb_df["reportype@hdr"]) == {16090}
        assert set(odb_df["codetype@hdr"]) == {179}
        assert set(odb_df["obstype@hdr"]) == {1}
        assert set(odb_df["groupid@hdr"]) == {17}

        # MARS DATE/TIME keys are the analysis cycle, constant per file
        assert set(odb_df["andate@desc"]) == {20250101}
        assert set(odb_df["antime@desc"]) == {60000}

        # per-datum date/time stay the observation time
        assert set(odb_df["date@hdr"]) == {20250101}
        assert set(odb_df["time@hdr"]) == {0, 10000}

        # entryno numbers each report's data 1..n: station A carried three
        # values (temperature, wind, pressure), station B only wind
        assert sorted(odb_df.loc[odb_df["statid@hdr"] == "A", "entryno@body"]) == [1, 2, 3]
        assert sorted(odb_df.loc[odb_df["statid@hdr"] == "B", "entryno@body"]) == [1]
        # no vertco declared for these mappings -> encoded as missing
        assert odb_df["vertco_type@body"].isna().all()
        assert odb_df["vertco_reference_1@body"].isna().all()
        assert set(odb_df["datum_status@body"]) == {1}  # STATUS_t active bit
        # no altitude coordinate declared -> stalt encodes as missing, not a value
        assert odb_df["stalt@hdr"].isna().all()

        temp_rows = odb_df[odb_df["varno@body"] == 39]
        assert len(temp_rows) == 1
        assert abs(temp_rows["obsvalue@body"].iloc[0] - 285.45) < 0.01

        pressure_rows = odb_df[odb_df["varno@body"] == 108]
        assert len(pressure_rows) == 1
        assert abs(pressure_rows["obsvalue@body"].iloc[0] - 101240.0) < 0.01

        wind_values = sorted(odb_df[odb_df["varno@body"] == 112]["obsvalue@body"])
        assert abs(wind_values[0] - 3.2) < 0.01
        assert abs(wind_values[1] - 5.5) < 0.01

    async def test_obsvalues_convert_from_declared_units_to_varno_units(
        self, connection: FakeFlight, temp_data_path: Path, arrow_store_writer
    ) -> None:
        metadata = IngestionMetadata(
            name="test",
            dataset_schema=DatasetSchema(
                time=TimeCoordinate(),
                coordinates=geographic_point_coordinates(),
                variables=[
                    Variable(
                        name="air_temperature",
                        semantics=CfSemantics(standard_name="air_temperature"),
                        unit="degC",
                    ),
                    Variable(
                        name="relative_humidity",
                        semantics=CfSemantics(standard_name="relative_humidity"),
                        unit="%",
                    ),
                ],
                tags=[Tag(name="station_id")],
            ),
        )
        df = pd.DataFrame(
            {
                "time": pd.to_datetime(["2025-01-01T00:00:00Z"], utc=True),
                "lat": [50.7],
                "lon": [7.1],
                "station_id": ["A"],
                "air_temperature": [18.6],
                "relative_humidity": [57.0],
            }
        )
        await arrow_store_writer(
            _build_key(T0), df, schema=canonical_arrow_schema(metadata)
        )

        exporter = ODBExporter(
            ODBExporterConfig(
                output_path=temp_data_path / "output",
                assembly_quiesce=timedelta(0),
            ),
            variable_map=[
                VarNoMapping(
                    varno=39,
                    unit="K",
                    mapped_from=[CfSemantics(standard_name="air_temperature")],
                ),
                VarNoMapping(
                    varno=58,
                    unit="%",
                    mapped_from=[CfSemantics(standard_name="relative_humidity")],
                ),
            ],
        )
        exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

        odb_df = pyodc.read_odb(_cycle_file(temp_data_path), single=True)
        temperature = odb_df[odb_df["varno@body"] == 39]["obsvalue@body"].iloc[0]
        assert abs(temperature - 291.75) < 0.01  # degC -> K
        humidity = odb_df[odb_df["varno@body"] == 58]["obsvalue@body"].iloc[0]
        assert abs(humidity - 57.0) < 0.01  # ODB carries percent, as delivered

    async def test_header_coordinates_convert_from_declared_units(
        self, connection: FakeFlight, temp_data_path: Path, arrow_store_writer
    ) -> None:
        """stalt@hdr carries metres whatever length unit the source declared
        its altitude in; lat/lon pass through as degrees."""
        metadata = IngestionMetadata(
            name="test",
            dataset_schema=DatasetSchema(
                time=TimeCoordinate(),
                coordinates=[
                    *geographic_point_coordinates(),
                    Coordinate(name="altitude", axis="z",
                               semantics=CfSemantics(standard_name="altitude"),
                               unit="ft"),
                ],
                variables=[
                    Variable(
                        name="air_temperature",
                        semantics=CfSemantics(standard_name="air_temperature"),
                        unit="K",
                    ),
                ],
                tags=[Tag(name="station_id")],
            ),
        )
        df = pd.DataFrame(
            {
                "time": pd.to_datetime(["2025-01-01T00:00:00Z"], utc=True),
                "lat": [50.7],
                "lon": [7.1],
                "altitude": [328.084],  # 328.084 ft == 100 m
                "station_id": ["A"],
                "air_temperature": [285.0],
            }
        )
        await arrow_store_writer(
            _build_key(T0), df, schema=canonical_arrow_schema(metadata)
        )

        exporter = ODBExporter(
            ODBExporterConfig(
                output_path=temp_data_path / "output",
                assembly_quiesce=timedelta(0),
            ),
            variable_map=TEST_VARIABLE_MAP,
        )
        exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

        odb_df = pyodc.read_odb(_cycle_file(temp_data_path), single=True)
        assert abs(odb_df["stalt@hdr"].iloc[0] - 100.0) < 0.01  # ft -> m
        assert abs(odb_df["lat@hdr"].iloc[0] - 50.7) < 0.001
        assert abs(odb_df["lon@hdr"].iloc[0] - 7.1) < 0.001

    async def test_revised_window_rebuilds_the_cycle_without_duplicating(
        self,
        odb_exporter: ODBExporter,
        connection: FakeFlight,
        sample_build: str,
        temp_data_path: Path,
    ) -> None:
        event = _event(T0, T0 + timedelta(hours=1))

        odb_exporter.export_handler(connection, event)
        odb_exporter.export_handler(connection, event)

        assert len(pyodc.read_odb(_cycle_file(temp_data_path), single=True)) == 4

    async def test_windows_of_one_cycle_build_into_one_file(
        self,
        odb_exporter: ODBExporter,
        connection: FakeFlight,
        write_build,
        temp_data_path: Path,
    ) -> None:
        await write_build(T0)
        await write_build(T0 + timedelta(hours=1))

        odb_exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))
        odb_exporter.export_handler(
            connection, _event(T0 + timedelta(hours=1), T0 + timedelta(hours=2))
        )

        # both windows of the 06Z cycle land in one delivered file
        assert len(pyodc.read_odb(_cycle_file(temp_data_path), single=True)) == 8
        assert _seen_stamp(temp_data_path).exists()

    async def test_windows_in_different_cycles_get_separate_files(
        self,
        odb_exporter: ODBExporter,
        connection: FakeFlight,
        write_build,
        temp_data_path: Path,
    ) -> None:
        await write_build(T0)
        await write_build(T0 + timedelta(hours=6))

        odb_exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))
        odb_exporter.export_handler(
            connection, _event(T0 + timedelta(hours=6), T0 + timedelta(hours=7))
        )

        assert len(pyodc.read_odb(_cycle_file(temp_data_path), single=True)) == 4
        assert len(
            pyodc.read_odb(_cycle_file(temp_data_path, "20250101_12"), single=True)
        ) == 4

    async def test_out_of_order_windows_all_reach_the_delivery(
        self,
        odb_exporter: ODBExporter,
        connection: FakeFlight,
        write_build,
        temp_data_path: Path,
    ) -> None:
        """The whole point: a replay delivers a cycle's windows out of order,
        the cycle-closing window first. Because a build reads the cycle's current
        builds from the store, every window reaches the delivered file regardless
        of arrival order — the case the old index+finalize design silently
        truncated."""
        for hour in range(6):
            await write_build(T0 + timedelta(hours=hour))

        for hour in (5, 0, 3, 1, 4, 2):
            odb_exporter.export_handler(
                connection,
                _event(T0 + timedelta(hours=hour), T0 + timedelta(hours=hour + 1)),
            )

        # six windows, four mapped datums each
        assert len(pyodc.read_odb(_cycle_file(temp_data_path), single=True)) == 24

    async def test_higher_version_build_supersedes_without_duplicating(
        self,
        odb_exporter: ODBExporter,
        connection: FakeFlight,
        write_build,
        temp_data_path: Path,
    ) -> None:
        """When a window has more than one stored build, the cycle takes only its
        current (highest-version) build — a revision replaces, never doubles."""
        cold = _sample_df()
        cold.loc[0, "air_temperature"] = 280.0
        warm = _sample_df()
        warm.loc[0, "air_temperature"] = 290.0
        await write_build(T0, cold, version=1, digest="00000001")
        await write_build(T0, warm, version=2, digest="00000002")

        odb_exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

        odb_df = pyodc.read_odb(_cycle_file(temp_data_path), single=True)
        assert len(odb_df) == 4  # one window's worth, not doubled
        temperature = odb_df[odb_df["varno@body"] == 39]["obsvalue@body"].iloc[0]
        assert abs(temperature - 290.0) < 0.01  # the v2 value

    async def test_empty_cycle_is_skipped_without_poisoning_the_reconcile(
        self,
        odb_exporter: ODBExporter,
        connection: FakeFlight,
        write_build,
        temp_data_path: Path,
    ) -> None:
        """A reconcile touches every remembered cycle. One whose range holds no
        current build (its windows aren't built yet, or aged out) is skipped —
        not fatal — so a later, fully-built cycle still exports, and the empty
        cycle stays unstamped so it rebuilds once its windows land."""
        # the 12Z cycle is fully built; the 06Z cycle is only *seen*, never built
        for hour in range(6, 12):
            await write_build(T0 + timedelta(hours=hour))

        # poke a 06Z-cycle window (no builds) then a 12Z-cycle window (built)
        odb_exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))
        odb_exporter.export_handler(
            connection,
            _event(T0 + timedelta(hours=6), T0 + timedelta(hours=7)),
        )

        # the built cycle exported despite the empty one being reconciled first
        assert len(
            pyodc.read_odb(_cycle_file(temp_data_path, "20250101_12"), single=True)
        ) == 24
        # the empty 06Z cycle was seen but never stamped built — it will retry
        assert odb_exporter.store.read_stamp("test", T0 + timedelta(hours=6), "seen")
        assert odb_exporter.store.read_stamp("test", T0 + timedelta(hours=6), "built") is None

    async def test_event_burst_settles_before_building(
        self,
        odb_exporter: ODBExporter,
        connection: FakeFlight,
        write_build,
        temp_data_path: Path,
    ) -> None:
        """A replayed backlog or straggler burst pokes cycles but does not build
        per event — the cycle waits until it quiesces."""
        await write_build(T0)
        await write_build(T0 + timedelta(hours=1))
        exporter = ODBExporter(
            odb_exporter.config.model_copy(
                update={"assembly_quiesce": timedelta(seconds=60)}
            ),
            variable_map=TEST_VARIABLE_MAP,
        )
        for start in (T0, T0 + timedelta(hours=1), T0):  # third = replay
            exporter.export_handler(
                connection, _event(start, start + timedelta(hours=1))
            )

        assert not _cycle_file(temp_data_path).exists()

    async def test_open_cycle_holds_build_until_its_cutoff(
        self,
        odb_exporter: ODBExporter,
        connection: FakeFlight,
        temp_data_path: Path,
    ) -> None:
        """A window of the still-open current cycle must not deliver early —
        nothing builds before the data cutoff."""
        now = datetime.now(timezone.utc)
        odb_exporter.export_handler(
            connection, _event(now - timedelta(minutes=30), now)
        )

        assert not list((temp_data_path / "output").glob("*.odb"))

    async def test_next_event_builds_cycles_a_crash_left_behind(
        self,
        odb_exporter: ODBExporter,
        connection: FakeFlight,
        write_build,
        temp_data_path: Path,
    ) -> None:
        """A crash (or busy burst) can leave a due cycle unbuilt; the seen stamp
        is durable, so any later event's reconcile sweep picks it up."""
        await write_build(T0)
        await write_build(T0 + timedelta(hours=6))
        await write_build(T0 + timedelta(hours=7))

        interrupted = ODBExporter(
            odb_exporter.config.model_copy(
                update={"assembly_quiesce": timedelta(seconds=60)}
            ),
            variable_map=TEST_VARIABLE_MAP,
        )
        interrupted.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

        cycle_file = _cycle_file(temp_data_path)
        assert not cycle_file.exists()

        # "restart": a fresh exporter over the same store; an unrelated event's
        # reconcile sweep delivers the missed cycle too
        odb_exporter.export_handler(
            connection, _event(T0 + timedelta(hours=6), T0 + timedelta(hours=7))
        )
        assert len(pyodc.read_odb(cycle_file, single=True)) == 4

        # already-current cycles are left untouched by later reconciles
        before = cycle_file.stat().st_mtime_ns
        odb_exporter.export_handler(
            connection, _event(T0 + timedelta(hours=7), T0 + timedelta(hours=8))
        )
        assert cycle_file.stat().st_mtime_ns == before

    async def test_quiet_cycle_is_forgotten_but_a_replay_still_delivers_it_whole(
        self,
        connection: FakeFlight,
        write_build,
        temp_data_path: Path,
    ) -> None:
        """Past the revision horizon a cycle's stamps are dropped (hygiene), but
        the delivered file survives — and a straggler replay after cleanup
        re-tracks the cycle and rebuilds the full file from store truth, losing
        nothing."""
        await write_build(T0)
        await write_build(T0 + timedelta(hours=1))
        exporter = ODBExporter(
            ODBExporterConfig(
                output_path=temp_data_path / "output",
                assembly_quiesce=timedelta(0),
                revision_horizon=timedelta(0),
            ),
            variable_map=TEST_VARIABLE_MAP,
        )

        exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))
        cycle_file = _cycle_file(temp_data_path)
        assert len(pyodc.read_odb(cycle_file, single=True)) == 8  # both windows
        assert not _seen_stamp(temp_data_path).exists()  # forgotten

        exporter.export_handler(
            connection, _event(T0 + timedelta(hours=1), T0 + timedelta(hours=2))
        )
        assert len(pyodc.read_odb(cycle_file, single=True)) == 8  # still whole

    def test_forgetting_an_already_gone_cycle_is_a_noop(
        self, odb_exporter: ODBExporter
    ) -> None:
        """Redelivered or overlapping reconciles race to drop the same cycle's
        stamps; the second forget is a no-op, not an error."""
        odb_exporter.store.forget("test", T0)

    async def test_multi_batch_stream_appends_frames_and_flags_qc(
        self,
        odb_exporter: ODBExporter,
        connection: FakeFlight,
        sample_ingestion_metadata: IngestionMetadata,
        mock_arrow_store,
        temp_data_path: Path,
    ) -> None:
        """A window build spanning several batches encodes them in order; only a
        dataset's *declared* rejected flag values mark a datum rejected — QC
        vocabularies are per-source (FMI's 1 means good, not bad)."""
        odb_exporter = ODBExporter(
            odb_exporter.config.model_copy(update={"rejected_flags": {"test": [3]}}),
            variable_map=TEST_VARIABLE_MAP,
        )
        schema = canonical_arrow_schema(sample_ingestion_metadata)

        def batch(hour: int, flag: int) -> pa.RecordBatch:
            frame = pd.DataFrame(
                {
                    "time": pd.to_datetime([f"2025-01-01T0{hour}:00:00Z"], utc=True),
                    "lat": [50.7],
                    "lon": [7.1],
                    "station_id": ["A"],
                    "air_temperature": [285.45],
                    "air_temperature_status_flag": [flag],
                    "wind_speed": [np.nan],
                    "air_pressure_at_mean_sea_level": [np.nan],
                }
            )
            columns = {
                field.name: frame[field.name]
                if field.name in frame
                else pd.Series([None], dtype="float64")
                for field in schema
            }
            return pa.RecordBatch.from_pandas(
                pd.DataFrame(columns), schema=schema, preserve_index=False
            )

        # two batches under one window build: good (nonzero, not rejected) then poor
        mock_arrow_store._storage[_build_key(T0)] = [batch(0, 1), batch(1, 3)]
        odb_exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

        odb_df = pyodc.read_odb(_cycle_file(temp_data_path), single=True)
        assert len(odb_df) == 2

        by_time = odb_df.sort_values("time@hdr")
        # STATUS_t bits: 1 = active, 4 = rejected
        assert list(by_time["datum_status@body"]) == [1, 4]
        # the raw source QC code rides along, not just its collapse
        assert list(by_time["quality@body"]) == [1, 3]

    async def test_undeclared_qc_vocabulary_rejects_nothing(
        self,
        odb_exporter: ODBExporter,
        connection: FakeFlight,
        sample_build: str,
        temp_data_path: Path,
    ) -> None:
        """Without a declared vocabulary the flag is uninterpretable — collapse
        nothing; the raw code in quality@body keeps the information."""
        odb_exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

        odb_df = pyodc.read_odb(_cycle_file(temp_data_path), single=True)
        assert set(odb_df["datum_status@body"]) == {1}  # all active

    async def test_report_identity_is_per_dataset_config(
        self,
        odb_exporter: ODBExporter,
        connection: FakeFlight,
        sample_build: str,
        temp_data_path: Path,
    ) -> None:
        """A dataset with different provenance is a config entry, not a code
        change; unlisted datasets keep the crowd-AWS defaults."""
        config = odb_exporter.config.model_copy(
            update={
                "report_identity": {
                    "test": ReportIdentity(reportype=16002, codetype=11)
                }
            }
        )
        exporter = ODBExporter(config, variable_map=TEST_VARIABLE_MAP)
        exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

        odb_df = pyodc.read_odb(_cycle_file(temp_data_path), single=True)
        assert set(odb_df["reportype@hdr"]) == {16002}
        assert set(odb_df["codetype@hdr"]) == {11}
        assert set(odb_df["obstype@hdr"]) == {1}  # default retained

    async def test_long_station_ids_become_stable_digests(
        self,
        odb_exporter: ODBExporter,
        connection: FakeFlight,
        sample_ingestion_metadata: IngestionMetadata,
        arrow_store_writer,
        temp_data_path: Path,
    ) -> None:
        """An 8-char prefix of a long structured id collides across stations;
        the exporter digests instead. Short ids pass through untouched."""
        long_id = "0-250-0-9c8e197789cff89e"
        df = pd.DataFrame(
            {
                "time": pd.to_datetime(["2025-01-01T00:00:00Z"] * 2, utc=True),
                "lat": [50.7, 51.7],
                "lon": [7.1, 7.2],
                "station_id": ["A", long_id],
                "air_temperature": [285.45, 286.0],
                "air_temperature_status_flag": [0.0, 0.0],
                "wind_speed": [np.nan, np.nan],
                "air_pressure_at_mean_sea_level": [np.nan, np.nan],
            }
        )
        await arrow_store_writer(
            _build_key(T0), df, schema=canonical_arrow_schema(sample_ingestion_metadata)
        )

        odb_exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

        odb_df = pyodc.read_odb(_cycle_file(temp_data_path), single=True)
        statids = set(odb_df["statid@hdr"])
        assert "A" in statids
        digest = next(s for s in statids if s != "A")
        assert len(digest) == 8 and not long_id.startswith(digest)


@pytest.fixture(scope="module")
def s3_endpoint():
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


async def test_s3_output_delivers_cycle_files(
    s3_endpoint, monkeypatch, tmp_path, connection, write_build
):
    """The delivered cycle file lands at the S3 prefix root, the union of the
    cycle's window builds."""
    import pyarrow.fs as pafs

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", s3_endpoint)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    fs = pafs.S3FileSystem(
        endpoint_override=s3_endpoint, region="us-east-1", allow_bucket_creation=True
    )
    fs.create_dir("odb-test")

    await write_build(T0)
    await write_build(T0 + timedelta(hours=2))

    exporter = ODBExporter(
        ODBExporterConfig(
            output_path="s3://odb-test/odb", assembly_quiesce=timedelta(0)
        ),
        variable_map=TEST_VARIABLE_MAP,
    )

    # two windows of the same 06Z cycle; a repeated poke re-delivers idempotently
    for start in (T0, T0, T0 + timedelta(hours=2)):
        exporter.export_handler(connection, _event(start, start + timedelta(hours=1)))

    cycle_bytes = fs.open_input_stream("odb-test/odb/test_20250101_06.odb").read()
    local = tmp_path / "cycle.odb"
    local.write_bytes(cycle_bytes)
    odb_df = pyodc.read_odb(local, single=True)
    assert len(odb_df) == 8  # 4 mapped values per window, two windows
    assert set(odb_df["varno@body"]) == {39, 108, 112}
