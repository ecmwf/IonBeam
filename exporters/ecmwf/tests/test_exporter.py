# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""ODB export of built windows into 6h analysis cycles: mapping, identity,
assembly, and recovery."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight
import pyodc
import pytest

from ionbeam_client import AvailableDataset
from ionbeam_client.canonical_stream import canonical_arrow_schema
from ionbeam_client.models import (
    CfSemantics,
    Coordinate,
    DatasetSchema,
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
from ecmwf.exporter import DTS_TOKEN_VAR, DataTransferService, analysis_time

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


class StubFlight:
    """The one Flight interaction the exporter depends on: a ``dataset_range``
    lookup answering with the current build of every window starting in the
    range, in window order, then a stream of their batches.

    Publishing a window twice supersedes it, as a revision does on the server.
    Which stored build is current is the server's concern and stays unmodelled
    here."""

    def __init__(self):
        self._windows: dict[tuple[str, datetime], list[pa.RecordBatch]] = {}

    def publish(self, dataset: str, start: datetime, batches) -> None:
        self._windows[(dataset, start)] = list(batches)

    def get_flight_info(self, descriptor):
        cmd = json.loads(descriptor.command)
        assert cmd["op"] == "dataset_range"
        start = datetime.fromisoformat(cmd["start"])
        end = datetime.fromisoformat(cmd["end"])
        in_range = sorted(
            window
            for window in self._windows
            if window[0] == cmd["dataset"] and start <= window[1] < end
        )
        if not in_range:
            raise flight.FlightServerError("no builds in range")
        ticket = flight.Ticket(
            json.dumps(
                {"windows": [[name, at.isoformat()] for name, at in in_range]}
            ).encode()
        )
        return SimpleNamespace(endpoints=[SimpleNamespace(ticket=ticket)])

    def do_get(self, ticket):
        windows = json.loads(ticket.ticket)["windows"]
        return _Reader(
            [
                batch
                for name, at in windows
                for batch in self._windows[(name, datetime.fromisoformat(at))]
            ]
        )


@pytest.fixture
def connection() -> StubFlight:
    return StubFlight()


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


def _batches(df: pd.DataFrame, metadata: IngestionMetadata) -> list[pa.RecordBatch]:
    return [
        pa.RecordBatch.from_pandas(
            df, schema=canonical_arrow_schema(metadata), preserve_index=False
        )
    ]


@pytest.fixture
def publish(connection, sample_ingestion_metadata):
    """Publish a window's current build, so the exporter's cycle resolution finds it."""

    def _publish(start: datetime, df: pd.DataFrame | None = None, metadata=None) -> None:
        connection.publish(
            "test",
            start,
            _batches(
                df if df is not None else _sample_df(),
                metadata or sample_ingestion_metadata,
            ),
        )

    return _publish


@pytest.fixture
def sample_build(publish) -> None:
    """A single window build at T0 (the 00Z window of the 06Z cycle)."""
    publish(T0)


@pytest.fixture
def odb_exporter(tmp_path: Path) -> ODBExporter:
    output_path = tmp_path / "output"
    output_path.mkdir(parents=True, exist_ok=True)

    # zero quiesce: cycles here are far past their cutoff, so the event that
    # pokes a cycle also builds it, observable immediately
    config = ODBExporterConfig(
        output_path=output_path, assembly_quiesce=timedelta(0)
    )
    return ODBExporter(config, variable_map=TEST_VARIABLE_MAP)


def _event(
    start: datetime, end: datetime, dataset: str = "test"
) -> AvailableDataset:
    # the exporter resolves cycle membership from the store; an empty info suffices
    info = flight.FlightInfo(
        pa.schema([]), flight.FlightDescriptor.for_command(b"{}"), [], -1, -1
    )
    return AvailableDataset(
        id=uuid4(),
        dataset=dataset,
        start_time=start,
        end_time=end,
        version=1,
        revisable_until=end + timedelta(days=7),
        info=info,
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


async def test_a_window_build_delivers_a_cycle_odb_with_governance_identity(
    odb_exporter: ODBExporter,
    connection: StubFlight,
    sample_build,
    tmp_path: Path,
) -> None:
    odb_exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

    cycle_file = _cycle_file(tmp_path)
    assert cycle_file.exists() and cycle_file.stat().st_size > 0

    odb_df = pyodc.read_odb(cycle_file, single=True)

    # one row per (input row x mapped varno with a value)
    assert len(odb_df) == 4
    assert set(odb_df["varno@body"]) == {39, 108, 112}
    assert set(odb_df["statid@hdr"]) == {"A", "B"}
    assert set(odb_df["source@hdr"]) == {"test"}

    # crowd-AWS report identity per the ODB governance tables
    identity = ("reportype@hdr", "codetype@hdr", "obstype@hdr", "groupid@hdr")
    assert {column: set(odb_df[column]) for column in identity} == {
        "reportype@hdr": {16090},
        "codetype@hdr": {179},
        "obstype@hdr": {1},
        "groupid@hdr": {17},
    }

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
    # no altitude coordinate declared: stalt encodes as missing
    assert odb_df["stalt@hdr"].isna().all()

    assert sorted(
        zip(
            odb_df["varno@body"],
            odb_df["statid@hdr"],
            odb_df["obsvalue@body"].round(2),
        )
    ) == [(39, "A", 285.45), (108, "A", 101240.0), (112, "A", 5.5), (112, "B", 3.2)]


async def test_obsvalues_convert_from_declared_units_to_varno_units(
    connection: StubFlight, tmp_path: Path, publish
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
    publish(T0, df, metadata)

    exporter = ODBExporter(
        ODBExporterConfig(
            output_path=tmp_path / "output",
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

    odb_df = pyodc.read_odb(_cycle_file(tmp_path), single=True)
    temperature = odb_df[odb_df["varno@body"] == 39]["obsvalue@body"].iloc[0]
    assert abs(temperature - 291.75) < 0.01  # degC -> K
    humidity = odb_df[odb_df["varno@body"] == 58]["obsvalue@body"].iloc[0]
    assert abs(humidity - 57.0) < 0.01  # ODB carries percent, as delivered


async def test_header_coordinates_convert_from_declared_units(
    connection: StubFlight, tmp_path: Path, publish
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
    publish(T0, df, metadata)

    exporter = ODBExporter(
        ODBExporterConfig(
            output_path=tmp_path / "output",
            assembly_quiesce=timedelta(0),
        ),
        variable_map=TEST_VARIABLE_MAP,
    )
    exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

    odb_df = pyodc.read_odb(_cycle_file(tmp_path), single=True)
    assert abs(odb_df["stalt@hdr"].iloc[0] - 100.0) < 0.01  # ft -> m
    assert abs(odb_df["lat@hdr"].iloc[0] - 50.7) < 0.001
    assert abs(odb_df["lon@hdr"].iloc[0] - 7.1) < 0.001


async def test_a_cycle_file_is_the_union_of_its_windows_however_events_arrive(
    odb_exporter: ODBExporter,
    connection: StubFlight,
    publish,
    tmp_path: Path,
) -> None:
    """Each build reads the whole cycle's current windows, so the delivered file
    is their union whatever order events arrive in and however often they repeat.
    Windows of the next cycle deliver separately."""
    for hour in range(7):
        publish(T0 + timedelta(hours=hour))

    # the 06Z cycle's six windows, out of order, one of them replayed
    for hour in (5, 0, 3, 1, 4, 2, 0):
        odb_exporter.export_handler(
            connection,
            _event(T0 + timedelta(hours=hour), T0 + timedelta(hours=hour + 1)),
        )
    odb_exporter.export_handler(
        connection, _event(T0 + timedelta(hours=6), T0 + timedelta(hours=7))
    )

    # six windows, four mapped datums each — the replay added nothing
    assert len(pyodc.read_odb(_cycle_file(tmp_path), single=True)) == 24
    assert _seen_stamp(tmp_path).exists()
    assert (
        len(pyodc.read_odb(_cycle_file(tmp_path, "20250101_12"), single=True))
        == 4
    )


async def test_empty_cycle_is_skipped_without_poisoning_the_reconcile(
    odb_exporter: ODBExporter,
    connection: StubFlight,
    publish,
    tmp_path: Path,
) -> None:
    """A reconcile touches every remembered cycle. One whose range holds no
    current build is skipped, so a later, fully-built cycle still exports; the
    empty cycle stays unstamped and rebuilds once its windows land."""
    # the 12Z cycle is fully built; the 06Z cycle is only *seen*, never built
    for hour in range(6, 12):
        publish(T0 + timedelta(hours=hour))

    # poke a 06Z-cycle window (no builds) then a 12Z-cycle window (built)
    odb_exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))
    odb_exporter.export_handler(
        connection,
        _event(T0 + timedelta(hours=6), T0 + timedelta(hours=7)),
    )

    # the built cycle exported despite the empty one being reconciled first
    assert len(
        pyodc.read_odb(_cycle_file(tmp_path, "20250101_12"), single=True)
    ) == 24
    # the empty 06Z cycle was seen but never stamped built — it will retry
    assert odb_exporter.store.read_stamp("test", T0 + timedelta(hours=6), "seen")
    assert odb_exporter.store.read_stamp("test", T0 + timedelta(hours=6), "built") is None


async def test_event_burst_settles_before_building(
    odb_exporter: ODBExporter,
    connection: StubFlight,
    publish,
    tmp_path: Path,
) -> None:
    """A replayed backlog or straggler burst pokes cycles but does not build
    per event — the cycle waits until it quiesces."""
    publish(T0)
    publish(T0 + timedelta(hours=1))
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

    assert not _cycle_file(tmp_path).exists()


async def test_open_cycle_holds_build_until_its_cutoff(
    odb_exporter: ODBExporter,
    connection: StubFlight,
    tmp_path: Path,
) -> None:
    """A window of the still-open current cycle must not deliver early —
    nothing builds before the data cutoff."""
    now = datetime.now(timezone.utc)
    odb_exporter.export_handler(
        connection, _event(now - timedelta(minutes=30), now)
    )

    assert not list((tmp_path / "output").glob("*.odb"))


async def test_next_event_builds_cycles_a_crash_left_behind(
    odb_exporter: ODBExporter,
    connection: StubFlight,
    publish,
    tmp_path: Path,
) -> None:
    """A crash (or busy burst) can leave a due cycle unbuilt; the seen stamp
    is durable, so any later event's reconcile sweep picks it up."""
    publish(T0)
    publish(T0 + timedelta(hours=6))
    publish(T0 + timedelta(hours=7))

    interrupted = ODBExporter(
        odb_exporter.config.model_copy(
            update={"assembly_quiesce": timedelta(seconds=60)}
        ),
        variable_map=TEST_VARIABLE_MAP,
    )
    interrupted.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

    cycle_file = _cycle_file(tmp_path)
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
    connection: StubFlight,
    publish,
    tmp_path: Path,
) -> None:
    """Past the revision horizon a cycle's stamps are dropped but the delivered
    file survives; a straggler replay after cleanup re-tracks the cycle and
    rebuilds the full file from store truth."""
    publish(T0)
    publish(T0 + timedelta(hours=1))
    exporter = ODBExporter(
        ODBExporterConfig(
            output_path=tmp_path / "output",
            assembly_quiesce=timedelta(0),
            revision_horizon=timedelta(0),
        ),
        variable_map=TEST_VARIABLE_MAP,
    )

    exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))
    cycle_file = _cycle_file(tmp_path)
    assert len(pyodc.read_odb(cycle_file, single=True)) == 8  # both windows
    assert not _seen_stamp(tmp_path).exists()  # forgotten

    exporter.export_handler(
        connection, _event(T0 + timedelta(hours=1), T0 + timedelta(hours=2))
    )
    assert len(pyodc.read_odb(cycle_file, single=True)) == 8  # still whole


async def test_a_multi_batch_build_encodes_in_order_and_flags_declared_rejects(
    odb_exporter: ODBExporter,
    connection: StubFlight,
    sample_ingestion_metadata: IngestionMetadata,
    tmp_path: Path,
) -> None:
    """A window build spanning several batches encodes them in order. QC
    vocabularies are per-source: only a dataset's declared rejected flag values
    mark a datum rejected."""
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

    # two batches under one window build: a passing flag, then a declared-rejected one
    connection.publish("test", T0, [batch(0, 1), batch(1, 3)])
    odb_exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

    odb_df = pyodc.read_odb(_cycle_file(tmp_path), single=True)
    assert len(odb_df) == 2

    by_time = odb_df.sort_values("time@hdr")
    # STATUS_t bits: 1 = active, 4 = rejected
    assert list(by_time["datum_status@body"]) == [1, 4]
    # the raw source QC code rides along in quality@body
    assert list(by_time["quality@body"]) == [1, 3]


async def test_report_identity_is_per_dataset_config(
    odb_exporter: ODBExporter,
    connection: StubFlight,
    sample_build,
    tmp_path: Path,
) -> None:
    """Report identity comes from per-dataset config; unlisted datasets keep the
    crowd-AWS defaults."""
    config = odb_exporter.config.model_copy(
        update={
            "report_identity": {
                "test": ReportIdentity(reportype=16002, codetype=11)
            }
        }
    )
    exporter = ODBExporter(config, variable_map=TEST_VARIABLE_MAP)
    exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

    odb_df = pyodc.read_odb(_cycle_file(tmp_path), single=True)
    assert set(odb_df["reportype@hdr"]) == {16002}
    assert set(odb_df["codetype@hdr"]) == {11}
    assert set(odb_df["obstype@hdr"]) == {1}  # default retained


async def test_long_station_ids_become_stable_digests(
    odb_exporter: ODBExporter,
    connection: StubFlight,
    sample_ingestion_metadata: IngestionMetadata,
    publish,
    tmp_path: Path,
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
    publish(T0, df)

    odb_exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))

    odb_df = pyodc.read_odb(_cycle_file(tmp_path), single=True)
    statids = set(odb_df["statid@hdr"])
    assert "A" in statids
    digest = next(s for s in statids if s != "A")
    assert len(digest) == 8 and not long_id.startswith(digest)


@pytest.fixture
def dts_api():
    """A stand-in DTS API recording the transfers it is asked for; ``refuse``
    turns it into an unavailable one."""
    import json as json_
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    asked: list[dict] = []
    state = {"refuse": False}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            asked.append(
                {
                    "path": self.path,
                    "authorization": self.headers.get("Authorization"),
                    "body": json_.loads(body),
                }
            )
            self.send_response(503 if state["refuse"] else 202)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}", asked, state
    server.shutdown()


@pytest.fixture(scope="module")
def s3_endpoint():
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


async def test_s3_output_delivers_cycle_files(
    s3_endpoint, dts_api, monkeypatch, tmp_path, connection, publish
    ):
    """The delivered cycle file lands at the S3 prefix root, the union of the
    cycle's window builds, and each publish requests its transfer by object
    key."""
    import pyarrow.fs as pafs

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", s3_endpoint)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    dts_url, asked, dts_state = dts_api
    monkeypatch.setenv(DTS_TOKEN_VAR, "robot-token")

    fs = pafs.S3FileSystem(
        endpoint_override=s3_endpoint, region="us-east-1", allow_bucket_creation=True
    )
    fs.create_dir("odb-test")

    publish(T0)
    publish(T0 + timedelta(hours=2))

    exporter = ODBExporter(
        ODBExporterConfig(
            output_path="s3://odb-test/odb",
            assembly_quiesce=timedelta(0),
            dts=DataTransferService(
                api_url=f"{dts_url}/api/v2",
                source="ionbeam-odb",
                destination="ionbeam-perm",
            ),
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

    assert asked and all(
        request["path"] == "/api/v2/dataset-transfers"
        and request["authorization"] == "Bearer robot-token"
        and request["body"]
        == {
            "source": {
                "id": "ionbeam-odb",
                "query": {"target": "odb/test_20250101_06.odb"},
            },
            "destination": {"id": "ionbeam-perm"},
        }
        for request in asked
    )

    built_before = fs.open_input_stream(
        "odb-test/odb/cycles/test/20250101_06.built"
    ).read()
    dts_state["refuse"] = True
    with pytest.raises(httpx.HTTPStatusError):
        exporter.export_handler(connection, _event(T0, T0 + timedelta(hours=1)))
    assert (
        fs.open_input_stream("odb-test/odb/cycles/test/20250101_06.built").read()
        == built_before
    )
