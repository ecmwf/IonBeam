# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""ODB-2 export of built datasets, delivered as per-analysis-cycle files.

An event pokes the exporter; the cycle it feeds is rebuilt from the current
build of every window it covers, resolved from the store (one Flight
``dataset_range`` lookup), so out-of-order windows, replays and revisions need
no tracking. Two per-cycle stamps schedule the rebuild; a cycle is forgotten
once quiet past ``revision_horizon``.

codc (not pyodc) encodes: pyodc writes NaN into DOUBLE columns rather than the
ODB missing sentinel. Column set, order and types are fixed per frame so a file
never mixes layouts; codc is not thread-safe, so events are handled serially.
"""

import hashlib
import json
import os
import pathlib
import shutil
import tempfile
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from importlib.metadata import version
from typing import BinaryIO, Dict, Generator, List, Optional

import cf_units
import codc
import pandas as pd
import pyarrow as pa
import pyarrow.fs as pafs
import structlog
from pyarrow import flight
from pydantic import BaseModel, field_validator

from ionbeam_client.models import CfSemantics, DataSetAvailableEvent, Semantics
from ionbeam_client.schema_meta import (
    ancillaries_of,
    find_coordinates,
    semantics,
    time_field,
    unit,
    value_fields,
)

from .varno_map import (
    HEADER_UNITS,
    STATION_ALTITUDE,
    VARIABLE_MAP,
    VarNoMapping,
    quantity,
)


logger = structlog.get_logger(__name__)


class ReportIdentity(BaseModel):
    """ODB report provenance for one dataset, per the governance tables
    (codes.ecmwf.int/odb). Defaults describe crowd-sourced automatic weather
    stations under conventional SYNOP — ionbeam's usual delivery."""

    reportype: int = 16090  # Crowd AWS
    codetype: int = 179  # Crowd AWS
    obstype: int = 1  # SYNOP
    groupid: int = 17  # conventional data


class ODBExporterConfig(BaseModel):
    """Configuration for ODB exporter."""

    # A local directory or an S3 prefix (s3://bucket/prefix).
    output_path: str
    # schemes this exporter registers for; None accepts any. A dataset whose
    # primary variables declare no semantics in these schemes is skipped whole.
    scheme_filter: Optional[List[str]] = None
    # per-dataset overrides; an unlisted dataset exports with the defaults
    report_identity: dict[str, ReportIdentity] = {}
    # QC vocabularies are per-source data (FMI netatmo: 0 unknown, 1 good,
    # 3 poor), so which flag values mean "rejected" must be declared per
    # dataset. Undeclared -> nothing rejected; the raw code always rides in
    # quality@body.
    rejected_flags: dict[str, List[int]] = {}
    assembly_cutoff: timedelta = timedelta(hours=1)
    assembly_quiesce: timedelta = timedelta(minutes=10)
    # Must exceed a window's aggregation span plus its finalize delay, so a late
    # revision never finds its cycle already forgotten.
    revision_horizon: timedelta = timedelta(days=7)

    @field_validator("output_path", mode="before")
    @classmethod
    def _path_to_str(cls, value):
        return str(value)


_COPY_CHUNK = 1 << 20


class OdbStore:
    """Delivered cycle files and per-cycle scheduling stamps, on a local
    directory or an S3 prefix. Stamps are written whole, never
    read-modify-written, so racing replicas never lose each other's writes.
    Publishes are atomic: S3 multipart close, local tmp-and-rename."""

    def __init__(self, output: str):
        if output.startswith("s3://"):
            self.fs: pafs.FileSystem = pafs.S3FileSystem(
                endpoint_override=os.environ.get("AWS_ENDPOINT_URL_S3") or None,
                region=os.environ.get("AWS_DEFAULT_REGION") or None,
                background_writes=True,
            )
            self.root = output[len("s3://"):].rstrip("/")
            self._atomic_close = True
        else:
            self.fs = pafs.LocalFileSystem()
            self.root = str(pathlib.Path(output).absolute())
            self._atomic_close = False

    def cycle_key(self, dataset: str, analysis: datetime) -> str:
        return f"{self.root}/{dataset}_{analysis:%Y%m%d_%H}.odb"

    def _stamp_key(self, dataset: str, analysis: datetime, kind: str) -> str:
        return f"{self.root}/cycles/{dataset}/{analysis:%Y%m%d_%H}.{kind}"

    def read_stamp(
        self, dataset: str, analysis: datetime, kind: str
    ) -> Optional[datetime]:
        try:
            with self.fs.open_input_stream(
                self._stamp_key(dataset, analysis, kind)
            ) as src:
                return datetime.fromisoformat(src.readall().decode())
        except FileNotFoundError:
            return None

    def write_stamp(
        self, dataset: str, analysis: datetime, kind: str, when: datetime
    ) -> None:
        with self._writer(self._stamp_key(dataset, analysis, kind)) as out:
            out.write(when.isoformat().encode())

    def forget(self, dataset: str, analysis: datetime) -> None:
        for kind in ("seen", "built"):
            self.delete(self._stamp_key(dataset, analysis, kind))

    def cycles(self, dataset: str) -> List[datetime]:
        try:
            infos = self.fs.get_file_info(
                pafs.FileSelector(f"{self.root}/cycles/{dataset}")
            )
        except OSError:
            return []
        return [
            datetime.strptime(info.base_name[:-len(".seen")], "%Y%m%d_%H").replace(
                tzinfo=timezone.utc
            )
            for info in infos
            if info.type == pafs.FileType.File and info.base_name.endswith(".seen")
        ]

    @contextmanager
    def _writer(self, key: str) -> Generator[BinaryIO, None, None]:
        if self._atomic_close:
            with self.fs.open_output_stream(key) as out:
                yield out
            return
        pathlib.Path(key).parent.mkdir(parents=True, exist_ok=True)
        tmp = f"{key}.tmp"
        with self.fs.open_output_stream(tmp) as out:
            yield out
        os.replace(tmp, key)

    def publish(self, key: str, source: BinaryIO) -> None:
        with self._writer(key) as out:
            shutil.copyfileobj(source, out, _COPY_CHUNK)

    def delete(self, key: str) -> None:
        try:
            self.fs.delete_file(key)
        except FileNotFoundError:
            pass


# The release version is stamped into the package by CI at image build time.
CREATED_BY = f"ionbeam-{version('ecmwf')}"

# STATUS_t layout for datum_status@body; a datum is either active (usable) or
# rejected (failed its source's QC), never both.
DATUM_STATUS_BITFIELDS = {
    "datum_status@body": [("active", 1), ("passive", 1), ("rejected", 1), ("blacklisted", 1)]
}
_STATUS_ACTIVE = 1
_STATUS_REJECTED = 4

# Every frame of every file carries exactly these columns, in this order, with
# these types — explicit so codc never re-infers per batch (a float column of
# integral values would otherwise flap between DOUBLE and INTEGER mid-file).
ODB_TYPES = {
    "expver@desc": codc.STRING,
    "class@desc": codc.INTEGER,
    "stream@desc": codc.INTEGER,
    "type@desc": codc.INTEGER,
    "creaby@desc": codc.STRING,
    "andate@desc": codc.INTEGER,
    "antime@desc": codc.INTEGER,
    "reportype@hdr": codc.INTEGER,
    "obstype@hdr": codc.INTEGER,
    "codetype@hdr": codc.INTEGER,
    "groupid@hdr": codc.INTEGER,
    "statid@hdr": codc.STRING,
    "source@hdr": codc.STRING,
    "seqno@hdr": codc.INTEGER,
    "stalt@hdr": codc.REAL,
    "lat@hdr": codc.REAL,
    "lon@hdr": codc.REAL,
    "date@hdr": codc.INTEGER,
    "time@hdr": codc.INTEGER,
    "entryno@body": codc.INTEGER,
    "varno@body": codc.INTEGER,
    "vertco_type@body": codc.INTEGER,
    "vertco_reference_1@body": codc.DOUBLE,
    "obsvalue@body": codc.DOUBLE,
    "datum_status@body": codc.BITFIELD,
    # Not a governed ODB column, carried deliberately: the source's raw QC code
    # (e.g. FMI's netatmo quality_code), which datum_status collapses to
    # active/rejected. 0 = unflagged.
    "quality@body": codc.INTEGER,
}
ODB_COLUMNS = list(ODB_TYPES)


# The 6-hourly analysis cycle is institutional: it keys MARS DATE/TIME and the
# delivered file names, so it is not deployment configuration.
ANALYSIS_CYCLE = timedelta(hours=6)


def analysis_time(window_start: datetime, cycle: timedelta) -> datetime:
    """The analysis cycle a window feeds: the first cycle boundary strictly
    after the window opens (with a 6h cycle, a window starting 03:00 belongs to
    the 06Z analysis)."""
    since_epoch = window_start - datetime(1970, 1, 1, tzinfo=window_start.tzinfo)
    return window_start - (since_epoch % cycle) + cycle


def _is_status_flag(field: pa.Field) -> bool:
    sem = semantics(field)
    return isinstance(sem, CfSemantics) and sem.standard_name == "status_flag"


def _statid(station_id: str) -> str:
    """statid@hdr is governed as character*8. A short id passes through; a long
    structured id (whose 8-char prefix would collide across stations) becomes a
    stable 8-char digest."""
    if len(station_id) <= 8:
        return station_id
    return hashlib.sha1(station_id.encode()).hexdigest()[:8]


def _to_unit(values, from_unit: Optional[str], to_unit: Optional[str]):
    """Convert a numpy array from a declared unit to a target unit; a missing
    declared unit is assumed to already be the target, and a missing target
    (a code-table varno) takes the values raw."""
    if to_unit is None or from_unit is None or from_unit == to_unit:
        return values
    return cf_units.Unit(from_unit).convert(values, cf_units.Unit(to_unit))


@dataclass(frozen=True)
class _Target:
    varno: int
    unit: Optional[str]


@dataclass(frozen=True)
class _MappedVariable:
    """A value column with everything the per-batch loop needs, resolved once."""

    column: str
    targets: List[_Target]
    unit: Optional[str]
    quality_column: Optional[str]


@dataclass(frozen=True)
class _OdbCapabilities:
    time_column: str
    x_column: str
    y_column: str
    x_unit: Optional[str]
    y_unit: Optional[str]
    altitude_column: Optional[str]
    altitude_unit: Optional[str]
    variables: List[_MappedVariable]


class ODBExporter:
    """ODB format exporter for ECMWF."""

    def __init__(
        self,
        config: ODBExporterConfig,
        variable_map: List[VarNoMapping] = VARIABLE_MAP,
    ):
        self.config = config
        self.store = OdbStore(config.output_path)
        self.logger = logger.bind(exporter="odb")

        # Lookup from varno-significant quantity to the map entries (varno and
        # target unit travel together): both the map's entries and each field's
        # declared semantics reduce through quantity(), so a source's
        # declaration flavor (level, period, point vs mean) never needs
        # mirroring here.
        self.variable_lookup: Dict[Semantics, List[VarNoMapping]] = defaultdict(list)
        for mapping in variable_map:
            for mapped in mapping.mapped_from:
                self.variable_lookup[quantity(mapped)].append(mapping)

    def _capabilities(self, schema: pa.Schema) -> Optional[_OdbCapabilities]:
        try:
            t_field = time_field(schema)
        except ValueError:
            return None
        x_fields = find_coordinates(schema, axis="x", crs_kind="geographic")
        y_fields = find_coordinates(schema, axis="y", crs_kind="geographic")

        if not x_fields or not y_fields:
            return None

        # stalt@hdr is specifically the station altitude — never "whatever z
        # exists" (a cloud-base or sensor height must not route into the header),
        # so the z coordinate is fetched by its governed semantics.
        altitude = next(
            (
                field
                for field in find_coordinates(schema, axis="z")
                if semantics(field) in STATION_ALTITUDE
            ),
            None,
        )

        primary_fields = value_fields(schema, primary_only=True)

        if self.config.scheme_filter is not None:
            declared = {
                sem.scheme for field in primary_fields
                if (sem := semantics(field)) is not None
            }
            if not declared & set(self.config.scheme_filter):
                self.logger.info(
                    "Dataset declares no variables in this exporter's schemes; skipping",
                    schemes=sorted(declared),
                    scheme_filter=self.config.scheme_filter,
                )
                return None

        variables: List[_MappedVariable] = []
        unmapped: List[str] = []
        for field in primary_fields:
            field_semantics = semantics(field)
            mappings = (
                self.variable_lookup.get(quantity(field_semantics))
                if field_semantics is not None
                else None
            )
            if not mappings:
                unmapped.append(field.name)
                continue

            from_unit = unit(field)
            targets: List[_Target] = []
            for mapping in mappings:
                # A quantity-bearing varno converts from the declared unit; a
                # code-table varno (map unit None) takes the value raw.
                if mapping.unit is not None and from_unit is None:
                    self.logger.error(
                        "Mapped variable lacks unit metadata; skipping",
                        column=field.name,
                        varno=mapping.varno,
                    )
                    continue
                targets.append(_Target(varno=mapping.varno, unit=mapping.unit))
            if not targets:
                continue

            quality_field = next(
                (
                    ancillary
                    for ancillary in ancillaries_of(schema, field.name)
                    if _is_status_flag(ancillary)
                ),
                None,
            )
            variables.append(
                _MappedVariable(
                    column=field.name,
                    targets=targets,
                    unit=from_unit,
                    quality_column=quality_field.name if quality_field else None,
                )
            )

        # Exact-match semantic keys fail silently on convention drift (an attrs
        # mismatch between schema and map), so say which values stayed behind.
        if unmapped:
            self.logger.info(
                "Variables without a varno mapping; not exported", columns=unmapped
            )

        if not variables:
            return None

        for field in (x_fields[0], y_fields[0], altitude):
            if field is not None and unit(field) is None:
                self.logger.debug(
                    "Coordinate lacks unit metadata; assuming the ODB header unit",
                    column=field.name,
                )

        return _OdbCapabilities(
            time_column=t_field.name,
            x_column=x_fields[0].name,
            y_column=y_fields[0].name,
            x_unit=unit(x_fields[0]),
            y_unit=unit(y_fields[0]),
            altitude_column=altitude.name if altitude is not None else None,
            altitude_unit=unit(altitude) if altitude is not None else None,
            variables=variables,
        )

    def _map_canonical_batch_to_odb(
        self,
        dataset_name: str,
        batch: pa.RecordBatch,
        capabilities: _OdbCapabilities,
        identity: ReportIdentity,
        rejected_flags: set[int],
        analysis: datetime,
        seqno_start: int,
    ) -> Optional[pd.DataFrame]:
        df = batch.to_pandas(
            types_mapper={pa.string(): pd.StringDtype(storage="python")}.get
        )

        odb_frames: List[pd.DataFrame] = []

        ts = pd.to_datetime(df[capabilities.time_column], utc=True)
        date_hdr = ts.dt.strftime("%Y%m%d").astype(int)
        time_hdr = ts.dt.strftime("%H%M%S").astype(int)

        if "station_id" in df.columns:
            statid_series = df["station_id"].fillna("UNKNOWN").map(_statid)
        else:
            statid_series = pd.Series("UNKNOWN", index=df.index)

        # Header geolocation converts from the declared coordinate units; a
        # declaration whose units cannot serve these roles is rejected at
        # registration, so a failure here raises rather than mislocating rows.
        if capabilities.altitude_column is not None:
            stalt = pd.Series(
                _to_unit(
                    pd.to_numeric(
                        df[capabilities.altitude_column], errors="coerce"
                    ).to_numpy(),
                    capabilities.altitude_unit,
                    HEADER_UNITS["stalt@hdr"],
                ),
                index=df.index,
            )
        else:
            stalt = pd.Series(float("nan"), index=df.index)

        # Base frame with header/MARS qualifiers. andate/antime are the MARS
        # DATE/TIME keys — the analysis cycle, constant across the whole file.
        base = pd.DataFrame(index=df.index)
        base["expver@desc"] = "xxxx"
        base["class@desc"] = 2  # Research department
        base["stream@desc"] = 1247
        base["type@desc"] = 264
        base["creaby@desc"] = CREATED_BY
        base["andate@desc"] = int(analysis.strftime("%Y%m%d"))
        base["antime@desc"] = int(analysis.strftime("%H%M%S"))
        base["reportype@hdr"] = identity.reportype
        base["obstype@hdr"] = identity.obstype
        base["codetype@hdr"] = identity.codetype
        base["groupid@hdr"] = identity.groupid
        base["statid@hdr"] = statid_series
        base["source@hdr"] = dataset_name[:8]
        base["seqno@hdr"] = seqno_start + pd.RangeIndex(len(df))
        base["stalt@hdr"] = stalt
        base["lat@hdr"] = _to_unit(
            pd.to_numeric(df[capabilities.y_column], errors="coerce").to_numpy(),
            capabilities.y_unit,
            HEADER_UNITS["lat@hdr"],
        )
        base["lon@hdr"] = _to_unit(
            pd.to_numeric(df[capabilities.x_column], errors="coerce").to_numpy(),
            capabilities.x_unit,
            HEADER_UNITS["lon@hdr"],
        )
        base["date@hdr"] = date_hdr
        base["time@hdr"] = time_hdr

        # entryno@body numbers a report's data 1..n; a report is one input row,
        # so the counter lives on the input index and survives across variables.
        entry_counter = pd.Series(0, index=df.index)

        for variable in capabilities.variables:
            col = variable.column

            for target in variable.targets:
                values = pd.to_numeric(df[col], errors="coerce")
                mask = values.notna()
                if not mask.any():
                    continue

                # Variable units are only warned about at registration, not
                # rejected, so an inconvertible declaration surfaces here —
                # raising, never silently dropping the column.
                converted = _to_unit(
                    values[mask].to_numpy(), variable.unit, target.unit
                )

                part = base.loc[mask].copy()
                entry_counter.loc[mask] += 1
                part["entryno@body"] = entry_counter.loc[mask]
                part["varno@body"] = target.varno
                part["vertco_type@body"] = float("nan")
                part["vertco_reference_1@body"] = float("nan")
                part["obsvalue@body"] = pd.Series(converted, index=part.index).astype(
                    float
                )

                # Only the dataset's declared rejected flag values mark a datum
                # rejected — QC vocabularies differ per source, and a bare
                # "nonzero means bad" rule misreads them (FMI's 1 means good).
                # The raw flag value rides along in quality@body regardless.
                if variable.quality_column is not None:
                    qc = (
                        pd.to_numeric(df[variable.quality_column], errors="coerce")
                        .loc[mask]
                        .fillna(0)
                    )
                    part["datum_status@body"] = (
                        qc.isin(rejected_flags)
                        .map({False: _STATUS_ACTIVE, True: _STATUS_REJECTED})
                        .astype(int)
                    )
                    part["quality@body"] = qc.astype(int)
                else:
                    part["datum_status@body"] = _STATUS_ACTIVE
                    part["quality@body"] = 0

                odb_frames.append(part)

        if not odb_frames:
            return None

        return pd.concat(odb_frames, ignore_index=True)[ODB_COLUMNS]

    def export_handler(
        self, connection: flight.FlightClient, event: DataSetAvailableEvent
    ) -> None:
        dataset = event.metadata.name
        if event.end_time - event.start_time > ANALYSIS_CYCLE:
            self.logger.warning(
                "Dataset window spans more than one analysis cycle; "
                "the whole window is filed under its first cycle",
                dataset=dataset,
                window=str(event.end_time - event.start_time),
                cycle=str(ANALYSIS_CYCLE),
            )
        analysis = analysis_time(event.start_time, ANALYSIS_CYCLE)
        self.store.write_stamp(
            dataset, analysis, "seen", datetime.now(timezone.utc)
        )
        self._reconcile(connection, dataset)

    def _reconcile(self, connection: flight.FlightClient, dataset: str) -> None:
        now = datetime.now(timezone.utc)
        for analysis in self.store.cycles(dataset):
            seen = self.store.read_stamp(dataset, analysis, "seen")
            if seen is None:
                continue
            due = (
                now >= analysis + self.config.assembly_cutoff
                and now >= seen + self.config.assembly_quiesce
            )
            if due:
                built = self.store.read_stamp(dataset, analysis, "built")
                if built is None or built < seen:
                    # stamp only a cycle that resolved; an empty range is left
                    # unstamped so a later poke retries once its windows land.
                    if self._build_cycle(connection, dataset, analysis):
                        # stamp the seen we built from: a poke mid-build re-triggers
                        self.store.write_stamp(dataset, analysis, "built", seen)
            if now - seen >= self.config.revision_horizon:
                self.store.forget(dataset, analysis)

    def _build_cycle(
        self, connection: flight.FlightClient, dataset: str, analysis: datetime
    ) -> bool:
        """Build and publish one cycle's ODB file. Returns ``True`` once the
        cycle is handled (whether or not it yielded rows), ``False`` when its
        range holds no current build yet — the caller leaves such a cycle
        unstamped so it retries."""
        descriptor = flight.FlightDescriptor.for_command(
            json.dumps(
                {
                    "op": "dataset_range",
                    "dataset": dataset,
                    "start": (analysis - ANALYSIS_CYCLE).isoformat(),
                    "end": analysis.isoformat(),
                }
            ).encode()
        )
        try:
            flight_info = connection.get_flight_info(descriptor)
        except flight.FlightServerError as exc:
            # A cycle with no current build in its range — its windows are not
            # yet built, or aged out of the canonical store. Skip it; other
            # cycles in this reconcile still export, and a later poke rebuilds
            # this one once its windows land.
            if "no builds in range" in str(exc):
                self.logger.info(
                    "No builds for cycle yet; skipping",
                    dataset=dataset,
                    analysis=analysis.isoformat(),
                )
                return False
            raise
        reader = connection.do_get(flight_info.endpoints[0].ticket)
        capabilities = self._capabilities(reader.schema)
        if capabilities is None:
            reader.cancel()
            self.logger.info("dataset lacks ODB capabilities; skipping", dataset=dataset)
            return True

        identity = self.config.report_identity.get(dataset, ReportIdentity())
        rejected_flags = set(self.config.rejected_flags.get(dataset, []))
        total_rows = 0
        seqno = 0
        with tempfile.TemporaryFile() as out:
            for chunk in reader:
                if chunk.data.num_rows == 0:
                    continue
                odb_df = self._map_canonical_batch_to_odb(
                    dataset, chunk.data, capabilities, identity,
                    rejected_flags, analysis, seqno,
                )
                seqno += chunk.data.num_rows
                if odb_df is None:
                    continue
                codc.encode_odb(
                    odb_df, out, types=ODB_TYPES, bitfields=DATUM_STATUS_BITFIELDS
                )
                total_rows += len(odb_df)

            if total_rows == 0:
                self.logger.warning(
                    "No ODB data generated", dataset=dataset, analysis=analysis.isoformat()
                )
                return True

            out.seek(0)
            cycle_key = self.store.cycle_key(dataset, analysis)
            self.store.publish(cycle_key, out)

        self.logger.info(
            "Built cycle", dataset=dataset, cycle=cycle_key, rows=total_rows
        )
        return True
