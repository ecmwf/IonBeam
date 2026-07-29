# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

import hashlib
import json
import re
from datetime import datetime, timedelta
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, TypeAdapter, model_validator


# "datetime" permits secondary time columns (forecast reference time, QC-processing
# time) as ordinary coordinates/variables; DatasetSchema.time remains the single
# structural axis that windowing operates on.
ScalarDType = Literal["float64", "float32", "int64", "uint64", "bool", "string", "datetime"]

NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
# Every column the platform synthesizes lives under this prefix — ib_geometry,
# ib_id, and the ib_record_id provenance tag. Declared source columns may use
# any other name, including the plain words a source's own standard uses
# (time, year, source, …).
RESERVED_PREFIX = "ib_"


class Link(BaseModel):
    mime_type: str
    title: str
    href: str


class CfSemantics(BaseModel):
    """CF-governed semantic identity.

    ``standard_name`` must come from the CF Standard Name Table
    (https://cfconventions.org/Data/cf-standard-names/current/build/cf-standard-name-table.html);
    ``cell_method`` uses the CF Conventions §7.3 method vocabulary;
    ``level`` and ``period`` follow E-SOH parameter naming (sensor height in
    metres, ISO-8601 aggregation period).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    scheme: Literal["cf"] = "cf"
    standard_name: str
    level: Optional[float] = None
    cell_method: Optional[
        Literal[
            "point", "sum", "mean", "maximum", "minimum",
            "mid_range", "standard_deviation", "variance", "mode", "median",
        ]
    ] = None
    period: Optional[str] = None


Semantics = CfSemantics
SEMANTICS_ADAPTER: TypeAdapter = TypeAdapter(Semantics)


class DeclaredColumn(BaseModel):
    """A declared column, by its canonical name.

    A source's raw column names never reach this contract: whatever a feed calls
    its columns is renamed inside the source's own transform, and frames arrive
    at the client edge already canonical. Unknown fields are rejected so a
    declaration in an outdated shape fails loudly instead of silently carrying
    no semantics."""

    model_config = ConfigDict(extra="forbid")

    name: str                       # canonical column name (NAME_RE, never the ib_ prefix)


class Coordinate(DeclaredColumn):
    """A column locating an observation in some space (not necessarily geographic)."""

    dtype: ScalarDType = "float64"
    axis: Optional[Literal["x", "y", "z"]] = None  # spatial role, if any
    crs: Optional[str] = None       # e.g. "EPSG:4326"; requires axis
    semantics: Optional[Semantics] = None
    unit: Optional[str] = None

    @model_validator(mode="after")
    def _crs_requires_axis(self) -> "Coordinate":
        if self.crs is not None and self.axis is None:
            raise ValueError(f"coordinate '{self.name}': crs requires an axis role")
        return self


class Variable(DeclaredColumn):
    """A measured value column."""

    dtype: ScalarDType = "float64"
    semantics: Optional[Semantics] = None  # None = ungoverned column
    unit: Optional[str] = None
    ancillary_of: list[str] = []    # names of the Variables this one qualifies
                                    # (QC flag, standard error, count) — CF
                                    # "ancillary_variables"

    @property
    def is_primary(self) -> bool:
        return not self.ancillary_of


def cf(name: str, unit: str) -> Variable:
    """A variable whose canonical name is its CF standard name."""
    return Variable(name=name, semantics=CfSemantics(standard_name=name), unit=unit)


class Tag(DeclaredColumn):
    """A low-cardinality string column stored as an influx tag."""


class TimeCoordinate(DeclaredColumn):
    """The phenomenon-time column — the actual UTC instant each observation is
    about, never a nominal, receipt, or reference time. It keeps its declared
    name end-to-end and is located by its ``role=time`` field metadata, like
    every other dimension; other times a standard carries are ordinary
    declared columns."""

    name: str = "time"


class DatasetSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    time: TimeCoordinate = TimeCoordinate()
    coordinates: list[Coordinate] = []
    variables: list[Variable]
    tags: list[Tag] = []

    @model_validator(mode="after")
    def _structural_contract(self) -> "DatasetSchema":
        # -- name validity
        named = (
            [("time", self.time.name)]
            + [("coordinate", c.name) for c in self.coordinates]
            + [("variable", v.name) for v in self.variables]
            + [("tag", t.name) for t in self.tags]
        )
        for kind, name in named:
            if not NAME_RE.match(name):
                raise ValueError(f"{kind} name '{name}' must match {NAME_RE.pattern}")
            if name.startswith(RESERVED_PREFIX):
                raise ValueError(
                    f"{kind} name '{name}' uses the platform prefix '{RESERVED_PREFIX}'"
                )

        # -- uniqueness across all four groups
        names = [n for _, n in named]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"duplicate column names: {sorted(dupes)}")

        # -- at least one variable
        if not self.variables:
            raise ValueError("a dataset must declare at least one variable")

        # -- ancillary links: exist, no self-reference, no cycles
        by_name = {v.name: v for v in self.variables}
        for v in self.variables:
            for target in v.ancillary_of:
                if target not in by_name:
                    raise ValueError(
                        f"variable '{v.name}': ancillary_of references "
                        f"unknown variable '{target}'"
                    )
                if target == v.name:
                    raise ValueError(f"variable '{v.name}' cannot qualify itself")
        seen: set[str] = set()

        def _walk(name: str, path: tuple[str, ...]) -> None:
            if name in path:
                raise ValueError(f"ancillary cycle: {' -> '.join(path + (name,))}")
            if name in seen:
                return
            seen.add(name)
            for target in by_name[name].ancillary_of:
                _walk(target, path + (name,))

        for v in self.variables:
            _walk(v.name, ())

        if not any(v.is_primary for v in self.variables):
            raise ValueError("at least one variable must be primary (no ancillary_of)")
        return self

    @property
    def primary_variables(self) -> list[Variable]:
        return [v for v in self.variables if v.is_primary]

    @property
    def canonical_columns(self) -> list[str]:
        """Declared columns, in order — the only vocabulary the pipeline speaks."""
        return (
            [self.time.name]
            + [c.name for c in self.coordinates]
            + [v.name for v in self.variables]
            + [t.name for t in self.tags]
        )


class DatasetMetadata(BaseModel):
    """The server-side definition of an *output* dataset: how a dataset is
    produced and presented, not what any single source declares at ingestion.

    A data source does not own these fields — they live in server-side per-dataset
    config (see the ionbeam service's dataset registry) and travel outward on the
    :class:`DataSetAvailableEvent` for exporters to consume.
    """

    name: str
    description: str = ""
    aggregation_span: timedelta = timedelta(days=1)
    source_links: list[Link] = []
    keywords: list[str] = []


class IngestionMetadata(BaseModel):
    """What a source declares at ingestion: its dataset identity and the data schema.

    Dataset-production concerns (aggregation span, finalisation, feature type,
    presentation metadata) are deliberately absent — those are the server's to
    decide, keyed by :attr:`name`.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    dataset_schema: DatasetSchema
    version: int = 1

    def schema_hash(self) -> str:
        """Hash of the contract a source owns: its name, the data schema, and the
        version. Structural changes trip it. Dataset-production settings are not
        folded in because a source does not supply them."""
        contract = {
            "name": self.name,
            "version": self.version,
            "dataset_schema": self.dataset_schema.model_dump(mode="json"),
        }
        blob = json.dumps(contract, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode()).hexdigest()


class IngestDataCommand(BaseModel):
    id: UUID
    metadata: IngestionMetadata
    start_time: datetime
    end_time: datetime


class WindowRecord(BaseModel):
    """One aggregation window's rows within a coverage claim: every row the
    claim delivered to that window carries this id as its record tag. Only
    windows that actually received rows get a record."""

    id: UUID
    window_start: datetime


class DataAvailableEvent(BaseModel):
    """A coverage claim. The span is the contiguous range the ingestion swept —
    it answers "was this interval checked", distinguishing missing data from
    data that does not exist — and ``records`` names the row batches born
    inside it, one per window with rows. ``arrived_at`` orders corrections
    across claims when a build collapses overlapping records."""

    id: UUID
    metadata: IngestionMetadata
    start_time: datetime
    end_time: datetime
    arrived_at: datetime
    records: list[WindowRecord] = []


class StartSourceCommand(BaseModel):
    id: UUID
    source_name: str
    start_time: datetime
    end_time: datetime


class DataSetAvailableEvent(BaseModel):
    id: UUID
    metadata: DatasetMetadata
    # the exact store keys of the published build
    dataset_locations: list[str]
    start_time: datetime
    end_time: datetime
    # the window's finalize delay has passed: this build is immutable and no
    # further revisions will be published
    is_final: bool = False


def geographic_point_coordinates(altitude: bool = False) -> list[Coordinate]:
    """Geographic point coordinates (``lat``/``lon``, optionally ``altitude``).

    Latitude and longitude need no semantics — axis role and CRS are their whole
    identity. Altitude carries governed semantics because consumers select the
    station-altitude z by it (a sensor or cloud-base height must not be mistaken
    for it); a source under another scheme declares its z coordinate directly
    with that scheme's semantics.
    """
    coordinates = [
        Coordinate(name="lat", axis="y", crs="EPSG:4326", unit="degrees_north"),
        Coordinate(name="lon", axis="x", crs="EPSG:4326", unit="degrees_east"),
    ]
    if altitude:
        coordinates.append(
            Coordinate(name="altitude", axis="z",
                       semantics=CfSemantics(standard_name="altitude"), unit="m")
        )
    return coordinates
