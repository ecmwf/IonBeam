# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Arrow metadata helpers for the ionbeam dataset schema."""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from .models import (
    SEMANTICS_ADAPTER,
    CfSemantics,
    DatasetMetadata,
    IngestionMetadata,
)

DATASET = "ionbeam.dataset"
# The build's own provenance entry (JSON), stamped into schema metadata so the
# parquet footer keeps the artifact self-describing wherever it is copied.
BUILD = "ionbeam.build"
SCHEMA_HASH = "ionbeam.schema_hash"
ROLE = "ionbeam.role"
AXIS = "ionbeam.axis"
CRS = "ionbeam.crs"
SEMANTICS = "ionbeam.semantics"
UNIT = "ionbeam.unit"
ANCILLARY_OF = "ionbeam.ancillary_of"

ROLE_TIME = "time"
ROLE_COORDINATE = "coordinate"
ROLE_VALUE = "value"
ROLE_TAG = "tag"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _canonical_semantics(semantics: CfSemantics) -> str:
    return _canonical_json(semantics.model_dump(mode="json", exclude_none=True))


def _encode(mapping: dict[str, str]) -> dict[bytes, bytes]:
    return {key.encode(): value.encode() for key, value in mapping.items()}


def _metadata_dict(metadata: dict[bytes, bytes] | None) -> dict[str, str]:
    if not metadata:
        return {}
    return {key.decode(): value.decode() for key, value in metadata.items()}


def _field_metadata(field: pa.Field) -> dict[str, str]:
    return _metadata_dict(field.metadata)


def _json_field_metadata(field: pa.Field, key: str, default: Any) -> Any:
    raw = _field_metadata(field).get(key)
    if raw is None:
        return default
    return json.loads(raw)


def attach_metadata(
    schema: pa.Schema,
    meta: IngestionMetadata,
    dataset: DatasetMetadata | None = None,
) -> pa.Schema:
    """Attach ionbeam schema- and field-level metadata to an Arrow schema.

    ``dataset`` is the server-side production descriptor. A source does not own it,
    so client-produced streams leave it out; the builder attaches it when it writes
    the output dataset file, for exporters to read back with :func:`dataset_metadata`.
    """
    dataset_schema = meta.dataset_schema
    field_meta: dict[str, dict[str, str]] = {
        dataset_schema.time.name: {ROLE: ROLE_TIME},
    }

    for coordinate in dataset_schema.coordinates:
        values = {ROLE: ROLE_COORDINATE}
        if coordinate.axis is not None:
            values[AXIS] = coordinate.axis
        if coordinate.crs is not None:
            values[CRS] = coordinate.crs
        if coordinate.semantics is not None:
            values[SEMANTICS] = _canonical_semantics(coordinate.semantics)
        if coordinate.unit is not None:
            values[UNIT] = coordinate.unit
        field_meta[coordinate.name] = values

    for variable in dataset_schema.variables:
        values = {
            ROLE: ROLE_VALUE,
            ANCILLARY_OF: _canonical_json(variable.ancillary_of),
        }
        if variable.semantics is not None:
            values[SEMANTICS] = _canonical_semantics(variable.semantics)
        if variable.unit is not None:
            values[UNIT] = variable.unit
        field_meta[variable.name] = values

    for tag in dataset_schema.tags:
        field_meta[tag.name] = {ROLE: ROLE_TAG}

    fields = []
    for field in schema:
        merged = _metadata_dict(field.metadata)
        merged.update(field_meta.get(field.name, {}))
        fields.append(field.with_metadata(_encode(merged)))

    schema_metadata = _metadata_dict(schema.metadata)
    schema_metadata.update({SCHEMA_HASH: meta.schema_hash()})
    if dataset is not None:
        schema_metadata[DATASET] = dataset.model_dump_json()
    return pa.schema(fields, metadata=_encode(schema_metadata))


def dataset_metadata(schema: pa.Schema) -> DatasetMetadata:
    metadata = _metadata_dict(schema.metadata)
    try:
        raw = metadata[DATASET]
    except KeyError as exc:
        raise ValueError("schema is missing ionbeam dataset metadata") from exc
    return DatasetMetadata.model_validate_json(raw)


def _fields_with_role(schema: pa.Schema, role: str) -> list[pa.Field]:
    return [field for field in schema if _field_metadata(field).get(ROLE) == role]


# The CRSs ionbeam interprets as geographic (degrees on WGS84). Anything else
# is stored and served untouched but skipped by geo products — reprojection is
# the source's job.
GEOGRAPHIC_CRS = {"EPSG:4326", "CRS84", "OGC:CRS84"}


def _matches_crs_kind(field: pa.Field, crs_kind: str) -> bool:
    crs = _field_metadata(field).get(CRS, "")
    if crs_kind == "geographic":
        return crs.upper() in GEOGRAPHIC_CRS
    return crs == crs_kind


def find_coordinates(
    schema: pa.Schema, *, axis: str | None = None, crs_kind: str | None = None
) -> list[pa.Field]:
    fields = _fields_with_role(schema, ROLE_COORDINATE)
    if axis is not None:
        fields = [field for field in fields if _field_metadata(field).get(AXIS) == axis]
    if crs_kind is not None:
        fields = [field for field in fields if _matches_crs_kind(field, crs_kind)]
    return fields


def time_field(schema: pa.Schema) -> pa.Field:
    fields = _fields_with_role(schema, ROLE_TIME)
    if len(fields) != 1:
        raise ValueError(f"expected exactly one ionbeam time field, found {len(fields)}")
    return fields[0]


def value_fields(schema: pa.Schema, *, primary_only: bool = False) -> list[pa.Field]:
    fields = _fields_with_role(schema, ROLE_VALUE)
    if primary_only:
        fields = [field for field in fields if not _json_field_metadata(field, ANCILLARY_OF, [])]
    return fields


def tag_fields(schema: pa.Schema) -> list[pa.Field]:
    return _fields_with_role(schema, ROLE_TAG)


def ancillaries_of(schema: pa.Schema, name: str) -> list[pa.Field]:
    return [
        field
        for field in value_fields(schema)
        if name in _json_field_metadata(field, ANCILLARY_OF, [])
    ]


def unit(field: pa.Field) -> str | None:
    return _field_metadata(field).get(UNIT)


def semantics(field: pa.Field) -> CfSemantics | None:
    raw = _field_metadata(field).get(SEMANTICS)
    if raw is None:
        return None
    return SEMANTICS_ADAPTER.validate_json(raw)
