# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Core-side checks of an incoming stream against the registered schema contract."""

from __future__ import annotations

import pyarrow as pa
from ionbeam_client.arrow_tools import canonical_arrow_schema
from ionbeam_client.models import IngestionMetadata
from ionbeam_client.schema_meta import SCHEMA_HASH


def _schema_metadata(schema: pa.Schema) -> dict[str, str]:
    if not schema.metadata:
        return {}
    return {key.decode(): value.decode() for key, value in schema.metadata.items()}


def schema_hash_from_schema(schema: pa.Schema) -> str | None:
    return _schema_metadata(schema).get(SCHEMA_HASH)


def schema_difference(schema: pa.Schema, metadata: IngestionMetadata) -> str | None:
    expected = metadata.dataset_schema.canonical_columns
    actual = list(schema.names)

    for column in expected:
        if column not in actual:
            return f"missing declared column '{column}'"
    for column in actual:
        if column not in expected:
            return f"undeclared column '{column}'"
    for index, (expected_name, actual_name) in enumerate(zip(expected, actual)):
        if expected_name != actual_name:
            return (
                f"column order mismatch at position {index}: "
                f"expected '{expected_name}', got '{actual_name}'"
            )

    for expected_field in canonical_arrow_schema(metadata):
        actual_type = schema.field(expected_field.name).type
        if actual_type != expected_field.type:
            return (
                f"column '{expected_field.name}' type mismatch: "
                f"expected {expected_field.type}, got {actual_type}"
            )
    return None


def verify_stream_schema(schema: pa.Schema, metadata: IngestionMetadata) -> None:
    """Reject a stream schema that does not match registered metadata."""
    dataset = metadata.name
    difference = schema_difference(schema, metadata)
    if difference is not None:
        raise ValueError(f"dataset '{dataset}' schema mismatch: {difference}")

    actual_hash = schema_hash_from_schema(schema)
    if actual_hash is None:
        raise ValueError(
            f"dataset '{dataset}' stream schema is missing {SCHEMA_HASH} metadata"
        )
    expected_hash = metadata.schema_hash()
    if actual_hash != expected_hash:
        raise ValueError(
            f"dataset '{dataset}' schema hash mismatch: expected {expected_hash}, "
            f"got {actual_hash}"
        )
