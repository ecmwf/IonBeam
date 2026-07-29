# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from ionbeam_client.models import (
    CfSemantics,
    Coordinate,
    DatasetSchema,
    Variable,
    geographic_point_coordinates,
)
from ionbeam_client.schemes import structural_errors, unit_warnings


def _schema(coordinates) -> DatasetSchema:
    return DatasetSchema(
        coordinates=coordinates,
        variables=[Variable(name="temperature", unit="K")],
    )


def test_current_source_declarations_pass_structural_checks():
    assert structural_errors(_schema(geographic_point_coordinates())) == []
    assert structural_errors(_schema(geographic_point_coordinates(altitude=True))) == []


def test_geographic_axis_unit_must_be_angular():
    schema = _schema([
        Coordinate(name="lat", axis="y", crs="EPSG:4326", unit="m"),
        Coordinate(name="lon", axis="x", crs="EPSG:4326", unit="degrees_east"),
    ])
    errors = structural_errors(schema)
    assert len(errors) == 1
    assert "lat" in errors[0] and "degrees" in errors[0]


def test_geographic_axis_unit_is_required():
    schema = _schema([
        Coordinate(name="lat", axis="y", crs="EPSG:4326", unit="degrees_north"),
        Coordinate(name="lon", axis="x", crs="EPSG:4326"),
    ])
    errors = structural_errors(schema)
    assert len(errors) == 1
    assert "lon" in errors[0]


def test_altitude_unit_must_be_a_length():
    schema = _schema([
        *geographic_point_coordinates(),
        Coordinate(name="altitude", axis="z",
                   semantics=CfSemantics(standard_name="altitude"), unit="hPa"),
    ])
    errors = structural_errors(schema)
    assert len(errors) == 1
    assert "altitude" in errors[0] and "metres" in errors[0]


def test_altitude_in_feet_is_interpretable_and_passes():
    schema = _schema([
        *geographic_point_coordinates(),
        Coordinate(name="altitude", axis="z",
                   semantics=CfSemantics(standard_name="altitude"), unit="ft"),
    ])
    assert structural_errors(schema) == []


def test_non_structural_coordinates_are_never_rejected():
    # A z coordinate without altitude semantics (a sensor height, a pressure
    # level) and a non-geographic coordinate system are pass-through data:
    # nonsense units warn, never error.
    schema = _schema([
        Coordinate(name="height", axis="z", unit="banana"),
        Coordinate(name="degree", dtype="int64", unit="not a unit"),
    ])
    assert structural_errors(schema) == []
    warnings = [w for c in schema.coordinates for w in unit_warnings(c)]
    assert len(warnings) == 2


def test_unit_warnings_accepts_variables_and_coordinates():
    assert unit_warnings(Variable(name="temperature", unit="K")) == []
    assert unit_warnings(Coordinate(name="lat", unit="degrees_north")) == []
