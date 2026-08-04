# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Structural validation of declared coordinates: uninterpretable units on
structural axes error, pass-through units warn."""

import pytest

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


def _altitude(unit: str) -> Coordinate:
    return Coordinate(
        name="altitude", axis="z",
        semantics=CfSemantics(standard_name="altitude"), unit=unit,
    )


def test_current_source_declarations_pass_structural_checks():
    assert structural_errors(_schema(geographic_point_coordinates())) == []
    assert structural_errors(_schema(geographic_point_coordinates(altitude=True))) == []
    assert structural_errors(_schema([*geographic_point_coordinates(), _altitude("ft")])) == []


@pytest.mark.parametrize(
    "coordinates, culprit, expected",
    [
        (
            [
                Coordinate(name="lat", axis="y", crs="EPSG:4326", unit="m"),
                Coordinate(name="lon", axis="x", crs="EPSG:4326", unit="degrees_east"),
            ],
            "lat",
            "degrees",
        ),
        (
            [
                Coordinate(name="lat", axis="y", crs="EPSG:4326", unit="degrees_north"),
                Coordinate(name="lon", axis="x", crs="EPSG:4326"),
            ],
            "lon",
            "degrees",
        ),
        (
            [*geographic_point_coordinates(), _altitude("hPa")],
            "altitude",
            "metres",
        ),
    ],
    ids=["geographic unit is not angular", "geographic unit missing", "altitude unit is not a length"],
)
def test_structural_coordinate_units_must_be_interpretable(coordinates, culprit, expected):
    """Geographic x/y and altitude are structural: core reads their values for
    geometry and geolocation, so a unit it cannot interpret names the culprit."""
    errors = structural_errors(_schema(coordinates))
    assert len(errors) == 1
    assert culprit in errors[0] and expected in errors[0]


def test_non_structural_coordinates_are_never_rejected():
    # A z coordinate without altitude semantics (a sensor height, a pressure
    # level) and a non-geographic coordinate system are pass-through data:
    # nonsense units warn, never error.
    schema = _schema([
        Coordinate(name="height", axis="z", unit="banana"),
        Coordinate(name="degree", dtype="int64", unit="not a unit"),
    ])
    assert structural_errors(schema) == []
    assert len([w for c in schema.coordinates for w in unit_warnings(c)]) == 2
    assert unit_warnings(Variable(name="temperature", unit="K")) == []
    assert unit_warnings(Coordinate(name="lat", unit="degrees_north")) == []
