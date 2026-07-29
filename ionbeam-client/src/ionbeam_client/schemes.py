# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""Declaration checks beyond what the models validate structurally.

Two tiers, matching how the pipeline treats the columns:

* :func:`structural_errors` — the coordinates ionbeam core *interprets*
  (geographic x/y, station altitude) sit at the same tier as the time axis:
  the geo projection and exporters read their values directly, so an
  uninterpretable unit is rejected at registration, not warned about.
* :func:`unit_warnings` — everything else is pass-through data; a unit that
  does not parse is worth a warning, never a rejection.

Both are best-effort client-side (skipped gracefully when the optional
cf_units tooling is absent); the ionbeam service depends on cf_units, so
enforcement at the Flight edge is deterministic.
"""

from __future__ import annotations

from .models import CfSemantics, Coordinate, DatasetSchema, Variable
from .schema_metadata import GEOGRAPHIC_CRS


def unit_warnings(column: Variable | Coordinate) -> list[str]:
    """Warn when a declared unit does not parse as a UDUNITS string.

    Best-effort: skipped gracefully when the optional cf_units tooling is
    absent. Structural semantics validation is pydantic's job on the
    :class:`~ionbeam_client.models.Semantics` union.
    """
    if column.unit is None:
        return []

    try:
        from cf_units import Unit  # type: ignore[import-not-found]
    except ImportError:
        return []

    try:
        Unit(column.unit)
    except Exception as exc:  # cf_units raises several concrete exception types
        return [f"column '{column.name}': invalid UDUNITS unit '{column.unit}': {exc}"]
    return []


def _is_geographic(coordinate: Coordinate) -> bool:
    return (
        coordinate.crs is not None and coordinate.crs.upper() in GEOGRAPHIC_CRS
    )


def structural_errors(dataset_schema: DatasetSchema) -> list[str]:
    """Errors for coordinates whose values ionbeam core interprets.

    Geographic x/y feed the GeoParquet geometry and exporter geolocation, and
    an altitude-semantics z feeds the ODB station altitude — their units must
    be present and convertible to the roles' units (degrees, metres). Skipped
    gracefully without cf_units; the ionbeam service always enforces it.
    """
    try:
        from cf_units import Unit  # type: ignore[import-not-found]
    except ImportError:
        return []

    def _convertible(declared: str | None, target: str) -> bool:
        if declared is None:
            return False
        try:
            return Unit(declared).is_convertible(Unit(target))
        except Exception:
            return False

    errors: list[str] = []
    for coordinate in dataset_schema.coordinates:
        if _is_geographic(coordinate) and coordinate.axis in ("x", "y"):
            if not _convertible(coordinate.unit, "degree"):
                errors.append(
                    f"geographic {coordinate.axis} coordinate '{coordinate.name}' "
                    f"must declare a unit convertible to degrees, got "
                    f"{coordinate.unit!r}"
                )
        elif (
            coordinate.axis == "z"
            and isinstance(coordinate.semantics, CfSemantics)
            and coordinate.semantics.standard_name == "altitude"
        ):
            if not _convertible(coordinate.unit, "m"):
                errors.append(
                    f"altitude coordinate '{coordinate.name}' must declare a "
                    f"unit convertible to metres, got {coordinate.unit!r}"
                )
    return errors
