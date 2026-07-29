# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from ionbeam_client.models import (
    CfSemantics,
    DatasetSchema,
    IngestionMetadata,
    Tag,
    TimeCoordinate,
    Variable,
    geographic_point_coordinates,
)


# The E-SOH parameters this dataset carries, all at 2 m sensor height:
# (standard_name, unit, cell_method, period). Both the declared variables and
# the pivot-column rename map derive from this one table, so the parameter key
# and the declared semantics cannot disagree on the (level, method, period) axes.
_PARAMETERS = [
    ("air_temperature", "degC", "point", "PT0S"),
    ("relative_humidity", "%", "point", "PT0S"),
    ("surface_air_pressure", "hPa", "point", "PT0S"),
    ("wind_from_direction", "degree", "mean", "PT5M"),
    ("wind_speed", "m s-1", "mean", "PT5M"),
    ("wind_speed_of_gust", "m s-1", "mean", "PT5M"),
    ("precipitation_amount", "kg m-2", "sum", "PT1M"),
]


def _esoh(standard_name: str, unit: str, cell_method: str, period: str) -> list[Variable]:
    """One E-SOH parameter: the observed value and its Titanlib status_flag
    ancillary, which shares the value's semantics axes."""

    def _semantics(name: str) -> CfSemantics:
        return CfSemantics(
            standard_name=name, level=2.0, cell_method=cell_method, period=period
        )

    return [
        Variable(
            name=standard_name,
            semantics=_semantics(standard_name),
            unit=unit,
        ),
        Variable(
            name=f"{standard_name}_status_flag",
            dtype="int64",
            semantics=_semantics("status_flag"),
            unit="1",
            ancillary_of=[standard_name],
        ),
    ]


def _parameter_key(standard_name: str, cell_method: str, period: str) -> str:
    return f"{standard_name}:2.0:{cell_method}:{period}"


# Pivoted E-SOH parameter key (and its _qc twin) -> canonical column.
PARAMETER_COLUMNS: dict[str, str] = {"datetime": "time"}
for _name, _, _method, _period in _PARAMETERS:
    _key = _parameter_key(_name, _method, _period)
    PARAMETER_COLUMNS[_key] = _name
    PARAMETER_COLUMNS[f"{_key}_qc"] = f"{_name}_status_flag"


# One dataset for the EUMETNET E-SOH IoT feed. Two topics land here:
#   raw-obs/+/netatmo/#  — the observation value, no quality
#   qc-obs/+/netatmo/#   — the SAME observation (identical level/function/period
#                          and datetime), re-published by FMI's Titanlib QC with
#                          a quality_code (0:unknown, 1:good, 3:poor).
# The QC message re-carries the observation value with its quality_code and
# shares the raw row's (station, time) identity, so the build's revision
# collapse replaces the whole row — value and status_flag reach the ODB with
# no join. This depends on the QC feed republishing the value: a flag-only
# feed would null the value out on collapse.
#
# QC is published ~2h after the observation (pubtime lag). The server-side dataset
# config must keep this window revisable long enough for the QC message to land
# before the window finalises (see the ionbeam service's dataset registry:
# rebuild_debounce / finalize_after).
netatmo_metadata: IngestionMetadata = IngestionMetadata(
    version=4,
    name="netatmo",
    dataset_schema=DatasetSchema(
        time=TimeCoordinate(),
        coordinates=geographic_point_coordinates(),
        variables=[
            variable for parameter in _PARAMETERS for variable in _esoh(*parameter)
        ],
        tags=[
            Tag(name="station_id"),
        ],
    ),
)
