# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

"""The canonical quantity → ODB varno map.

Which physical quantity a varno denotes is universal knowledge, not deployment
configuration — so the map lives here, in code, and knows nothing about any
data source. Matching is on :func:`quantity`, the varno-significant identity
of a declaration; a variable whose quantity has no entry is logged and not
exported.

See https://codes.ecmwf.int/odb/varno/
"""

from typing import List, Optional

from ionbeam_client.models import CfSemantics, Semantics
from pydantic import BaseModel


class VarNoMapping(BaseModel):
    """One ODB varno, its obsvalue unit, and the quantities that export to it.

    ``unit`` is the unit obsvalue@body carries for this varno
    (https://codes.ecmwf.int/odb/varno/); declared source units convert to it
    at export. ``unit=None`` marks a code/flag-table varno: the value is a
    table entry, not a quantity, and passes through unconverted."""

    varno: int
    unit: Optional[str]
    mapped_from: List[Semantics]


def quantity(sem: Semantics) -> Semantics:
    """The varno-significant identity of a declared semantics.

    A varno names a physical quantity, not a source's declaration flavor.
    ``level`` and ``period`` are observation metadata — every ionbeam export
    today is a near-surface observation; when a profile-shaped source arrives
    they route into vertco/time-significance columns instead of changing the
    varno. An instantaneous reading and a short mean are the same observed
    value, so ``point``/``mean`` drop out; aggregations that change the
    quantity (``sum``, ``minimum``, ``maximum``, ...) stay in the identity so
    a daily-maximum temperature never exports as plain temperature.
    """
    method = None if sem.cell_method in ("point", "mean") else sem.cell_method
    return CfSemantics(standard_name=sem.standard_name, cell_method=method)


# The z-coordinate semantics that mean "station altitude" — what stalt@hdr
# wants, fetched by governed identity so a sensor or cloud-base height never
# routes into the header.
STATION_ALTITUDE = {
    CfSemantics(standard_name="altitude"),
}

# The units ODB header geolocation carries; declared coordinate units convert
# to these at export.
HEADER_UNITS = {
    "lat@hdr": "degree",
    "lon@hdr": "degree",
    "stalt@hdr": "m",
}

VARIABLE_MAP: List[VarNoMapping] = [
    VarNoMapping(varno=39, unit="K", mapped_from=[        # 2m air temperature
        CfSemantics(standard_name="air_temperature"),
    ]),
    VarNoMapping(varno=58, unit="%", mapped_from=[        # 2m relative humidity
        CfSemantics(standard_name="relative_humidity"),
    ]),
    VarNoMapping(varno=107, unit="Pa", mapped_from=[      # station pressure
        CfSemantics(standard_name="air_pressure"),
        CfSemantics(standard_name="surface_air_pressure"),
    ]),
    VarNoMapping(varno=111, unit="degree", mapped_from=[  # wind direction
        CfSemantics(standard_name="wind_from_direction"),
    ]),
    VarNoMapping(varno=112, unit="m s-1", mapped_from=[   # wind speed
        CfSemantics(standard_name="wind_speed"),
    ]),
    VarNoMapping(varno=261, unit="m s-1", mapped_from=[   # wind gust speed
        CfSemantics(standard_name="wind_speed_of_gust"),
    ]),
]
