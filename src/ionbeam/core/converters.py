# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

unit_conversions = {
    "°C -> K": lambda t: t + 273.15,
    "Degrees -> °": lambda d: d,
    "m/s -> m.s-1": lambda s: s,
    "km/h -> m/s": lambda s: s * 0.277778,
    "mbar -> hPa": lambda p: p,
    "mbar -> Pa": lambda p: p * 100,
    "hPa -> Pa": lambda p: p * 100,
    "°C/100m -> K/100m": lambda x: x,
}
