# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #
import unicodedata as un

unit_conversions = {
    "°C -> K": lambda t: t + 273.15,
    "ºC -> K": lambda t: t + 273.15, # Yes, this degree symbol looks weird, this is actually the unicode character known as "MASCULINE ORDINAL INDICATOR"
    "dB -> dBA": lambda x: x, # This may or may not be wrong, depends on whether there are data sources reporting noise that is genuinely in dB rather than dBA
    "Degrees -> °": lambda d: d,
    "m/s -> m.s-1": lambda s: s,
    "km/h -> m/s": lambda s: s * 0.277778,
    "mbar -> hPa": lambda p: p,
    "mbar -> Pa": lambda p: p * 100,
    "kPa -> Pa": lambda p: p * 1000,
    "hPa -> Pa": lambda p: p * 100,
    "°C/100m -> K/100m": lambda x: x,
    "#/0.1l -> #/cm3" : lambda x: x / 100,
    "#/0.1L -> #/cm3" : lambda x: x / 100,
    "ug/m3 -> μg/m3" : lambda x: x,
    "ppb -> ppm" : lambda x: x * 1000,
}

unit_conversions = {un.normalize("NFKD", k): v for k, v in unit_conversions.items()}
