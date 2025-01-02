#
# (C) Copyright 2023 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#
import unicodedata as un

# These get passed the entire pandas column as a numpy array so they're pretty fast
unit_conversions = {
    "°C -> K": lambda t: t + 273.15,
    "ºC -> K": lambda t: t + 273.15, # Yes, this degree symbol looks weird, this is actually the unicode character known as "MASCULINE ORDINAL INDICATOR"
    "dB -> dBA": None, # This may or may not be wrong, depends on whether there are data sources reporting noise that is genuinely in dB rather than dBA
    "Degrees -> °": None,
    "m/s -> m/s": None,
    "km/h -> m/s": lambda s: s * 0.277778,
    'knots -> m/s': lambda s: s * 0.514444,
    "mbar -> hPa": None,
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