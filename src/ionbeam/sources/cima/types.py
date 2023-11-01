# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

from typing import Literal, Dict, NewType, NamedTuple
import dataclasses

SensorNameIT = Literal[
    "UNKNOWN",
    "PLUVIOMETRO",
    "IDROMETRO",
    "IDROMETRO_PORTATA",
    "IDROMETRO_VELOCITA",
    "NIVOMETRO",
    "TERMOMETRO",
    "IGROMETRO",
    "DIREZIONEVENTO",
    "ANEMOMETRO",
    "BAROMETRO",
    "RADIOMETRO",
    "UNKNOWN_TYPE",
    "INVASO",
    "TURBINATA",
    "PLUVIOMETRO_OTT",
    "PORTATA_PRESA",
    "PORTATA_ENTRANTE_STIMATA",
    "BATTERIA",
    "OZONO",
    "PM10",
    "IGROMETRO_SUOLO",
    "IGROMETRO_SUOLO_10",
    "IGROMETRO_SUOLO_20",
    "IGROMETRO_SUOLO_40",
    "CO",
    "NO2",
    "C6H6",
    "TERMOMETRO_INTERNA",
    "TEMPERATURA_OZONO",
    "DIREZIONEVENTO_RAFFICA",
    "ANEMOMETRO_RAFFICA",
    "TERMOMETRO_MIN",
    "TERMOMETRO_MAX",
    "FUEL_TEMPERATURE",
    "FUEL_MOISTURE",
    "SOIL_TEMPERATURE",
    "IONIZING_RADIATION",
    "SIGNAL_STRENGTH",
    "NO",
]
SensorNameEN = Literal[
    "UNKNOWN",
    "RAIN_GAUGE",
    "HYDROMETER",
    "HYDROMETER_FLOW",
    "HYDROMETER_SPEED",
    "NIVOMETER",
    "TEMPERATURE",
    "HYGROMETER",
    "WIND_DIRECTION",
    "ANEMOMETER",
    "BAROMETER",
    "RADIOMETER",
    "UNKNOWN_TYPE",
    "INVERSION_HEIGHT",
    "TURBINATED",
    "RAIN GAUGE_OCT",
    "COURSE_SOCKET",
    "ESTIMATED_IN_FLOW",
    "BATTERY_LEVEL",
    "OZONE",
    "PM10",
    "SOIL_HYGROMETER",
    "SOIL_HYGROMETER_10",
    "SOIL_HYGROMETER_20",
    "SOIL_HYGROMETER_40",
    "CO",
    "NO2",
    "C6H6",
    "INDOOR_THERMOMETER",
    "OZONE_TEMPERATURE",
    "DIRECTION_WIND_GUST",
    "ANEMOMETER_GUST",
    "THERMOMETER_MIN",
    "THERMOMETER_MAX",
    "FUEL_TEMPERATURE",
    "FUEL_MOISTURE",
    "SOIL_TEMPERATURE",
    "IONIZING_RADIATION",
    "SIGNAL_STRENGTH",
    "NO",
]
SensorName = SensorNameEN | SensorNameIT


sensor_name_translations_EN2IT: Dict[SensorNameEN, SensorNameIT] = {
    "UNKNOWN": "UNKNOWN",
    "RAIN_GAUGE": "PLUVIOMETRO",
    "HYDROMETER": "IDROMETRO",
    "HYDROMETER_FLOW": "IDROMETRO_PORTATA",
    "HYDROMETER_SPEED": "IDROMETRO_VELOCITA",
    "NIVOMETER": "NIVOMETRO",
    "TEMPERATURE": "TERMOMETRO",
    "HYGROMETER": "IGROMETRO",
    "WIND_DIRECTION": "DIREZIONEVENTO",
    "ANEMOMETER": "ANEMOMETRO",
    "BAROMETER": "BAROMETRO",
    "RADIOMETER": "RADIOMETRO",
    "UNKNOWN_TYPE": "UNKNOWN_TYPE",
    "INVERSION_HEIGHT": "INVASO",
    "TURBINATED": "TURBINATA",
    "RAIN GAUGE_OCT": "PLUVIOMETRO_OTT",
    "COURSE_SOCKET": "PORTATA_PRESA",
    "ESTIMATED_IN_FLOW": "PORTATA_ENTRANTE_STIMATA",
    "BATTERY_LEVEL": "BATTERIA",
    "OZONE": "OZONO",
    "PM10": "PM10",
    "SOIL_HYGROMETER": "IGROMETRO_SUOLO",
    "SOIL_HYGROMETER_10": "IGROMETRO_SUOLO_10",
    "SOIL_HYGROMETER_20": "IGROMETRO_SUOLO_20",
    "SOIL_HYGROMETER_40": "IGROMETRO_SUOLO_40",
    "CO": "CO",
    "NO2": "NO2",
    "C6H6": "C6H6",
    "INDOOR_THERMOMETER": "TERMOMETRO_INTERNA",
    "OZONE_TEMPERATURE": "TEMPERATURA_OZONO",
    "DIRECTION_WIND_GUST": "DIREZIONEVENTO_RAFFICA",
    "ANEMOMETER_GUST": "ANEMOMETRO_RAFFICA",
    "THERMOMETER_MIN": "TERMOMETRO_MIN",
    "THERMOMETER_MAX": "TERMOMETRO_MAX",
    "FUEL_TEMPERATURE": "FUEL_TEMPERATURE",
    "FUEL_MOISTURE": "FUEL_MOISTURE",
    "SOIL_TEMPERATURE": "SOIL_TEMPERATURE",
    "IONIZING_RADIATION": "IONIZING_RADIATION",
    "SIGNAL_STRENGTH": "SIGNAL_STRENGTH",
    "NO": "NO",
}
sensor_name_translations_IT2EN: Dict[SensorNameIT, SensorNameEN] = {
    v: k for k, v in sensor_name_translations_EN2IT.items()
}


# API Related types and classes
class CIMA_API_Error(BaseException):
    pass


URL = NewType("URL", str)


@dataclasses.dataclass
class Endpoints:
    token_endpoint: URL


@dataclasses.dataclass
class Credentials:
    username: str
    password: str
    client_id: str
    client_secret: str


StationName = NewType("StationName", str)
SensorID = NewType("SensorID", str)


@dataclasses.dataclass
class GenericSensor:
    "A representation of a generic class of sensors"
    unit: str
    name: SensorNameIT
    translation: SensorNameEN


@dataclasses.dataclass
class APISensor:
    "A sensor as represented by the CIMA API"
    station_name: StationName
    lat: float
    lon: float
    unit: str
    id: SensorID  # This an alphanumeric ID provided by the API for all
    # streams of readings, i.e all unique (station_name, sensor_type) pairs


@dataclasses.dataclass
class UniqueSensor:
    "We attach sensors to stations so it's unnecessary to store the location here"
    unit: str
    id: SensorID


@dataclasses.dataclass
class Station:
    name: StationName
    id: str
    lat: float
    lon: float
    sensors: Dict[SensorNameIT, UniqueSensor]


# This is a NamedTuple rather than a dataclass because it's annoying to make the latter iterable
class GeoBBox(NamedTuple):
    "Represents a rectangle in Lat/Long space"
    minLat: float
    minLon: float
    maxLat: float
    maxLon: float
