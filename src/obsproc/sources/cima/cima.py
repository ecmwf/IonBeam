import logging
from functools import cached_property
import itertools

import Levenshtein
import requests
import yaml

from munch import Munch

from typing import NewType, Dict, List

# To deal with the Open-Id/OAuth2 that the API uses
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session  # type : ignore

from datetime import timedelta, datetime

import pandas as pd
import numpy as np

import dataclasses


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


# Types and Classes relating to the data we're using
SensorNameEN = NewType("SensorNameEN", str)
SensorNameIT = NewType("SensorNameIT", str)
SensorName = SensorNameEN | SensorNameIT

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
    lat: float
    lon: float
    sensors: Dict[SensorNameIT, UniqueSensor]


class CIMA_API:
    api_url = "https://webdrops.cimafoundation.org/app/"
    endpoints_url = "https://testauth.cimafoundation.org/auth/realms/webdrops/.well-known/openid-configuration"

    sensor_name_translations_EN2IT: Dict[SensorNameEN, SensorNameIT] = {
        SensorNameEN("UNKNOWN"): SensorNameIT("UNKNOWN"),
        SensorNameEN("RAIN_GAUGE"): SensorNameIT("PLUVIOMETRO"),
        SensorNameEN("HYDROMETER"): SensorNameIT("IDROMETRO"),
        SensorNameEN("HYDROMETER_FLOW"): SensorNameIT("IDROMETRO_PORTATA"),
        SensorNameEN("HYDROMETER_SPEED"): SensorNameIT("IDROMETRO_VELOCITA"),
        SensorNameEN("NIVOMETER"): SensorNameIT("NIVOMETRO"),
        SensorNameEN("TEMPERATURE"): SensorNameIT("TERMOMETRO"),
        SensorNameEN("HYGROMETER"): SensorNameIT("IGROMETRO"),
        SensorNameEN("WIND_DIRECTION"): SensorNameIT("DIREZIONEVENTO"),
        SensorNameEN("ANEMOMETER"): SensorNameIT("ANEMOMETRO"),
        SensorNameEN("BAROMETER"): SensorNameIT("BAROMETRO"),
        SensorNameEN("RADIOMETER"): SensorNameIT("RADIOMETRO"),
        SensorNameEN("UNKNOWN_TYPE"): SensorNameIT("UNKNOWN_TYPE"),
        SensorNameEN("INVERSION_HEIGHT"): SensorNameIT("INVASO"),
        SensorNameEN("TURBINATED"): SensorNameIT("TURBINATA"),
        SensorNameEN("RAIN GAUGE_OCT"): SensorNameIT("PLUVIOMETRO_OTT"),
        SensorNameEN("COURSE_SOCKET"): SensorNameIT("PORTATA_PRESA"),
        SensorNameEN("ESTIMATED_IN_FLOW"): SensorNameIT("PORTATA_ENTRANTE_STIMATA"),
        SensorNameEN("BATTERY_LEVEL"): SensorNameIT("BATTERIA"),
        SensorNameEN("OZONE"): SensorNameIT("OZONO"),
        SensorNameEN("PM10"): SensorNameIT("PM10"),
        SensorNameEN("SOIL_HYGROMETER"): SensorNameIT("IGROMETRO_SUOLO"),
        SensorNameEN("SOIL_HYGROMETER_10"): SensorNameIT("IGROMETRO_SUOLO_10"),
        SensorNameEN("SOIL_HYGROMETER_20"): SensorNameIT("IGROMETRO_SUOLO_20"),
        SensorNameEN("SOIL_HYGROMETER_40"): SensorNameIT("IGROMETRO_SUOLO_40"),
        SensorNameEN("CO"): SensorNameIT("CO"),
        SensorNameEN("NO2"): SensorNameIT("NO2"),
        SensorNameEN("C6H6"): SensorNameIT("C6H6"),
        SensorNameEN("INDOOR_THERMOMETER"): SensorNameIT("TERMOMETRO_INTERNA"),
        SensorNameEN("OZONE_TEMPERATURE"): SensorNameIT("TEMPERATURA_OZONO"),
        SensorNameEN("DIRECTION_WIND_GUST"): SensorNameIT("DIREZIONEVENTO_RAFFICA"),
        SensorNameEN("ANEMOMETER_GUST"): SensorNameIT("ANEMOMETRO_RAFFICA"),
        SensorNameEN("THERMOMETER_MIN"): SensorNameIT("TERMOMETRO_MIN"),
        SensorNameEN("THERMOMETER_MAX"): SensorNameIT("TERMOMETRO_MAX"),
        SensorNameEN("FUEL_TEMPERATURE"): SensorNameIT("FUEL_TEMPERATURE"),
        SensorNameEN("FUEL_MOISTURE"): SensorNameIT("FUEL_MOISTURE"),
        SensorNameEN("SOIL_TEMPERATURE"): SensorNameIT("SOIL_TEMPERATURE"),
        SensorNameEN("IONIZING_RADIATION"): SensorNameIT("IONIZING_RADIATION"),
        SensorNameEN("SIGNAL_STRENGTH"): SensorNameIT("SIGNAL_STRENGTH"),
        SensorNameEN("NO"): SensorNameIT("NO"),
    }

    def __init__(
        self,
        credentials_file: str,
        logLevel: int = logging.DEBUG,
    ):
        self.logger = logging.getLogger("CIMA_API")
        self.logger.setLevel(logLevel)

        with open(credentials_file, "r") as f:
            parsed_yaml = yaml.safe_load(f)
            secrets_yaml_headers = parsed_yaml["headers"]
            credentials = Credentials(**parsed_yaml["CIMA_API"])

        # We can get a list of useful endpoints by GET'ing config_url
        self.logger.info(f"Getting list of endpoints from {self.endpoints_url}")
        self.endpoints = Endpoints(requests.get(self.endpoints_url).json()["token_endpoint"])
        self.logger.debug(f"    Endpoints {self.endpoints}")

        # let oauth-requests handle all the Open-ID/OAuth2 authentication stuff
        self.oauth = OAuth2Session(
            client=LegacyApplicationClient(client_id=credentials.client_id),
            auto_refresh_url=self.endpoints.token_endpoint,
            auto_refresh_kwargs=dict(client_id=credentials.client_id, client_secret=credentials.client_secret),
        )

        # OAuth2Session is a subclass of requests.Session so see that documentation for generic usage
        # Update the headers for all our requests so that
        self.oauth.headers.update(secrets_yaml_headers)

        # Tell oauth-requests to grab a token using username/password credentials
        # Usually this step would open a browser window where you would manually log in
        # But for this particular API they're just eneabled straight user/pass authentication
        # hence the use of 'LegacyApplicationClient' above
        self.logger.info("Fetching the token...")
        self.oauth.fetch_token(
            token_url=self.endpoints.token_endpoint,
            username=credentials.username,
            password=credentials.password,
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
        )
        self.logger.debug(f"Token: {self.oauth.token}")

        # Grab a list of sensors names in italian
        self.sensor_names: Munch[str, str] = Munch(IT=self.get(self.api_url + "sensors/classes").json())

        # Setup a two way translation table so you can also specify the names in English
        self.sensor_name_translations_IT2EN = {v: k for k, v in self.sensor_name_translations_EN2IT.items()}
        if set(self.sensor_names.IT) != set(self.sensor_name_translations_IT2EN.keys()):
            self.logger.warning("The translation tables need updating!")

    def refresh_token(self):
        "Refresh the OAuth2 token, tokens generally expire after 30 minutes for this API"
        self.logger.info("Refreshing the token...")
        self.oauth.refresh_token(self.endpoints.token_endpoint)
        self.logger.debug(f"Token: {self.oauth.token}")

    def get(self, *args, **kwargs):
        "Wrap the get command of the underlying oauth object"
        r = self.oauth.get(*args, **kwargs)
        if r.headers["Content-Type"] != "application/json":
            self.logger.info(f"Failed request ({r.status_code}) to {r.url}")
            self.logger.debug(f"Response: {r.text}")
            raise CIMA_API_Error(f"Request failed, code {r.status_code}")
        return r

    @classmethod
    def match_sensor_names(self, s: SensorName) -> SensorName:
        "Give a suggestion for what you meant when you typed TURBINATA wrong."
        translation = self.sensor_name_translations_EN2IT
        valid_keys = set(translation.keys()) | set(translation.values())
        best_match = max(valid_keys, key=lambda s2: Levenshtein.ratio(s.upper(), s2))
        return best_match

    def list_stations_by_sensor(
        self, name: SensorName, stationgroup="ComuneLive%IChange", geowin="6,36,18.6,47.5"
    ) -> List[APISensor]:
        """
        For a given sensor name, eg RAIN_GAUGE,
        Return a list of stations reporting that sensor
        """

        # If the given name is in English and in the translation table, translate it
        nameIT: SensorNameIT = self.sensor_name_translations_EN2IT.get(name) or name  # type: ignore

        if nameIT not in self.sensor_names.IT:
            raise ValueError(
                f"sensor name '{nameIT}' is not in the list of allowed sensor types, \
                            neither English nor Italian, closest match {self.match_sensor_names(nameIT)}"
            )

        r = self.get(
            self.api_url + f"sensors/list/{nameIT}",
            params=dict(stationgroup=stationgroup, geowin=geowin),
        )

        return [
            APISensor(station_name=x["name"], lat=x["lat"], lon=x["lng"], unit=x["mu"], id=x["id"]) for x in r.json()
        ]

    def station_name_to_id(self, name: StationName):
        return name.lower().replace(" ", "_").replace("-", "_")

    @cached_property
    def stations(self) -> Dict[StationName, Station]:
        self.logger.debug("Retrieving the list of stations and sensors, this takes a while the first time...")
        all_stations: Dict[StationName, Station] = {}
        all_sensors: Dict[SensorName, GenericSensor] = {}
        for sensor_name in self.sensor_names.IT:
            stations = self.list_stations_by_sensor(sensor_name)

            for sensor_info in stations:
                # Use dict.setdefault to create an entry for this station if it doesn't already exist
                station_info = all_stations.setdefault(
                    sensor_info.station_name,
                    Station(
                        name=sensor_info.station_name,
                        lat=sensor_info.lat,
                        lon=sensor_info.lon,
                        sensors={},
                    ),
                )

                # Check that stations with the same name are actually in the same place
                assert sensor_info.lat == station_info.lat
                assert sensor_info.lon == station_info.lon

                # Attach a record of this sensor to the corresponding Station object
                station_info.sensors[sensor_name] = UniqueSensor(
                    unit=sensor_info.unit,
                    id=sensor_info.id,
                )

                # Create a generic entry for this sensor if we haven't already got one
                all_sensors.setdefault(
                    sensor_name,
                    GenericSensor(
                        name=sensor_name,
                        unit=sensor_info.unit,
                        translation=self.sensor_name_translations_IT2EN[sensor_name],
                    ),
                )

        self.sensors = all_sensors

        return all_stations

    @cached_property
    def sensors(self) -> Dict[SensorName, GenericSensor]:
        self.stations
        return self.sensors

    def _single_request_station_and_sensor(
        self,
        station_name: StationName,
        sensor_name: SensorNameIT,
        start_date: datetime,
        end_date: datetime,
        aggregation_time_seconds: int = 60,
    ) -> dict:
        if end_date - start_date > timedelta(days=3):
            raise ValueError(
                f"Maximum time period for a single request is 72 hours, this one is {end_date - start_date}"
            )

        station_info = self.stations[station_name]
        sensor_id = station_info.sensors[sensor_name].id

        r = self.get(
            self.api_url + f"sensors/data/{sensor_name}/{sensor_id}",
            params={
                "from": start_date.strftime("%Y%m%d%H%M"),
                "to": end_date.strftime("%Y%m%d%H%M"),
                "aggr": aggregation_time_seconds,
                "date_as_string": True,
            },
        )

        j = r.json()[0]
        return j

    def translate_sensor_name(self, name: SensorName) -> SensorNameIT:
        return self.sensor_name_translations_EN2IT.get(name) or name  # type: ignore

    def get_data_by_station_and_sensor(
        self,
        station_name: StationName,
        sensor_name: SensorName,
        start_date: datetime,
        end_date: datetime,
        aggregation_time_seconds: int = 60,
    ) -> pd.DataFrame:
        "Iterate over the date range in increments of 3 days"
        station_info = self.stations[station_name]

        if aggregation_time_seconds < 60:
            raise ValueError("Minimum aggregation time is 60 seconds")

        sensor_name_IT = self.translate_sensor_name(sensor_name)

        if sensor_name_IT not in self.sensors:
            raise KeyError(
                f"Sensor name {sensor_name_IT} not valid, closest match {self.match_sensor_names(sensor_name_IT)}"
            )
        if sensor_name_IT not in station_info.sensors:
            raise KeyError("Station does not support this sensor")

        sensor_unit = station_info.sensors[sensor_name_IT].unit

        dates = pd.date_range(start=start_date, end=end_date, freq="3D")
        if dates[-1] != end_date:
            dates = pd.DatetimeIndex(
                dates.union(
                    [
                        end_date,
                    ]
                )
            )
        logging.debug(f"This date range will require {len(dates) - 1} API requests")

        # might be better to use a pre-allocated numpy array here
        columns: Dict[SensorName | str, List] = {
            sensor_name: [],
            "time_index": [],
        }

        date_ranges = list(zip(dates[:-1], dates[1:]))

        for s, e in date_ranges:
            json_data = self._single_request_station_and_sensor(
                station_name,
                sensor_name_IT,
                start_date=s,
                end_date=e,
                aggregation_time_seconds=aggregation_time_seconds,
            )
            columns[sensor_name].append(json_data["values"])
            columns["time_index"].append(json_data["timeline"])

        # flatten the lists of lists
        for key in columns:
            columns[key] = list(itertools.chain.from_iterable(columns[key]))

        df = pd.DataFrame(
            {
                f"{sensor_name} [{sensor_unit}]": columns[sensor_name],
            },
            index=pd.to_datetime(columns["time_index"], format="%Y%m%d%H%M"),
        )
        return df

    def get_data_by_station(
        self, station_name: StationName, start_date: datetime, end_date: datetime, aggregation_time_seconds: int = 60
    ) -> pd.DataFrame:
        "Iterate over the date range in increments of 3 days"
        station_info = self.stations[station_name]

        if aggregation_time_seconds < 60:
            raise ValueError("Minimum aggregation time is 60 seconds")

        dates = pd.date_range(start=start_date, end=end_date, freq="3D")
        if dates[-1] != end_date:
            dates = pd.DatetimeIndex(
                dates.union(
                    [
                        end_date,
                    ]
                )
            )
        logging.debug(f"This date range will require {len(dates) - 1} API requests")

        # might be better to use a pre-allocated numpy array here
        columns: Dict[SensorName | str, List] = {sensor_name: [] for sensor_name in station_info.sensors}
        columns["time_index"] = []

        for s, e in list(zip(dates[:-1], dates[1:])):
            self.oauth.token = self.oauth.refresh_token(self.endpoints.token_endpoint)

            time_points = None
            for sensor_name in station_info.sensors.keys():
                json_data = self._single_request_station_and_sensor(
                    station_name,
                    sensor_name,
                    start_date=s,
                    end_date=e,
                    aggregation_time_seconds=aggregation_time_seconds,
                )
                columns[sensor_name].append(json_data["values"])

                # Check that the sensor readings were really taken at the same times
                if time_points is not None:
                    assert json_data["timeline"] == time_points
                else:
                    time_points = json_data["timeline"]

            columns["time_index"].append(time_points)

        # flatten the lists of lists
        for key in columns:
            columns[key] = list(itertools.chain.from_iterable(columns[key]))

        df = pd.DataFrame(
            {
                f"{sensor_name} [{sensor_info.unit}]": columns[sensor_name]
                for sensor_name, sensor_info in station_info.sensors.items()
            },
            index=pd.to_datetime(columns["time_index"], format="%Y%m%d%H%M"),
        )
        df.replace(to_replace=-9998.0, value=np.nan, inplace=True)
        return df
