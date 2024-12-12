# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

"""


TODO: Currently the geowindow and stationgroup used are hardcoded, that might need to be fixed at some point
"""

import itertools
import logging
import pickle
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from functools import cached_property, reduce
from pathlib import Path
from typing import Dict, List, Tuple

import Levenshtein
import numpy as np
import pandas as pd
import requests
from munch import Munch

# To deal with the Open-Id/OAuth2 that the API uses
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session  # type: ignore[import]

from .types import (
    APISensor,
    CIMA_API_Error,
    Credentials,
    Endpoints,
    GenericSensor,
    GeoBBox,
    SensorName,
    SensorNameIT,
    Station,
    StationName,
    UniqueSensor,
    sensor_name_translations_EN2IT,
    sensor_name_translations_IT2EN,
)

logger = logging.getLogger(__name__)

for package in ["requests_oauthlib", "urllib3"]:
    package_logger = logging.getLogger(package)
    package_logger.setLevel(logging.WARNING)


class CIMA_API:
    api_url = "https://webdrops.cimafoundation.org/app/"
    endpoints_url = "https://testauth.cimafoundation.org/auth/realms/webdrops/.well-known/openid-configuration"

    def __init__(
        self,
        credentials: dict[str, str],
        logLevel: int | None = None,
        cache_file: Path | None = None,
        headers: dict[str, str] = {},
    ):
        self.logger = logging.getLogger(__name__)

        assert cache_file is not None
        self.cache_file = Path(cache_file)
        logger.info(f"Looking for cache file at {cache_file}")
        try:
            with cache_file.open("rb") as f:
                self.cache = pickle.load(f)
                logger.info(f"Loaded cache file {cache_file}")
                if datetime.now(tz=timezone.utc) - self.cache["date"] > timedelta(days=1):
                    logger.info("Cache older than 1 day, refreshing")
                    self.cache = {}
        except (OSError, KeyError, AttributeError):
            self.cache = {}


        self.credentials = Credentials(**credentials)

        # We can get a list of useful endpoints by GET'ing config_url
        if "endpoints" not in self.cache:
            self.logger.info(f"Getting list of endpoints from {self.endpoints_url}")
            self.endpoints = Endpoints(requests.get(self.endpoints_url).json()["token_endpoint"])
            self.logger.debug(f"    Endpoints {self.endpoints}")
            self.cache["endpoints"] = self.endpoints
        else:
            self.endpoints = self.cache["endpoints"]

        # let oauth-requests handle all the Open-ID/OAuth2 authentication stuff
        self.oauth = OAuth2Session(
            client=LegacyApplicationClient(client_id=self.credentials.client_id),
            auto_refresh_url=self.endpoints.token_endpoint,
            auto_refresh_kwargs=dict(
                client_id=self.credentials.client_id, client_secret=self.credentials.client_secret
            ),
        )

        # OAuth2Session is a subclass of requests.Session so see that documentation for generic usage
        # Update the headers for all our requests so that
        self.oauth.headers.update(headers)

        # self.logger.debug(f"Token: {self.oauth.token}")

        # Grab a list of sensors names in italian
        if "sensor_names" not in self.cache:
            self.sensor_names: Munch[str, str] = Munch(IT=self.get(self.api_url + "sensors/classes").json())
            self.cache["sensor_names"] = self.sensor_names
        else:
            self.sensor_names = self.cache["sensor_names"]

        # Setup a two way translation table so you can also specify the names in English
        self.sensor_name_translations_IT2EN = {v: k for k, v in sensor_name_translations_EN2IT.items()}
        if set(self.sensor_names.IT) != set(self.sensor_name_translations_IT2EN.keys()):
            self.logger.warning("The translation tables need updating!")

    def refresh_token(self) -> None:
        "Refresh the OAuth2 token, tokens generally expire after 30 minutes for this API"
        self.logger.info("Refreshing the token...")
        self.oauth.refresh_token(self.endpoints.token_endpoint)
        self.logger.debug(f"Token: {self.oauth.token}")

    def authenticate(self) -> None:
        self.logger.info("Fetching the token...")
        self.logger.info(f"{self.endpoints.token_endpoint=}")
        self.oauth.fetch_token(
            token_url=self.endpoints.token_endpoint,
            username=self.credentials.username,
            password=self.credentials.password,
            client_id=self.credentials.client_id,
            client_secret=self.credentials.client_secret,
        )

    def get(self, *args, **kwargs) -> requests.Response:
        "Wrap the get command of the underlying oauth object"
        # Tell oauth-requests to grab a token using username/password credentials
        # Usually this step would open a browser window where you would manually log in
        # But for this particular API they're just eneabled straight user/pass authentication
        # hence the use of 'LegacyApplicationClient' above
        if not self.oauth.token:
            self.authenticate()

        r = self.oauth.get(*args, **kwargs)
        if r.headers["Content-Type"] != "application/json":
            self.logger.info(f"Failed request ({r.status_code}) to {r.url}")
            self.logger.debug(f"Response: {r.text}")
            raise CIMA_API_Error(f"Request failed, code {r.status_code}")
        return r

    @classmethod
    def match_sensor_names(cls, s: SensorName) -> SensorName:
        "Give a suggestion for what you meant when you typed TURBINATA wrong."
        translation = sensor_name_translations_EN2IT
        valid_keys = set(translation.keys()) | set(translation.values())
        best_match = max(valid_keys, key=lambda s2: Levenshtein.ratio(s.upper(), s2))
        return best_match

    def list_stations_by_sensor(
        self,
        name: SensorName,
        stationgroup="ComuneLive%IChange",
        geobbox: GeoBBox | Tuple[float, float, float, float] = GeoBBox(6, 36, 18.6, 47.5),
    ) -> List[APISensor]:
        """
        For a given sensor name, eg RAIN_GAUGE,
        Return a list of stations reporting that sensor
        """

        # If the given name is in English and in the translation table, translate it
        nameIT: SensorNameIT = sensor_name_translations_EN2IT.get(name) or name  # type: ignore

        if nameIT not in self.sensor_names.IT:
            raise ValueError(
                f"sensor name '{nameIT}' is not in the list of allowed sensor types, \
                            neither English nor Italian, closest match {self.match_sensor_names(nameIT)}"
            )

        r = self.get(
            self.api_url + f"sensors/list/{nameIT}",
            params=dict(stationgroup=stationgroup, geowin=",".join(str(s) for s in geobbox)),
        )

        return [
            APISensor(station_name=x["name"], lat=x["lat"], lon=x["lng"], unit=x["mu"], id=x["id"]) for x in r.json()
        ]

    def station_name_to_id(self, name: StationName):
        return name.lower().replace(" ", "_").replace("-", "_")

    @cached_property
    def stations(self) -> Dict[StationName, Station]:
        if "stations" in self.cache:
            self.sensors = self.cache["sensors"]
            return self.cache["stations"]

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
                        id=self.station_name_to_id(sensor_info.station_name),
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
                        translation=sensor_name_translations_IT2EN[sensor_name],
                    ),
                )

        self.sensors = all_sensors
        self.cache["stations"] = all_stations
        self.cache["sensors"] = self.sensors

        if self.cache_file is not None:
            logger.info("Writing out cache to file.")
            if not self.cache_file.parent.exists():
                self.cache_file.parent.mkdir(parents=True)
            with self.cache_file.open("wb") as f:
                self.cache["date"] = datetime.now(tz=timezone.utc)
                pickle.dump(self.cache, f)

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
        return sensor_name_translations_EN2IT.get(name) or name  # type: ignore

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

        # might be better to use a pre-allocated numpy array here
        columns: Dict[SensorName | str, List[List[float]]] = {
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
        flattened_columns: Dict[SensorName | str, List[float]] = {}
        for key in columns:
            flattened_columns[key] = list(itertools.chain.from_iterable(columns[key]))

        df = pd.DataFrame(
            {
                f"{sensor_name} [{sensor_unit}]": flattened_columns[sensor_name],
            },
            index=pd.to_datetime(flattened_columns["time_index"], format="%Y%m%d%H%M", utc=True),
        )
        return df

    def get_data_by_station(
        self,
        station_name: StationName,
        start_date: datetime,
        end_date: datetime,
        aggregation_time_seconds: int = 60,
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

        if not self.oauth.token:
            self.authenticate()


        # might be better to use a pre-allocated numpy array here
        columns : defaultdict[SensorName, dict] = defaultdict(lambda : dict(sensor = None, times = [], readings = []))

        for s, e in list(zip(dates[:-1], dates[1:])):
            self.oauth.token = self.oauth.refresh_token(self.endpoints.token_endpoint)

            for sensor_name, sensor in station_info.sensors.items():
                try:
                    json_data = self._single_request_station_and_sensor(
                        station_name,
                        sensor_name,
                        start_date=s,
                        end_date=e,
                        aggregation_time_seconds=aggregation_time_seconds,
                    )
                except CIMA_API_Error as e:
                    logger.warning(f"Failed to get data for {station_name} {sensor_name} {s} {e}")
                    json_data = {"timeline": False}
                    continue

                # Skip any sensors that didn't return any data
                if json_data["timeline"]:
                    columns[sensor_name]['sensor'] = sensor
                    columns[sensor_name]["times"].extend(json_data["timeline"])
                    columns[sensor_name]["readings"].extend(json_data["values"])

        dfs = [
            pd.DataFrame(
                        {
                            f"{sensor_name} [{details['sensor'].unit}]": details["readings"],
                            "time": pd.to_datetime(details["times"], format="%Y%m%d%H%M", utc = True),
                        })
            for sensor_name, details in columns.items()
        ]

        df = reduce(
                lambda left, right: pd.merge(left, right, on="time", how="outer"),
                dfs
            )

        df.replace(to_replace=-9998.0, value=np.nan, inplace=True)

        return df
