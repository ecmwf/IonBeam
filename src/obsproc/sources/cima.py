import logging
from functools import cached_property
import itertools

import Levenshtein
import requests
import yaml

from munch import Munch

# To deal with the Open-Id/OAuth2 that the API uses
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session

from datetime import timedelta

import pandas as pd


class CIMA_API_Error(BaseException):
    pass


class CIMA_API:
    api_url = "https://webdrops.cimafoundation.org/app/"
    endpoints_url = "https://testauth.cimafoundation.org/auth/realms/webdrops/.well-known/openid-configuration"

    sensor_name_translations_EN2IT = {
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

    def __init__(
        self,
        credentials_file,
        logLevel=logging.DEBUG,
    ):
        self.logger = logging.getLogger("CIMA_API")
        self.logger.setLevel(logLevel)

        with open(credentials_file, "r") as f:
            credentials = Munch(yaml.safe_load(f)["CIMA_API"])

        # We can get a list of useful endpoints by GET'ing config_url
        self.logger.info(f"Getting list of endpoints from {self.endpoints_url}")
        self.endpoints = Munch(requests.get(self.endpoints_url).json())
        self.logger.debug(f"    Endpoints {self.endpoints}")

        # let oauth-requests handle all the Open-ID/OAuth2 authentication stuff
        self.oauth = OAuth2Session(
            client=LegacyApplicationClient(client_id=credentials.client_id),
            auto_refresh_url=self.endpoints.token_endpoint,
            auto_refresh_kwargs=dict(client_id=credentials.client_id, client_secret=credentials.client_secret),
            # token_updater=token_saver,
        )

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
        self.sensor_names = Munch(IT=self.get(self.api_url + "sensors/classes").json())

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
    def match_sensor_names(self, s):
        "Give a suggestion for what you meant when you typed TURBINATA wrong."
        translation = self.sensor_name_translations_EN2IT
        valid_keys = set(translation.keys()) | set(translation.values())
        best_match = max(valid_keys, key=lambda s2: Levenshtein.ratio(s.upper(), s2))
        return best_match

    def list_stations_by_sensor(self, name, stationgroup="ComuneLive%IChange", geowin="6,36,18.6,47.5"):
        """
        For a given sensor name, eg RAIN_GAUGE,
        Return a pandas dataframe of weather stations that report that sensor reading.
        """

        # If the given name is in English and in the translation table, translate it
        name = self.sensor_name_translations_EN2IT.get(name) or name

        if name not in self.sensor_names.IT:
            raise ValueError(
                f"sensor name '{name}' is not in the list of allowed sensor types, \
                            neither English nor Italian, closest match {self.match_sensor_names(name)}"
            )

        r = self.get(
            self.api_url + f"sensors/list/{name}",
            params=dict(stationgroup=stationgroup, geowin=geowin),
        )

        return r.json()

    def station_name_to_id(self, name):
        return name.lower().replace(" ", "_").replace("-", "_")

    @cached_property
    def stations(self):
        self.logger.debug("Retrieving the list of stations and sensors, this takes a while the first time...")
        all_stations = {}
        all_sensors = {}
        for sensor_name in self.sensor_names.IT:
            stations = self.list_stations_by_sensor(sensor_name)

            for sensor_info in stations:
                station_name = sensor_info["name"]

                # Use dict.setdefault to create an entry for this station if it doesn't already exist
                station_info = all_stations.setdefault(
                    station_name,
                    dict(
                        name=station_name,
                        lat=sensor_info["lat"],
                        lon=sensor_info["lng"],
                        sensors={},
                    ),
                )

                # Check that stations with the same name are actually in the same place
                assert sensor_info["lat"] == station_info["lat"]
                assert sensor_info["lng"] == station_info["lon"]

                sensor_record = {
                    "unit": sensor_info["mu"],
                    "id": sensor_info["id"],  # This an alphanumeric ID provided by the API for all
                    # streams of readings, i.e all unique (station_name, sensor_type) pairs
                }

                station_info["sensors"][sensor_name] = sensor_record

                all_sensors.setdefault(
                    sensor_name,
                    dict(
                        unit=sensor_info["mu"],
                        translation=self.sensor_name_translations_IT2EN[sensor_name],
                    ),
                )

        self.sensors = all_sensors

        return all_stations

    @cached_property
    def sensors(self):
        self.stations
        return self.sensors

    def _single_request_station_and_sensor(
        self, station_name, sensor_name, start_date, end_date, aggregation_time_seconds=60
    ):
        if end_date - start_date > timedelta(days=3):
            raise ValueError(
                f"Maximum time period for a single request is 72 hours, this one is {end_date - start_date}"
            )

        station_info = Munch(self.stations[station_name])
        sensor_id = station_info.sensors[sensor_name]["id"]

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

    def get_data_by_station_and_sensor(self, station_name, sensor_name, start_date, end_date, aggregation_time_seconds):
        "Iterate over the date range in increments of 3 days"
        station_info = Munch(self.stations[station_name])

        if aggregation_time_seconds < 60:
            raise ValueError("Minimum aggregation time is 60 seconds")

        sensor_name = self.sensor_name_translations_EN2IT.get(sensor_name) or sensor_name
        if sensor_name not in self.sensors:
            raise KeyError(f"Sensor name {sensor_name} not valid, closest match {self.match_sensor_names(sensor_name)}")
        if sensor_name not in station_info.sensors:
            raise KeyError("Station does not support this sensor")

        sensor_unit = station_info.sensors[sensor_name]["unit"]

        dates = pd.date_range(start=start_date, end=end_date, freq="3D")
        if dates[-1] != end_date:
            dates = dates.union(
                [
                    end_date,
                ]
            )
        logging.debug(f"This date range will require {len(dates) - 1} API requests")

        # might be better to use a pre-allocated numpy array here
        columns = {
            sensor_name: [],
            "time_index": [],
        }

        date_ranges = list(zip(dates[:-1], dates[1:]))

        for s, e in date_ranges:
            json_data = self._single_request_station_and_sensor(
                station_name,
                sensor_name,
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

    def get_data_by_station(self, station_name, start_date, end_date, aggregation_time_seconds):
        "Iterate over the date range in increments of 3 days"
        station_info = Munch(self.stations[station_name])

        if aggregation_time_seconds < 60:
            raise ValueError("Minimum aggregation time is 60 seconds")

        dates = pd.date_range(start=start_date, end=end_date, freq="3D")
        if dates[-1] != end_date:
            dates = dates.union(
                [
                    end_date,
                ]
            )
        logging.debug(f"This date range will require {len(dates) - 1} API requests")

        # might be better to use a pre-allocated numpy array here
        columns = {sensor_name: [] for sensor_name in station_info.sensors}
        columns["time_index"] = []

        for s, e in list(zip(dates[:-1], dates[1:])):
            self.oauth.token = self.oauth.refresh_token(self.endpoints.token_endpoint)

            time_points = None
            for sensor_name, sensor_info in station_info.sensors.items():
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
                f"{sensor_name} [{sensor_info['unit']}]": columns[sensor_name]
                for sensor_name, sensor_info in station_info.sensors.items()
            },
            index=pd.to_datetime(columns["time_index"], format="%Y%m%d%H%M"),
        )
        return df
