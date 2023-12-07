# #
# # (C) Copyright 2023 ECMWF.
# #
# # This software is licensed under the terms of the Apache Licence Version 2.0
# # which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# # In applying this licence, ECMWF does not waive the privileges and immunities
# # granted to it by virtue of its status as an intergovernmental organisation nor
# # does it submit to any jurisdiction.
# #

from typing import Literal, ClassVar, Dict, NewType, Tuple, List, Annotated

import dataclasses
import logging
import yaml

import pandas

import requests

from shapely import geometry

# To deal with the Open-Id/OAuth2 that the API uses

from pathlib import Path

from datetime import datetime, timedelta


@dataclasses.dataclass
class Credentials:
    email: str
    password: str


MT_DataType = Literal["my", "all", "shared", "preferred", "trashedSess"]
MT_Sortby = Literal[
    "startTime",
    "endTime",
    "nPoints",
    "nPhotos",
    "maxTemp",
    "minTemp",
    "avgTemp",
    "maxHum",
    "maxAlt",
    "maxPress",
    "maxTd",
    "maxHDX",
    "maxInv",
]
MT_ObservationType = Literal[
    "time", "lattitude", "longitude", "T0", "H", "P", "td", "HDX", "i", "s", "L", "CO2", "T3", "RSSI", "bt"
]


@dataclasses.dataclass
class MT_WeatherPoint:
    "Represents a single observation in a MeteoTracker session"
    time: datetime
    lat: float
    lon: float
    air_temperature: float | None = None
    relative_humidity: float | None = None
    pressure: float | None = None
    dew_point: float | None = None
    humidex: float | None = None
    vertical_temperature_gradient: float | None = None
    wind_speed: float | None = None
    solar_radiation_index: float | None = None
    CO2: float | None = None
    IR_temperature: float | None = None
    cellular_RSSI: float | None = None
    bluetooth_RSSI: float | None = None

    _API_keys: ClassVar[Dict[MT_ObservationType, str]] = {
        "time": "time",
        "lattitude": "lat",
        "longitude": "lon",
        "T0": "air_temperature",
        "H": "relative_humidity",
        "P": "pressure",
        "td": "dew_point",
        "HDX": "humidex",
        "i": "vertical_temperature_gradient",
        "s": "wind_speed",
        "L": "solar_radiation_index",
        "CO2": "CO2",
        "T3": "IR_temperature",
        "RSSI": "cellular_RSSI",
        "bt": "bluetooth_RSSI",
    }

    @classmethod
    def from_API(cls, json: dict):
        return cls(**{cls._API_keys[key]: json[key] for key in cls._API_keys if key in json})

    def to_API(self):
        return {
            api_key: getattr(self, attr_name)
            for api_key, attr_name in self._API_keys.items()
            if getattr(self, attr_name) is not None
        }


SessionId = NewType("SessionId", str)
Location = NewType("Location", str)
Username = NewType("Username", str)


@dataclasses.dataclass
class MT_Session:
    "Represents a single MeteoTracker trip"
    id: SessionId
    n_points: int
    offset_tz: str
    start_time: datetime
    author: str
    end_time: datetime | None
    columns: List[str]

    def __init__(self, **d):
        self.id = SessionId(d["_id"])
        self.n_points = int(d["nPoints"])
        self.offset_tz = d["offsetTZ"]
        self.start_time = datetime.fromisoformat(
            d["startTime"]
        )  # TODO Check whether this is really UTC or the offset needs to be incorporated
        self.end_time = datetime.fromisoformat(d["endTime"]) if "endTime" in d else None
        self.columns = [k for k in d if isinstance(d[k], dict) and "avgVal" in d[k]]  # filter for nonzero data
        self.author = d["by"]


@dataclasses.dataclass
class Endpoints:
    token_endpoint: str = "https://app.meteotracker.com/auth/login/api"
    refresh_endpoint: str = "https://app.meteotracker.com/auth/refreshtoken"


@dataclasses.dataclass
class Token:
    access_token: str
    refresh_token: str


class MeteoTracker_API_Error(BaseException):
    pass


class MeteoTracker_API:
    endpoints: Endpoints = Endpoints()
    max_queries: int = 100

    def __init__(
        self,
        credentials: dict,
        headers,
    ):
        self.credentials = Credentials(**credentials)
        self.logger = logging.getLogger("MeteoTracker_API")

        # let oauth-requests handle all the Open-ID/OAuth2 authentication stuff
        self.oauth = requests.Session()

        # OAuth2Session is a subclass of requests.Session so see that documentation for generic usage
        # Update the headers for all our requests so that people know who's crawling them
        self.oauth.headers.update(headers)
        self.token = None

    def auth(self):
        resp = self.oauth.post(
            self.endpoints.token_endpoint, json=dict(email=self.credentials.email, password=self.credentials.password)
        )

        self.token = Token(resp.json()["accessToken"], resp.json()["refreshToken"])
        self.oauth.headers.update({"Authorization": f"Bearer {self.token.access_token}"})

    def get(self, *args, **kwargs) -> requests.Response:
        "Wrap the get command of the underlying oauth object"
        if not self.token:
            for i in range(3):
                try:
                    self.auth()
                except KeyError:
                    pass
            if not self.token:
                raise ValueError("Could not authenticate MeteoTracker after 3 tries")
        r = self.oauth.get(*args, **kwargs)
        try:
            r.json()
        except requests.JSONDecodeError:
            self.logger.info(f"Failed request ({r.status_code}) to {r.url}")
            self.logger.debug(f"Response: {r.text}")

            if r.status_code == 401:
                raise MeteoTracker_API_Error(
                    f"Request Authentication Error, " f"code {r.status_code} " f"Response: {r.text} "
                ) from None

            raise MeteoTracker_API_Error(
                f"Request failed, code {r.status_code} " f"URL: {r.url} " f"Response: {r.text} "
            ) from None
        return r

    def query_sessions(
        self,
        timespan: Tuple[datetime, datetime] | None = None,
        type: MT_DataType = "all",
        items: int = 1000,
        page: int = 0,
        author: str = "",
        raw=False,
        **kwargs,
    ) -> List[MT_Session]:
        params = {
            "dataType": type,
            "items": items,
        }
        if author:
            params["by"] = author

        if timespan is not None:
            t1, t2 = (int(t.timestamp()) for t in timespan)
            if t2 < t1:
                raise ValueError("The timespan must be ordered (earlier, later)")
            # This cannot contain any spaces and must use double quotes, hence the ugly f-string
            params["startTime"] = (f'{{"$gte":{t1},"$lte":{t2}}}',)

        params.update(kwargs)
        responses = []
        for i in range(self.max_queries - 1):
            params["page"] = i
            response = self.get("https://app.meteotracker.com/api/Sessions", params=params).json()
            responses.append(response)
            if len(response) < 1000:
                break  # we're done

        json = [s for res in responses for s in res]
        if raw:
            return json
        return [MT_Session(**j) for j in json]

    def get_session_data(self, session: MT_Session) -> pandas.DataFrame:
        variables = session.columns + ["time", "lo"]

        json = self.get(
            "https://app.meteotracker.com/api/points/session", params=dict(id=session.id, data=" ".join(variables))
        ).json()

        df = pandas.DataFrame.from_records(json)
        if "lo" in df:
            df["lat"], df["lon"] = [r[1] for r in df["lo"].values], [r[0] for r in df["lo"].values]
            del df["lo"]

        return df

    def get_area_data(
        self,
        area: geometry.box = geometry.box(34, 8, 46, 10),
        timespan: Tuple[datetime, datetime] | None = None,
        type: MT_DataType = "allMap",
        items: int = 10000,
        raw=False,
        **kwargs,
    ) -> List[MT_Session]:
        # "https://app.meteotracker.com/api/points/map?dataType=myMap&
        # "time={"$gte" :1681381200000,"$lte":"2023-04-13 11:20:00Z"}&"
        # "lat={"$gte":39.1,"$lte":40.2}&"
        # "lon={"$gte":9.5,"$lte":10.3}&"""
        # "data=HDX &items=1000"
        if timespan is None:
            now = datetime.now()
            earlier = now - timedelta(days=3)
            timespan = (earlier, now)

        t1, t2 = (int(t.timestamp()) for t in timespan)
        if t2 < t1:
            raise ValueError("The timespan must be ordered (earlier, later)")

        params = {
            "dataType": type,
            "items": items,
            "time": f'{{"$gte":{t1},"$lte":{t2}}}',
            "lat": f'{{"$gte":{area.bounds[0]:.1f},"$lte":{area.bounds[2]:.1f}}}',
            "lon": f'{{"$gte":{area.bounds[1]:.1f},"$lte":{area.bounds[3]:.1f}}}',
            # This cannot contain any spaces and must use double quotes, hence the ugly f-string
        }

        params.update(kwargs)
        print(params)
        response = self.get("https://app.meteotracker.com/api/points/map", params=params).json()

        if raw:
            return response

        df = pandas.DataFrame.from_records(response.json())
        df["lat"], df["lon"] = [r[1] for r in df["lo"].values], [r[0] for r in df["lo"].values]
        del df["lo"]
        return df
