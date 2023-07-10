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
Test the MeteoTracker API code both online  and offline


"""
import json
import responses
import requests
from functools import wraps
import pytest
import logging
from pathlib import Path

from typing import Dict, List, Tuple, Callable

from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

from obsproc.sources.meteotracker.meteotracker import MT_DataType, MT_WeatherPoint, SessionId, MeteoTracker_API


def test_MT_WeatherPoint():
    test_date = datetime(2019, 10, 3, hour=0, tzinfo=timezone.utc)
    test_API_response = {"lattitude": 0, "longitude": 1, "time": test_date, "s": 3, "HDX": 5, "extra_key": 10}
    datum = MT_WeatherPoint.from_API(test_API_response)
    assert datum == MT_WeatherPoint(lat=0, lon=1, time=test_date, wind_speed=3, humidex=5)

    generated_API_version = datum.to_API()
    assert generated_API_version == {"lattitude": 0, "longitude": 1, "time": test_date, "s": 3, "HDX": 5}


def do_meteotracker_tests(caplog):
    logging.getLogger()
    caplog.set_level(logging.INFO)

    # check if there is a local secrets.yaml file
    secrets_file = "secrets.yaml" if Path("secrets.yaml").exists() else "example_secrets.yaml"

    # Setup the authentication and request a token
    api = MeteoTracker_API(secrets_file)

    from datetime import datetime, timedelta

    now = datetime.now()
    earlier = now - timedelta(days=1)
    timespan = (earlier, now)

    sessions = api.query_sessions(timespan, items=5)

    session = sessions[0]
    api.get_session_data(session)


@pytest.mark.network
def test_meteotracker_online(caplog):
    do_meteotracker_tests(caplog)


# TODO: Get the offline tests working
# def test_meteotracker_offline(caplog):
#     do_meteotracker_tests(caplog)


def API_endpoint(func: Callable) -> Callable:
    "Helper function to quickly make RESTful API endpoints from python functions"

    @wraps(func)
    def _f(request: requests.Request) -> Tuple[int, Dict, str]:
        parsed = urlparse(request.url)
        params = {k: json.loads(v) for k, v in parse_qs(parsed.query).items()}
        response_code, headers, content = func(**params)
        return response_code, headers, json.dumps(content)

    return _f


@API_endpoint
def query_sessions(*, dataType: MT_DataType, page: int, **kwargs) -> Tuple[int, Dict, List[Dict[str, str]]]:
    "Session endpoint is used to query for what sessios (tracks) exist"

    datetime(2019, 10, 3, hour=0, tzinfo=timezone.utc)
    headers = {"Content-Type": "application/json"}
    session = {"id": "6436c512cd4b1602a4088cd", "departure": "", "arrival": ""}
    session2 = {"id": "6456c512cd4b1602a4088cd", "departure": "", "arrival": ""}
    content = [session, session2]
    return (200, headers, content)


responses.add_callback(
    responses.GET,
    "https://app.meteotracker.com/api/Sessions",
    callback=query_sessions,
)


@API_endpoint
def get_session_data(*, id: SessionId, plots: List[str], **kwargs) -> Tuple[int, Dict, List[Dict[str, str]]]:
    "Used to get the WeatherPoints associated with a particular session"

    datetime(2019, 10, 3, hour=0, tzinfo=timezone.utc)
    headers = {"Content-Type": "application/json"}
    session = {"id": "6436c512cd4b1602a4088cd", "departure": "", "arrival": ""}
    session2 = {"id": "6456c512cd4b1602a4088cd", "departure": "", "arrival": ""}
    content = [session, session2]
    return (200, headers, content)


responses.add_callback(
    responses.GET,
    "https://app.meteotracker.com/api/points/Session",
    callback=get_session_data,
)
