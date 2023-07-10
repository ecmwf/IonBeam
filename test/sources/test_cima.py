"""
Test the CIMA API code both online  and offline

do_cima_tests isn't directly picked up as a test by pytest, instead it gets run in two places:

1. test_cima_online just let's the code loose to test against the live API, this is a bit slow.
2. test_cima_offline mocks up the API using the responses python library, the downside is that you have to add
    to the mocked API implementation whenever you add more tests.

"""

import logging
import json

import pytest
import responses
import re

from time import time

from datetime import datetime, timezone, timedelta

from obsproc.sources.cima.cima import CIMA_API, GenericSensor, APISensor

from pathlib import Path


def do_cima_tests(caplog):
    "This function isn't a test directlt, it's meant to be called with either a real or fake API, see below"
    # do pytest --log-cli--level=true to see the log output
    logger = logging.getLogger()
    caplog.set_level(logging.INFO)

    # check if there is a local secrets.yaml file
    secrets_file = "secrets.yaml" if Path("secrets.yaml").exists() else "example_secrets.yaml"

    # Setup the authentication and request a token
    cima_api = CIMA_API(secrets_file)

    # Check that we can also refresh the token
    old_expiry = cima_api.oauth.token["expires_at"]
    cima_api.refresh_token()
    assert cima_api.oauth.token["expires_at"] > old_expiry

    assert "PLUVIOMETRO" in cima_api.sensor_names.IT
    logger.debug(cima_api.sensor_names.IT)

    # Check that translating works
    stations_with_a_rain_sensor = cima_api.list_stations_by_sensor("RAIN_GAUGE")
    stations_with_a_rain_sensor_2 = cima_api.list_stations_by_sensor("PLUVIOMETRO")
    assert stations_with_a_rain_sensor == stations_with_a_rain_sensor_2

    known_station = APISensor(
        station_name="Campo Sportivo Bajardo", lat=43.900158, lon=7.726422, unit="mm", id="-1937156895_2"
    )
    assert known_station in stations_with_a_rain_sensor

    # Stations and sensors are implemented as cached properties
    # they get computed once when you first access them
    assert cima_api.stations["Campo Sportivo Bajardo"].lat == known_station.lat

    assert cima_api.sensors["PLUVIOMETRO"] == GenericSensor(
        name="PLUVIOMETRO",
        unit="mm",
        translation="RAIN_GAUGE",
    )

    # You can only get 3 days worth of data with a single API request
    start_date = datetime(2019, 10, 3, hour=0, tzinfo=timezone.utc)
    end_date = start_date + timedelta(days=3)

    cima_api._single_request_station_and_sensor(
        station_name="Campo Sportivo Bajardo",
        sensor_name="PLUVIOMETRO",
        start_date=start_date,
        end_date=end_date,
        aggregation_time_seconds=60 * 60,
    )

    # So this code automates sending multiple such requests
    end_date = start_date + timedelta(days=5)  # must use > 3 days here to test that functionality

    cima_api.get_data_by_station_and_sensor(
        station_name="Campo Sportivo Bajardo",
        sensor_name="RAIN_GAUGE",
        start_date=start_date,
        end_date=end_date,
        aggregation_time_seconds=60 * 60,
    )

    cima_api.get_data_by_station(
        station_name="Campo Sportivo Bajardo",
        start_date=start_date,
        end_date=end_date,
        aggregation_time_seconds=60 * 60,
    )


def test_match_sensor_names():
    assert CIMA_API.match_sensor_names("PLUVIOMETR") == "PLUVIOMETRO"


@pytest.fixture()
def mock_cima_api():
    # Give a fake response when the API initially asks for the endpoint configuration
    responses.get(
        "https://testauth.cimafoundation.org/auth/realms/webdrops/.well-known/openid-configuration",
        json=dict(token_endpoint="https://test.com/token"),
    )

    responses.add_callback(
        responses.POST,
        "https://test.com/token",
        callback=lambda r: (
            200,
            {},
            json.dumps(
                dict(
                    access_token="xxxxxxx",
                    expires_at=time() + 60 * 60,
                )
            ),
        ),
    )

    responses.get(
        "https://webdrops.cimafoundation.org/app/sensors/classes", json=["BAROMETRO", "PLUVIOMETRO", "DIREZIONEVENTO"]
    )

    responses.add_callback(
        responses.GET,
        re.compile("https://webdrops.cimafoundation.org/app/sensors/list/[^/]+"),
        callback=lambda r: (
            200,
            {"Content-Type": "application/json"},
            json.dumps(
                [
                    {
                        "id": "-1937156895_2",
                        "name": "Campo Sportivo Bajardo",
                        "lat": 43.900158,
                        "lng": 7.726422,
                        "mu": "mm",  # This is here so that I can return the same data for every sensor
                    }
                ],
            ),
        ),
    )

    responses.add_callback(
        responses.GET,
        re.compile("https://webdrops.cimafoundation.org/app/sensors/data/[^/]+/[^/]+"),
        callback=lambda r: (
            200,
            {"Content-Type": "application/json"},
            json.dumps([dict(values=[1, 2, 3], timeline=[202305010000, 202305010001, 202305010002])]),
        ),
    )


@responses.activate
def test_cima_offline(mock_cima_api, caplog):
    "This test uses the responses library to mock the CIMA API"
    do_cima_tests(caplog)


@pytest.mark.network
def test_cima_online(caplog):
    do_cima_tests(caplog)
