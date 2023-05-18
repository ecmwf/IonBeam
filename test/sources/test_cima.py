import logging

import pytest

from obsproc.sources.cima import CIMA_API


@pytest.mark.network
def test_live_server(caplog):
    # do pytest --log-cli--level=true to see the log output
    logger = logging.getLogger()
    caplog.set_level(logging.INFO)

    # Setup the authentication and requests a token
    cima_api = CIMA_API("secrets.yaml")

    # Check that we can also refresh the token
    old_expiry = cima_api.oauth.token["expires_at"]
    cima_api.refresh_token()
    assert cima_api.oauth.token["expires_at"] > old_expiry

    assert "PLUVIOMETRO" in cima_api.sensor_names.IT
    logger.debug(cima_api.sensor_names.IT)

    stations = cima_api.list_stations_by_sensor("PLUVIOMETRO")
    known_station = {
        "id": "-1937156895_2",
        "name": "Campo Sportivo Bajardo",
        "lat": 43.900158,
        "lng": 7.726422,
        "mu": "mm",
    }
    assert known_station in stations

    # Stations and sensors are implemented as cached properties
    # they get computed once when you first access them
    assert "campo_sportivo_bajardo" in cima_api.stations
    assert (
        cima_api.stations["campo_sportivo_bajardo"]["name"] == "Campo Sportivo Bajardo"
    )

    assert cima_api.sensors["DIREZIONEVENTO"] == {
        "unit": "Degrees",
        "translation": "WIND_DIRECTION",
    }


def test_match_sensor_names():
    assert CIMA_API.match_sensor_names("PLUVIOMETR") == "PLUVIOMETRO"
