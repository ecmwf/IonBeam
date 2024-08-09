import logging
from datetime import datetime
from unicodedata import normalize

from shapely.geometry import Point
from sqlalchemy.orm import Session

from ...metadata import db

logger = logging.getLogger(__name__)


def construct_sck_authors(session, device):
    owner = device["owner"]
    return [
        session.query(db.Author).filter_by(name=owner["username"], external_id=owner["id"]).one_or_none()
        or db.Author(
            name=owner["username"],
            external_id=owner["id"],
        ),
        session.query(db.Author).filter_by(name="I-CHANGE").one_or_none()
        or db.Author(name="I-CHANGE", platform="Smart Citizen Kit"),
    ]


def construct_sck_sensors(sck_source, session, sensors):
    key_unit_to_property = {(c.key, c.unit): c for c in sck_source.mappings}
    db_sensors = {}
    for s in sensors:
        m = s["measurement"]
        property_name = m["name"]
        unit = normalize("NFKD", s["unit"])
        # Some of the sck sensors have a hierarchy, that looks like this:
        #            |-- "DHT11 - Temperature"
        # - "DHT22" -|
        #            |-- "DHT11 - Humidity"
        # This is a little redundant, as the child sensors don't really carry any more information
        # except the measurement that we extract above
        if s["ancestry"] is not None:
            s = sck_source.get_sensor(s["ancestry"])

        # Lookup this name/unit combo in the mappings of the Smart Citizen Kit source
        # eg "Barometric Pressure", kPa gives us the entry:
        # - name: air_pressure_near_surface
        #   key: "Barometric Pressure"
        #   unit: "kPa"
        # This tells us the canonical property is called air_pressure_near_surface and the source unit is kPa
        try:
            property = key_unit_to_property[(property_name, unit)]
        except KeyError:
            logger.warning(
                f"Could not find variable with key {(property_name, unit)} in the mappings of Smart Citizen Kit source"
            )
            continue

        # Skip if this variable is marked to be discarded
        if property.discard:
            continue

        # Lookup the canonical variable in the database
        canonical_property = session.query(db.Property).filter_by(name=property.name).one_or_none()

        if canonical_property is None:
            raise RuntimeError(
                f"A Property (canonical variable) with name={property.name!r} does not exist in the database"
            )

        # We only want to make one DHT22 sensor and append two properties to it
        if s["name"] in db_sensors:
            sensor.properties.append(canonical_property)
        else:
            sensor = db.Sensor(
                name=s["name"],
                description=s["description"],
                external_id=s["id"],
                platform="Smart Citizen Kit",
                url="",
                properties=[canonical_property],
            )

            db_sensors[sensor.name] = sensor
        session.add(sensor)
        yield sensor


def construct_sck_metadata(sck_source, device):
    if device["last_reading_at"] is None:
        logger.warning(f"Skipping device {device['id']} as it has no last_reading_at = None")
        return None

    with Session(sck_source.globals.sql_engine) as session:
        station = session.query(db.Station).filter_by(external_id=str(device["id"])).one_or_none()
        if station is not None:
            # Update station
            logger.debug("Sck_metadata: Found existing station, updating")
            station.earliest_reading = min(datetime.fromisoformat(device["created_at"]), station.earliest_reading_utc)
            station.latest_reading = max(datetime.fromisoformat(device["last_reading_at"]), station.latest_reading_utc)

            return station.as_json()

        # authors = []
        # sensors = construct_sck_sensors(session, device["sensors"])
        location_point = Point(device["location"]["longitude"], device["location"]["latitude"])
        station = db.Station(
            external_id=device["id"],
            platform="Smart Citizen Kit",
            name=device["name"],
            description=device["description"],
            location=location_point.wkt,
            location_feature=location_point.wkt,
            earliest_reading=datetime.fromisoformat(device["created_at"]),
            latest_reading=datetime.fromisoformat(device["last_reading_at"]),
            authors=[],
            sensors=list(construct_sck_sensors(sck_source, session, device["data"]["sensors"])),
            extra={
                "uuid": device["uuid"],
                "system_tags": device["system_tags"],
                "user_tags": device["user_tags"],
                "location": device["location"],
                "hardware": device["hardware"],
            },
        )
        session.add(station)
        session.commit()
        return station.as_json()
