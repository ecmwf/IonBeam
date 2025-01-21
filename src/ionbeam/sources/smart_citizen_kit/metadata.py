import dataclasses
import logging
from datetime import datetime
from typing import Iterable

from geoalchemy2.shape import from_shape, to_shape
from shapely.geometry import Point
from sqlalchemy.orm import Session

from ...core.bases import CanonicalVariable, DataMessage, Mappings, Parser, TabularMessage
from ...metadata import db, id_hash

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


def construct_sck_sensors(session : Session, message : DataMessage):
    assert message.metadata.columns is not None
    for canonical_variable in message.metadata.columns.values():
        assert isinstance(canonical_variable, CanonicalVariable)
        raw_variable = canonical_variable.raw_variable
        assert raw_variable is not None
        
        if raw_variable.metadata is None or raw_variable.metadata == {}: 
            logger.warning(f"Skipping raw variable {raw_variable.name} as it has no metadata")
            continue

        # Lookup the canonical variable in the database
        db_canonical_variable = session.query(db.Property).filter_by(name=canonical_variable.name).one_or_none()

        if db_canonical_variable is None:
            logger.debug(f"Creating new canonical property {canonical_variable.name} unit = {canonical_variable.unit}")
            db_canonical_variable = db.Property(
                name=canonical_variable.name,
                description=canonical_variable.description,
                unit=canonical_variable.unit,
            )
            session.add(db_canonical_variable)

        # logger.debug(f"{raw_variable.metadata = }")
        sensor = db.Sensor(
            name=raw_variable.metadata["name"],
            description=raw_variable.metadata["description"],
            external_id=raw_variable.metadata["id"],
            platform="Smart Citizen Kit",
            url="",
            properties=[db_canonical_variable],
        )

        session.add(sensor)
        yield sensor


def construct_sck_metadata(
        db_session : Session, 
        output_message : DataMessage,
        device : dict,
        start_date : datetime, end_date : datetime):
    if device["last_reading_at"] is None:
        logger.warning(f"Skipping device {device['id']} as it has no last_reading_at = None")
        return None

    station = db_session.query(db.Station).filter_by(external_id=str(device["id"])).one_or_none()
    if station is not None:
        # Update station
        logger.debug("Sck_metadata: Found existing station, updating")
        feature = Point(device["location"]["longitude"], device["location"]["latitude"])

        if station.location_feature:
            union_geom = to_shape(station.location_feature).union(feature)
        else:
            union_geom = feature

        station.location_feature = from_shape(union_geom)
        station.location = from_shape(union_geom.centroid)

        station.earliest_reading = min(start_date, station.earliest_reading_utc)
        station.latest_reading = max(end_date, station.latest_reading_utc)
        db_session.add(station)
        return station.as_json()

    location_point = Point(device["location"]["longitude"], device["location"]["latitude"])

    logger.debug(f"{type(location_point.wkt) = }")
    station = db.Station(
        external_id=output_message.metadata.external_id,
        internal_id=output_message.metadata.internal_id,
        platform="smart_citizen_kit",
        name=device["name"],
        description=device["description"],
        location=from_shape(location_point),
        location_feature=from_shape(location_point),
        earliest_reading=start_date,
        latest_reading=end_date,
        authors=[],
        sensors=list(construct_sck_sensors(db_session, output_message)),
        extra={
            "uuid": device["uuid"],
            "system_tags": device["system_tags"],
            "user_tags": device["user_tags"],
            "location": device["location"],
            "hardware": device["hardware"],
        },
    )
    db_session.add(station)
    return station.as_json()


@dataclasses.dataclass
class AddSmartCitizenKitMetadata(Parser):
    mappings: Mappings
    
    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)
        self.mappings_variable_unit_dict = {(column.key, column.unit): column for column in self.mappings}


    def process(self, input_message: TabularMessage) -> Iterable[TabularMessage]:
        device = input_message.metadata.unstructured["device"]
        assert input_message.metadata.time_span is not None

        metadata = self.generate_metadata(
            message=input_message,
            external_id=device["id"],
            internal_id=id_hash(str(device["id"])),
        )

        output_msg = TabularMessage(
            metadata=metadata,
            data=input_message.data,
        )

        with self.globals.sql_session.begin() as db_session:
            construct_sck_metadata(db_session, 
                                   output_msg,
                                   device = input_message.metadata.unstructured["device"], 
                                   start_date = input_message.metadata.time_span.start,
                                   end_date = input_message.metadata.time_span.end)

        yield output_msg