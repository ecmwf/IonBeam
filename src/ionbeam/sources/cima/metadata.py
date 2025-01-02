import dataclasses
import logging
from typing import Iterable

from geoalchemy2.shape import to_shape
from shapely import to_geojson
from shapely.geometry import Point
from sqlalchemy.orm import Session

from ...core.bases import (
    FinishMessage,
    Parser,
    TabularMessage,
)
from ...metadata import db, id_hash

logger = logging.getLogger(__name__)

def get_db_properties(session, keys):
    "Given a list of observed property names, extract them from the database and return them as ORM objects"
    properties = []
    for property_name in keys:

        # Lookup the canonical variable in the database        
        canonical_property = session.query(db.Property).filter_by(name = property_name).one_or_none()

        if canonical_property is None:
            raise RuntimeError(f"A Property (canonical variable) with name={property_name!r} does not exist in the database")

        properties.append(canonical_property)

    return properties

def create_sensor_if_necessary(mt_source, db_session, name, properties):
     # Get all the sensors from the database that correspond to this particular set of properties
    properties = get_db_properties(db_session, properties)

    # Find all the sensors that contain all of the above properties
    candidates = db_session.query(db.Sensor).filter(*(db.Sensor.properties.contains(p) for p in properties)).all()
    
    # Manually filter out any sensors that have extra properties we didn't ask for
    candidates = [c for c in candidates if len(c.properties) == len(properties)]

    assert len(candidates) <= 1
    
    if not candidates:
        logger.info(f"Constructing a new Acronet sensor object with properties {[p.key for p in properties]}")

        # Construct a new sensor object to put into the database
        sensor = db.Sensor(
            name = f"{name} Sensor",
            description = "A placeholder sensor that is likely composed of multiple physical devices.",
            platform = "acronet",
            url = "",
            properties = properties)
        db_session.add(sensor)
        return sensor
    else: 
        return candidates[0]

def create_authors_if_necessary(db_session, authors):
    ORM_authors = []
    for name in authors:
        author = db_session.query(db.Author).where(db.Author.name == name).one_or_none()
        if not author:
            author = db.Author(name = name)
        ORM_authors.append(author)
    return ORM_authors

def add_acronet_station_to_metadata_store(db_session, message):
    station = message.metadata.unstructured["station"]
    properties = message.data.columns
    sensor = create_sensor_if_necessary(station, db_session, properties = properties, name = station["name"])

    # Convert the lat lon to shapely object can calculate the bbox
    feature = Point(station["lon"], station["lat"])
    
    station = db.Station(
        external_id = station["id"],
        internal_id = message.metadata.internal_id,
        platform = "acronet",
        name = station["name"],
        description = "An Acronet station",
        sensors = [sensor,],
        location = feature.wkt,
        location_feature = to_geojson(feature),
        earliest_reading = message.data["time"].min(), 
        latest_reading = message.data["time"].max(),
        authors = create_authors_if_necessary(db_session, ["acronet"]),
        extra = {
        }
    )

    logger.info(f"Adding station {station}")
    return station
    

def update_acronet_station_in_metadata_store(db_session, message, station):
    metadata_station = message.metadata.unstructured["station"]
    feature = Point(metadata_station["lon"], metadata_station["lat"])

    if station.location_feature:
        union_geom = to_shape(station.location_feature).union(feature)
    else:
        union_geom = feature

    station.location_feature = to_geojson(union_geom)
    station.location = union_geom.centroid.wkt

    new_earliest = message.data["time"].min()
    new_latest = message.data["time"].max()

    if station.earliest_reading is None:
        station.earliest_reading = new_earliest
    else:
        station.earliest_reading = min(station.earliest_reading_utc, new_earliest)

    if station.latest_reading is None:
        station.latest_reading = new_latest
    else:
        station.latest_reading = max(station.latest_reading_utc, new_latest)

@dataclasses.dataclass
class AddAcronetMetadata(Parser):
    def init(self, globals, **kwargs):
        super().init(globals, **kwargs)


    def process(self, input_message: TabularMessage | FinishMessage) -> Iterable[TabularMessage]:
        if isinstance(input_message, FinishMessage):
            return
        
        station = input_message.metadata.unstructured["station"]

        metadata = self.generate_metadata(
            message=input_message,
            internal_id = id_hash(station["id"]),
            external_id = station["id"],
        )

        output_msg = TabularMessage(
            metadata=metadata,
            data=input_message.data,
        )

        with Session(self.globals.sql_engine) as db_session:
            station = db_session.query(db.Station).where(db.Station.external_id == station["id"]).one_or_none()
            if station is None:
                station = add_acronet_station_to_metadata_store(db_session, output_msg)
            else:
                update_acronet_station_in_metadata_store(db_session, output_msg, station)

            db_session.add(station)
            db_session.commit()

        yield self.tag_message(output_msg, input_message)