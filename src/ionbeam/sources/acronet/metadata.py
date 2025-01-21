import dataclasses
import logging
from typing import Iterable

from geoalchemy2.shape import from_shape, to_shape
from shapely.geometry import Point
from sqlalchemy.orm import Session

from ...core.bases import Action, Parser, TabularMessage
from ...metadata import db, id_hash

logger = logging.getLogger(__name__)

def create_sensor_if_necessary(source : Action, db_session : Session, name : str, property_names : list[str]):
     # Get all the sensors from the database that correspond to this particular set of properties
    properties = db.get_db_properties(source.globals, db_session, property_names)

    # Find all the sensors that contain all of the above properties
    candidates = db_session.query(db.Sensor).filter(*(db.Sensor.properties.contains(p) for p in properties)).all()
    
    # Manually filter out any sensors that have extra properties we didn't ask for
    candidates = [c for c in candidates if len(c.properties) == len(properties)]

    assert len(candidates) <= 1
    
    if not candidates:
        logger.info(f"Constructing a new Acronet sensor object with properties {[p.name for p in properties]}")

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

def add_acronet_station_to_metadata_store(action, db_session, message):
    station = message.metadata.unstructured["station"]
    properties = message.data.columns
    sensor = create_sensor_if_necessary(action, db_session, name = station["name"], property_names = properties, )

    # Convert the lat lon to shapely object can calculate the bbox
    feature = Point(station["lon"], station["lat"])
    
    station = db.Station(
        external_id = station["id"],
        internal_id = message.metadata.internal_id,
        platform = "acronet",
        name = station["name"],
        description = "An Acronet station",
        sensors = [sensor,],
        location = from_shape(feature, srid=4326),
        location_feature = from_shape(feature, srid=4326),
        earliest_reading = message.data["datetime"].min(), 
        latest_reading = message.data["datetime"].max(),
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

    station.location_feature = from_shape(union_geom)
    station.location = from_shape(union_geom.centroid)

    new_earliest = message.data["datetime"].min()
    new_latest = message.data["datetime"].max()

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


    def process(self, input_message: TabularMessage) -> Iterable[TabularMessage]:

        
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

        with self.globals.sql_session.begin() as db_session:
            station = db_session.query(db.Station).where(db.Station.external_id == station["id"]).one_or_none()
            if station is None:
                station = add_acronet_station_to_metadata_store(self, db_session, output_msg)
            else:
                update_acronet_station_in_metadata_store(db_session, output_msg, station)

            db_session.add(station)

        yield output_msg