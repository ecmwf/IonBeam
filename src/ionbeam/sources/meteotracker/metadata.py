import dataclasses
import logging
from typing import Iterable

from shapely import to_geojson
from shapely.geometry import MultiPoint, box
from sqlalchemy.orm import Session

from ...core.bases import Parser, TabularMessage
from ...metadata import db

logger = logging.getLogger(__name__)

def get_db_properties(db_session, properties):
    "Given a list of observed property names, extract them from the database and return them as ORM objects"
    for property in properties:
        canonical_property = db_session.query(db.Property).filter_by(name = property).one_or_none()

        if canonical_property is None:
            raise RuntimeError(f"A Property (canonical variable) with name={property} does not exist in the database")

        yield canonical_property

    return properties

def create_sensor_if_necessary(db_session, name, properties):
     # Get all the sensors from the database that correspond to this particular set of properties
    properties = list(get_db_properties(db_session, properties))

    # Find all the sensors that contain all of the above properties
    candidates = db_session.query(db.Sensor).filter(*(db.Sensor.properties.contains(p) for p in properties)).all()
    
    # Manually filter out any sensors that have extra properties we didn't ask for
    candidates = [c for c in candidates if len(c.properties) == len(properties)]

    assert len(candidates) <= 1
    
    if not candidates:
        logger.info(f"Constructing a new meteotracker sensor object with properties {[p.key for p in properties]}")

        # Construct a new sensor object to put into the database
        sensor = db.Sensor(
            name = f"{name} Sensor",
            description = "A placeholder sensor that is likely composed of multiple physical devices.",
            platform = "meteotracker",
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

def add_meteotracker_track_to_metadata_store(db_session, msg):
    track = db_session.query(db.Station).where(db.Station.internal_id == msg.metadata.internal_id).one_or_none()
    if track:
        logger.info(f"Track with {db.Station.internal_id = } already in the database.")
        return True, track

    sensor = create_sensor_if_necessary(db_session, properties = msg.data.columns, name = "Meteotracker Sensor")

    # Convert the lat lon to shapely object can calculate the bbox
    feature = MultiPoint(msg.data[["lon", "lat"]].values) 
    bbox = box(*feature.bounds)
    
    track = db.Station(
        external_id = msg.metadata.internal_id,
        internal_id = msg.metadata.external_id,
        platform = "meteotracker",
        name = "MeteoTracker Track",
        description = "A MeteoTracker Track.",
        sensors = [sensor,],
        location = bbox.centroid.wkt,
        location_feature = to_geojson(bbox),
        earliest_reading = msg.data["datetime"].min(), 
        latest_reading = msg.data["datetime"].max(),
        authors = create_authors_if_necessary(db_session, ["meteotracker", msg.metadata.author]),
        extra = {
        }
    )

    db_session.add(track)
    db_session.commit()
    logger.info(f"Adding track {track}")

    return False, track


@dataclasses.dataclass
class AddMeteotrackerMetadata(Parser):
    def process(self, input_message: TabularMessage) -> Iterable[TabularMessage]:
        
        with Session(self.globals.sql_engine) as db_session:
            already_there, track = add_meteotracker_track_to_metadata_store(db_session, input_message)

        if already_there:
            logger.info(f"Meteotracker with {track.external_id = } already in the metadata database. Skipping.")
            return
        
        output_msg = TabularMessage(
            metadata=input_message.metadata,
            data=input_message.data,
        )

        yield self.tag_message(output_msg, input_message)