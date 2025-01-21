import dataclasses
import logging
from typing import Iterable

from geoalchemy2.shape import from_shape
from shapely.geometry import MultiPoint, box

from ...core.bases import Parser, TabularMessage
from ...metadata import db

logger = logging.getLogger(__name__)

def create_sensor_if_necessary(source, db_session, name, properties):
     # Get all the sensors from the database that correspond to this particular set of properties
    properties = list(db.get_db_properties(source.globals, db_session, properties))

    # Find all the sensors that contain all of the above properties
    candidates = db_session.query(db.Sensor).filter(*(db.Sensor.properties.contains(p) for p in properties)).all()
    
    # Manually filter out any sensors that have extra properties we didn't ask for
    candidates = [c for c in candidates if len(c.properties) == len(properties)]

    assert len(candidates) <= 1
    
    if not candidates:
        logger.info(f"Constructing a new meteotracker sensor object with properties {[p.name for p in properties]}")

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

def add_meteotracker_track_to_metadata_store(source, db_session, msg):
    assert msg.metadata.internal_id is not None
    assert msg.metadata.external_id is not None

    track = db_session.query(db.Station).where(db.Station.internal_id == msg.metadata.internal_id).one_or_none()
    if track:
        return True, track

    sensor = create_sensor_if_necessary(source, db_session, properties = msg.data.columns, name = "Meteotracker Sensor")

    # Convert the lat lon to shapely object can calculate the bbox
    feature = MultiPoint(msg.data[["lon", "lat"]].values) 
    bbox = box(*feature.bounds)
    
    track = db.Station(
        external_id = msg.metadata.external_id,
        internal_id = msg.metadata.internal_id,
        platform = "meteotracker",
        name = "MeteoTracker Track",
        description = "A MeteoTracker Track.",
        sensors = [sensor,],
        location = from_shape(bbox.centroid),
        location_feature = from_shape(bbox),
        earliest_reading = msg.data["datetime"].min(), 
        latest_reading = msg.data["datetime"].max(),
        authors = create_authors_if_necessary(db_session, ["meteotracker", msg.data["living_lab"].iloc[0], msg.data["author"].iloc[0]]),
        extra = {
        }
    )
    db_session.add(track)

    logger.info(f"Adding track {track}")

    return False, track


@dataclasses.dataclass
class AddMeteotrackerMetadata(Parser):
    def process(self, input_message: TabularMessage) -> Iterable[TabularMessage]:

        with self.globals.sql_session.begin() as db_session:
            already_there, track = add_meteotracker_track_to_metadata_store(self, db_session, input_message)

            if already_there:
                logger.info(f"Meteotracker with {track.internal_id = } {track.external_id = } already in the metadata database. Skipping.")
                return
        
        
        metadata = self.generate_metadata(
            message=input_message,
        )

        output_msg = TabularMessage(
            metadata=metadata,
            data=input_message.data,
        )

        yield output_msg