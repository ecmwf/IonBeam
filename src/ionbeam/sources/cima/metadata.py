import dataclasses
import logging
from typing import Iterable

from geoalchemy2.shape import to_shape
from shapely import to_geojson
from shapely.geometry import MultiPoint, box
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
    sensor = create_sensor_if_necessary(station, db_session, properties = message.metadata.observation_variable.split(","), name = station["name"])

    # Convert the lat lon to shapely object can calculate the bbox
    feature = MultiPoint(message.data[["lon", "lat"]].values) 
    bbox = box(*feature.bounds)
    
    station = db.Station(
        external_id = station["id"],
        internal_id = id_hash(station["id"]),
        platform = "acronet",
        name = station["name"],
        description = "An Acronet station",
        sensors = [sensor,],
        location = bbox.centroid.wkt,
        location_feature = to_geojson(feature),
        earliest_reading = message.data["time"].min(), 
        latest_reading = message.data["time"].max(),
        authors = create_authors_if_necessary(db_session, ["acronet"]),
        extra = {
        }
    )

    db_session.add(station)
    db_session.commit()
    logger.info(f"Adding track {station}")
    

def update_acronet_station_in_metadata_store(db_session, message, station):
    feature = MultiPoint(message.data[["lon", "lat"]].values) 
    new_bbox = box(*feature.bounds)

    if station.location_feature:
        union_geom = to_shape(station.location_feature).union(new_bbox)
    else:
        union_geom = new_bbox

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
    def init(self, globals):
        super().init(globals)


    def process(self, input_message: TabularMessage | FinishMessage) -> Iterable[TabularMessage]:
        if isinstance(input_message, FinishMessage):
            return

        station = input_message.metadata.unstructured["station"]

        with Session(self.globals.sql_engine) as db_session:
            station = db_session.query(db.Station).where(db.Station.external_id == station["id"]).one_or_none()
            if station is None:
                add_acronet_station_to_metadata_store(db_session, input_message)
            else:
                update_acronet_station_in_metadata_store(db_session, input_message, station)

        output_msg = TabularMessage(
            metadata=self.generate_metadata(
                message=input_message,
                 time_span = input_message.metadata.time_span,
            ),
            data=input_message.data,
        )

        yield self.tag_message(output_msg, input_message)