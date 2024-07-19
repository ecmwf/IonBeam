from ...metadata import db
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound
from shapely.geometry import MultiPoint, box
from shapely import to_geojson

import logging
logger = logging.getLogger(__name__)

def get_db_properties(mt_source, session, keys):
    "Given a list of observed property names, extract them from the database and return them as ORM objects"
    key_to_property= {c.key : c for c in mt_source.mappings}
    properties = []
    for property_name in keys:
        try:
            property = key_to_property[property_name]
        except KeyError:
            logger.warning(f"Could not find variable with key {property_name} in the value_columns of Meteotracker Source")
            continue

        # Skip if this variable is marked to be discarded
        if property.discard: continue

        # Lookup the canonical variable in the database        
        canonical_property = session.query(db.Property).filter_by(name = property.name).one_or_none()

        if canonical_property is None:
            raise RuntimeError(f"A Property (canonical variable) with name={property.name!r} does not exist in the database")

        properties.append(canonical_property)

    return properties

def create_sensor_if_necessary(mt_source, db_session, name, properties):
     # Get all the sensors from the database that correspond to this particular set of properties
    properties = get_db_properties(mt_source, db_session, properties)

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

def add_meteotracker_track_to_metadata_store(mt_source, mt_session, data):
    with Session(mt_source.globals.sql_engine) as db_session:
        track = db_session.query(db.Station).where(db.Station.external_id == mt_session.id).one_or_none()
        if track:
            logger.info(f"Track with external_id == {mt_session.id} already in the database")
            return True, track

        sensor = create_sensor_if_necessary(mt_source, db_session, properties = data.columns, name = "Meteotracker Sensor")

        # Convert the lat lon to shapely object can calculate the bbox
        feature = MultiPoint(data[["lon", "lat"]].values) 
        bbox = box(*feature.bounds)
        
        track = db.Station(
            external_id = mt_session.id,
            platform = "meteotracker",
            name = "MeteoTracker Track",
            description = "A MeteoTracker Track.",
            sensors = [sensor,],
            location = bbox.centroid.wkt,
            location_feature = to_geojson(feature),
            earliest_reading = data["time"].min(), 
            latest_reading = data["time"].max(),
            authors = create_authors_if_necessary(db_session, ["meteotracker", mt_session.author]),
            extra = {
            "offset_tz": mt_session.offset_tz,
            }
        )

        db_session.add(track)
        db_session.commit()
        logger.info(f"Adding track {track}")

        return False, track
