from . import db
from sqlalchemy import select
from sqlalchemy.orm.exc import NoResultFound
from shapely.geometry import MultiPoint, box

import logging
logger = logging.getLogger(__name__)

def get_db_properties(session, keys):
    "Given a list of observed property names, extract them from the database and return them as ORM objects"
    properties = set()
    for key in keys:
        try: properties.add(session.scalars(select(db.Property).where(db.Property.key == key)).one())
        except NoResultFound as _:
            raise NoResultFound(f"""A Property (canonical variable) with key={key!r} does not exist in the database, this is likely because it isn't 
            defined in the canonical variables file.
            """) from None
    return properties

def create_sensor_if_necessary(db_session, name, properties):
     # Get all the sensors from the database that correspond to this particular set of properties
    properties = get_db_properties(db_session, properties)

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

def add_meteotracker_track_to_metadata_store(db_session, message):
    id = message.data.track_id[0]
    data = message.data
    
    track = db_session.query(db.Station).where(db.Station.external_id == id).one_or_none()
    if track:
        logger.info(f"Track with external_id == {id} already in the database")
        return track

    # We don't actually know anything about the meteotracker sensors so make a dummy one for each distinct set of sensors
    assert message.metadata.variables is not None, f"Need to populate the variables metadata key for source = {message.metadata.source}!"
    sensor = create_sensor_if_necessary(db_session, properties = message.metadata.variables, name = message.metadata.source)

    # Convert the lat lon to shapely object can calculate the bbox
    poly = MultiPoint(data[["lon", "lat"]].values) 
    bbox = box(*poly.bounds)
    
    track = db.Station(
        external_id = id,
        platform = message.metadata.source,
        sensors = {sensor,},
        location_point = bbox.centroid.wkt,
        location_bbox = bbox.wkt,
        location_hull = poly.convex_hull.wkt,
        start_time = data["time"].min(), 
        stop_time = data["time"].max(),
        authors = create_authors_if_necessary(db_session, [message.data.author[0], message.metadata.source]),
    )

    logger.info(f"Adding track {track}")
    db_session.add(track)
    db_session.commit()

    return track