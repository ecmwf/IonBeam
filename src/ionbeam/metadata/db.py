# from __future__ import annotations  # PEP 563: Postponed Evaluation of Type Annotations
from typing import List, Optional
from sqlalchemy import ForeignKey, Table, Column, event, create_engine, URL, text, MetaData, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session
from sqlalchemy.types import JSON
from sqlalchemy_utils import UUIDType, URLType
import uuid

from datetime import datetime
import datetime as dt

from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape  
from shapely import to_geojson

import json

import logging
logger = logging.getLogger(__name__)

class Base(DeclarativeBase):
    pass


# https://docs.sqlalchemy.org/en/20/orm/basic_relationships.html#setting-bi-directional-many-to-many
property_sensor_association_table = Table(
    "property_sensor_association_table",
    Base.metadata,
    Column("property", ForeignKey("property.id"), primary_key=True),
    Column("sensor", ForeignKey("sensor.id"), primary_key=True),
)

sensor_station_association_table = Table(
    "sensor_station_association_table",
    Base.metadata,
    Column("sensor", ForeignKey("sensor.id"), primary_key=True),
    Column("station", ForeignKey("station.id"), primary_key=True),
)

station_author_association_table = Table(
    "station_author_association_table",
    Base.metadata,
    Column("station", ForeignKey("station.id"), primary_key=True),
    Column("author", ForeignKey("author.id"), primary_key=True),
)

sensor_parent_child_association_table = Table(
    "sensor_parent_child_association_table",
    Base.metadata,
    Column("parent", ForeignKey("sensor.id"), primary_key=True),
    Column("child", ForeignKey("sensor.id"), primary_key=True),
)
    
class Property(Base):
    __tablename__ = "property"

    # # multiple humidity properties may exist but each must have a different unit
    __table_args__ = (UniqueConstraint("key", "unit"),)
    key: Mapped[str]
    unit: Mapped[str] = mapped_column(nullable=True)
    
    id: Mapped[int] = mapped_column(primary_key=True)
    url = mapped_column(URLType, nullable = True) # A semantic URL for this object
    name: Mapped[str] # A human readable name
    description: Mapped[str] = mapped_column(nullable=True)

    
    sensors: Mapped[List["Sensor"]] = relationship(
        secondary=property_sensor_association_table, back_populates="properties")

    def __repr__(self) -> str:
        return f"Property(key={self.key!r}, name={self.name!r}, unit={self.unit!r}, description={self.description!r})"

    def as_json(self):
        return dict(
            key = self.key,
            name = self.name,
            unit = self.unit,
            description = self.description,
            url = self.url.url if self.url else None,
        )

class Sensor(Base):
    __tablename__ = "sensor"
    id: Mapped[int] = mapped_column(primary_key=True)
    external_id: Mapped[str] = mapped_column(nullable = True) # Whatever id came from the external source
    platform: Mapped[str] # The source of this sensor
    name: Mapped[str]
    description: Mapped[str] 
    url = mapped_column(URLType, nullable = True) # A semantic URL for this object
    
    properties: Mapped[list[Property]] = relationship(
        secondary=property_sensor_association_table, back_populates="sensors")

    stations: Mapped[list["Station"]] = relationship(
        secondary=sensor_station_association_table, back_populates="sensors")
    
    parent : Mapped[Optional["Sensor"]] = relationship("Sensor", 
                                                       secondary = sensor_parent_child_association_table,
                                                         back_populates="children",
                                                         foreign_keys=[sensor_parent_child_association_table.c.child])
    children: Mapped[list["Sensor"]] = relationship("Sensor", 
                                                   secondary = sensor_parent_child_association_table,
                                                     back_populates="parent", uselist=True,
                                                     foreign_keys=[sensor_parent_child_association_table.c.parent])
    
    def __repr__(self) -> str:
        return f"Sensor(id={self.id}, name='{self.name}')"
    
    def as_json(self):
        return dict(
            name = self.name,
            description = self.description,
            url = self.url.url if self.url else None,
            properties = [p.as_json() for p in self.properties]
        )

class Author(Base):
    __tablename__ = "author"
    id: Mapped[int] = mapped_column(primary_key=True)
    external_id: Mapped[str] = mapped_column(nullable = True) # Whatever id came from the external source
    name: Mapped[str] = mapped_column(unique = True)
    description: Mapped[str] = mapped_column(nullable = True)
    url = mapped_column(URLType, nullable = True)

    stations: Mapped[list["Station"]] = relationship(
        secondary=station_author_association_table, back_populates="authors")
    
    def __repr__(self) -> str:
        return f"Author(id={self.id}, name={self.name!r}, description={self.name!r})"
    
    def as_json(self):
        return dict(
            name = self.name,
            description = self.description,
            url = self.url.url if self.url else None,
        )

class Station(Base):
    __tablename__ = "station"
    id = mapped_column(UUIDType, primary_key=True, default=uuid.uuid4)
    external_id: Mapped[str] # Whatever id came from the external source
    platform: Mapped[str]
    
    name: Mapped[str]
    description: Mapped[str] =  mapped_column(nullable = True)
    
    sensors: Mapped[list["Sensor"]] = relationship(
        secondary=sensor_station_association_table, back_populates="stations")
    
    location = mapped_column(Geometry('POINT'))
    location_feature = mapped_column(Geometry('GEOMETRY'))
    
    earliest_reading: Mapped[datetime]
    latest_reading: Mapped[datetime]
    
    authors: Mapped[list[Author]] = relationship(
        secondary=station_author_association_table, back_populates="stations")

    # Relating to how to find this in the FDB
    schema = mapped_column(JSON)
    schema_template = mapped_column(JSON)

    # Internal: Any extra data to keep around for development purposes
    extra = mapped_column(JSON)

    # Timezone aware (i.e they know they're in UTC) versions of the times,
    # This is needed because SQLAlchemy doesn't store timezone information internally
    # So while the stored times are in UTC, they are returned as naive datetimes
    @property
    def earliest_reading_utc(self):
        return self.earliest_reading.replace(tzinfo=dt.timezone.utc)
    
    @property
    def latest_reading_utc(self):
        return self.latest_reading.replace(tzinfo=dt.timezone.utc)

    def __repr__(self) -> str:
        return f"Station(id={self.id}, external_id={self.external_id!r})"

    def as_json(self):

        d = {k : getattr(self, k) for k in ["name", "description", "platform", "external_id"]}
        
        location_feature = to_shape(self.location_feature)
        location_geojson = json.loads(to_geojson(location_feature))
        location_geojson["bbox"] = location_feature.bounds # Optional bbox, see https://datatracker.ietf.org/doc/html/rfc7946#section-5

        d.update(
            location = tuple(to_shape(self.location).coords)[0],
            geojson = location_geojson,
            time_span = [self.earliest_reading.isoformat() + "Z", self.latest_reading.isoformat() + "Z"],
            authors = [a.as_json() for a in self.authors],
            sensors = [s.as_json() for s in self.sensors],
        )

        return d
    
    def find_by(session, **kwargs):
        return session.query(Station).filter_by(**kwargs).all()
    
    def find_by_external_id(session, external_id):
        return session.query(Station).filter_by(external_id = external_id).one_or_none()
    
    def get_all(session):
        return session.query(Station).all()
    

class IngestedChunk(Base):
    __tablename__ = "ingested_chunk"
    id = mapped_column(UUIDType, primary_key=True, default=uuid.uuid4)
    source: Mapped[str]
    cache_key: Mapped[str]

def init_db(globals):
    "Create or recreate the database schema"

    logger.warning(f"Creating Postgres schema '{globals.namespace}' if it doesn't exist")
    with Session(globals.sql_engine) as session:
        # Create a postgres schema for this namespace
        session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {globals.namespace}"))
        session.execute(text(f"CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public"))
        session.execute(text(f"GRANT ALL PRIVILEGES ON DATABASE postgres TO postgres;"))
        session.commit()

    db_engine = globals.sql_engine

    # Clear out all the tables and recreate them
    logger.warning(f"Deleting all SQL tables in schema {globals.namespace}")
    m = MetaData(schema=globals.namespace)
    m.reflect(db_engine)
    m.drop_all(db_engine)

    logger.warning("Recreating all SQL tables")
    Base.metadata.create_all(db_engine)

    logger.warning("Populating properties from the config")
    with Session(db_engine) as session:
        
        # Populate the properties table from the config
        for variable in globals.canonical_variables:
            property = session.query(Property).where(Property.key == variable.name).one_or_none()
            if not property:
                p = Property(
                    key = variable.name,
                    name = variable.name,
                    description = variable.desc,
                    unit = variable.unit,
                    url = "")
                session.add(p)

        # Prepopulate some authors
        for source in ["Sensor.Community", "Meteotracker", "Acronet", "SmartCitizenKit"]:
            author = session.query(Author).where(Author.name == source).one_or_none()
            if not author:
                logger.info(f"Adding {source!r} to Authors table")
                session.add(Author(name = source, description = ""))

        session.commit()

def create_namespaced_engine(namespace, **kwargs):
    engine = create_engine(URL.create(
        "postgresql+psycopg2", database = "postgres", **kwargs), 
         connect_args={"options": "-c timezone=utc"}, # Tell postgres to return all dates in UTC
        echo = False)
    
    @event.listens_for(engine, "connect")
    def set_schema(dbapi_connection, connection_record):
        with dbapi_connection.cursor() as cursor:
            cursor.execute(f"SET search_path TO {namespace}, public")

    return engine

def get_authors(globals):
    with Session(globals.sql_engine) as session:
        return session.query(Author).all()