from typing import List
from sqlalchemy import ForeignKey, Table, Column
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session
from sqlalchemy.types import JSON
from sqlalchemy_utils import UUIDType, URLType
import uuid

from datetime import datetime

from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape  

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

class Property(Base):
    __tablename__ = "property"

    # # multiple humidity properties may exist but each must have a different unit
    # __table_args__ = (UniqueConstraint("key", "unit"),)
    
    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(unique=True) # A simple, string reference name
    url = mapped_column(URLType, nullable = True) # A semantic URL for this object
    
    name: Mapped[str] # A human readable name
    description: Mapped[str] = mapped_column(nullable=True)
    unit: Mapped[str] = mapped_column(nullable=True)

    # unit_id: Mapped[int] = mapped_column(ForeignKey("unit.id"))
    # unit: Mapped["Unit"] = relationship()
    
    sensors: Mapped[List["Sensor"]] = relationship(
        secondary=property_sensor_association_table, back_populates="properties")

    def __repr__(self) -> str:
        return f"Property(id={self.id!r}, name={self.name!r}, unit={self.unit!r}, description={self.description!r})"

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
    name: Mapped[str]
    description: Mapped[str] 
    url = mapped_column(URLType, nullable = True) # A semantic URL for this object
    
    properties: Mapped[set[Property]] = relationship(
        secondary=property_sensor_association_table, back_populates="sensors")

    stations: Mapped[set["Station"]] = relationship(
        secondary=sensor_station_association_table, back_populates="sensors")
    
    def __repr__(self) -> str:
        return f"Sensor(id={self.id}, properties={self.properties!r})"
    
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
    name: Mapped[str] = mapped_column(unique = True)
    description: Mapped[str] = mapped_column(nullable = True)
    url = mapped_column(URLType, nullable = True)

    stations: Mapped[set["Station"]] = relationship(
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
    
    sensors: Mapped[set["Sensor"]] = relationship(
        secondary=sensor_station_association_table, back_populates="stations")
    
    location_point = mapped_column(Geometry('POINT'))
    location_bbox = mapped_column(Geometry('GEOMETRY'))
    location_hull = mapped_column(Geometry('GEOMETRY'))
    # location_geohash: Mapped[str]
    
    start_time: Mapped[datetime]
    stop_time: Mapped[datetime]
    
    authors: Mapped[list[Author]] = relationship(
        secondary=station_author_association_table, back_populates="stations")

    # Relating to how to find this in the FDB
    schema = mapped_column(JSON)
    schema_template = mapped_column(JSON)

    # Internal: Any extra data to keep around for development purposes
    raw = mapped_column(JSON)

    def __repr__(self) -> str:
        return f"Station(id={self.id}, external_id={self.external_id!r})"

    def as_json(self):
        return dict(
            external_id = self.external_id,
            location_point = to_shape(self.location_point).wkt,
            location_bbox = to_shape(self.location_bbox).wkt,
            location_hull = to_shape(self.location_hull).wkt,
            start_time = self.start_time.isoformat() + "Z",
            stop_time = self.stop_time.isoformat() + "Z",
            authors = [a.as_json() for a in self.authors],
            sensors = [s.as_json() for s in self.sensors],
        )
    

def init_db(db_engine, canonical_variables):
    "Create or recreate the database schema"

    # Clear out all the tables and recreate them
    logger.warning("Deleting all SQL tables")
    Base.metadata.drop_all(db_engine)

    logger.warning("Recreating all SQL tables")
    Base.metadata.create_all(db_engine)
    with Session(db_engine) as session:
        
        # Populate the properties table from the config
        for variable in canonical_variables:
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
        for source in ["Sensor.Community", "Meteotracker", "Acronet"]:
            author = session.query(Author).where(Author.name == source).one_or_none()
            if not author:
                logger.info(f"Adding {source!r} to Authors table")
                session.add(Author(name = source, description = ""))

        session.commit()
