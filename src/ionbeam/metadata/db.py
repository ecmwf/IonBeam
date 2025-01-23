# from __future__ import annotations  # PEP 563: Postponed Evaluation of Type Annotations
import logging
import uuid
from typing import Iterable, List, Self

import orjson
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape, to_shape
from shapely.errors import GEOSException
from shapely.geometry import MultiPolygon, Point, Polygon, box
from sqlalchemy import (
    URL,
    Column,
    ForeignKey,
    Index,
    String,
    Table,
    UniqueConstraint,
    create_engine,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, TSTZRANGE, insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, validates
from sqlalchemy.sql import text
from sqlalchemy.sql.expression import BooleanClauseList
from sqlalchemy_utils import URLType, UUIDType

from ..core.time import TimeSpan

logger = logging.getLogger(__name__)


"""
Data Model:

All data comes from one station.

Each station has a list measures a set of physical properties, i.e humidity, temperature, etc.

Each property is associated with exactly one sensor that measures it.

Each sensor can measure multiple properties.

Sensors can be shared between stations but properties are unique to a sensor.
"""

class Base(DeclarativeBase):
    pass


# https://docs.sqlalchemy.org/en/20/orm/basic_relationships.html#setting-bi-directional-many-to-many

property_station_association_table = Table(
    "property_station_association_table",
    Base.metadata,
    Column("property", ForeignKey("property.id"), primary_key=True),
    Column("station", ForeignKey("station.id"), primary_key=True),
)

station_author_association_table = Table(
    "station_author_association_table",
    Base.metadata,
    Column("station", ForeignKey("station.id"), primary_key=True),
    Column("author", ForeignKey("author.id"), primary_key=True),
)


class Property(Base):
    """
    Describes a physical property
    """
    __tablename__ = "property"
    __table_args__ = (
        UniqueConstraint("name", "unit"),
        Index("idx_name_unit", "name", "unit"),
    )
    name: Mapped[str]
    unit: Mapped[str] = mapped_column(nullable=True)
    id: Mapped[int] = mapped_column(primary_key=True)
    url = mapped_column(URLType, nullable=True)  # A semantic URL for this object
    description: Mapped[str] = mapped_column(nullable=True)
    sensor: Mapped[str] = mapped_column(nullable=True)

    stations: Mapped[List["Station"]] = relationship(
        secondary=property_station_association_table, back_populates="properties"
    )

    def __repr__(self) -> str:
        return f"Property(name={self.name!r}, unit={self.unit!r}, description={self.description!r})"

    def as_json(self):
        return dict(
            name=self.name,
            unit=self.unit,
            description=self.description,
            url=self.url.url if self.url else None,
        )
    
    @classmethod
    def upsert_multiple(cls, session : Session, properties: list[dict]) -> Iterable[Self]:
        stmt = insert(cls).values(properties).on_conflict_do_nothing()
        session.execute(stmt)
        property_names = [p["name"] for p in properties]
        return session.query(cls).filter(cls.name.in_(property_names)).all()


class Author(Base):
    """
    Describes an entity that produces data can be associated with one or more stations.
    """
    __tablename__ = "author"
    id: Mapped[int] = mapped_column(primary_key=True)
    external_id: Mapped[str] = mapped_column(
        nullable=True
    )
    name: Mapped[str] = mapped_column(unique=True)
    description: Mapped[str] = mapped_column(nullable=True)
    url = mapped_column(URLType, nullable=True)

    stations: Mapped[list["Station"]] = relationship(
        secondary=station_author_association_table, back_populates="authors"
    )

    def __repr__(self) -> str:
        return f"Author(id={self.id}, name={self.name!r}, description={self.name!r})"

    def as_json(self):
        json = dict(
            id=self.id,
            name=self.name,
        )
        if self.description: json["description"] = self.description
        if self.url: json["url"] = self.url.url
        return json
    
    @classmethod
    def upsert_multiple(cls, session : Session, authors: list[dict]) -> Iterable[Self]:
        for a in authors:
            author = session.query(Author).where(Author.name == a["name"]).one_or_none()
            if not author:
                author = cls(name = a["name"])
                session.add(author)
            yield author




class Station(Base):
    __tablename__ = "station"
    id = mapped_column(UUIDType, primary_key=True, default=uuid.uuid4)
    external_id: Mapped[str] = mapped_column(String, nullable=False)
    internal_id: Mapped[str] = mapped_column(String, nullable=False)
    platform: Mapped[str] = mapped_column(String, nullable=False)
    aggregation_type: Mapped[str] = mapped_column(String, nullable=False)

    name: Mapped[str] = mapped_column(String, nullable=True)
    description: Mapped[str] = mapped_column(String, nullable=True)
    
    # TimeSpan stored as PostgreSQL TSTZRANGE
    _time_span = mapped_column(TSTZRANGE, nullable=False)

    properties: Mapped[list[Property]] = relationship(
        secondary=property_station_association_table, back_populates="stations"
    )

    _location = mapped_column("location", Geometry("POINT", srid=4326), nullable=False)
    _bbox = mapped_column("bbox", Geometry("POLYGON", srid=4326), nullable=True)

    authors: Mapped[list[Author]] = relationship(
        secondary=station_author_association_table, back_populates="stations"
    )

    # # Internal: Any extra data to keep around for development purposes
    extra = mapped_column(JSONB)

    @property
    def bbox(self) -> Polygon | None:
        if self._bbox is None:
            return None
        return to_shape(self._bbox)

    @bbox.setter
    def bbox(self, value: Polygon):
        """Accept a Shapely shape, convert to GeoAlchemy geometry."""
        if isinstance(value, Point):
            self._bbox = None
        elif isinstance(value, (Polygon, MultiPolygon)):
            union_bounds = value.bounds  # (minx, miny, maxx, maxy)
            try:
                bbox_polygon = box(*union_bounds)
                self._bbox = from_shape(bbox_polygon, srid=4326)
            except GEOSException:
                self._bbox = None
        else:
            raise TypeError("Station.bbox must be a Shapely Point, Polygon, or MultiPolygon")


    @property
    def location(self) -> Point:
        return to_shape(self._location)

    @location.setter
    def location(self, value: Polygon):
        if isinstance(value, Point):
            self._location = from_shape(value, srid=4326)
        elif isinstance(value, (Polygon, MultiPolygon)):
            centroid = value.centroid
            self._location = from_shape(centroid, srid=4326)
        else:
            raise TypeError("Station.location must be a Shapely Point, Polygon, or MultiPolygon")
    
    @property
    def time_span(self) -> TimeSpan | None:
        if self._time_span.lower is None or self._time_span.upper is None:
            return None
        return TimeSpan(self._time_span.lower, self._time_span.upper)

    @time_span.setter
    def time_span(self, value: TimeSpan):
        if not isinstance(value, TimeSpan):
            raise TypeError("Station.time_span must be a TimeSpan instance")

        if value.start is None or value.end is None:
            raise ValueError("Station.time_span must have a start and end")
        
        tz_range = func.tstzrange(value.start, value.end)
        if tz_range is None:
            raise ValueError("Failed to create a time range")
        
        self._time_span = tz_range
    
    @validates('aggregation_type')
    def validate_aggregation_type(self, key, value):
        if value not in ["by_time", "by_station"]:
            raise ValueError("Station.aggregation_type must be 'by_time' or 'by_station'")
        return value
    
    __table_args__ = (
        UniqueConstraint("internal_id"),
        UniqueConstraint("external_id", "platform"),
        Index('idx_internal_id', "internal_id"),
        Index('idx_locations_point', "location", postgresql_using='gist'),
        Index('idx_locations_bounding_box', "bbox", postgresql_using='gist')
    )

    def __repr__(self) -> str:
        return f"Station(id={self.id}, external_id={self.external_id!r})"

    def mars_selection(self) -> dict[str, str]:
         # Compute a MARS date range that encloses the data for this station. 
        start_date = self.time_span.start.strftime("%Y%m%d")
        end_date = self.time_span.end.strftime("%Y%m%d")
        start_time = self.time_span.start.strftime("%H00")
        end_time = self.time_span.end.strftime("%H00")
        
        datetime_args = {}
        # If multi day period, just use data
        if start_date != end_date:
            datetime_args["date"] = f"{start_date}/to/{end_date}/by/1"
        
        # If singe day, multi hour period, just use time key with fixed date
        elif start_time != end_time:
            datetime_args["date"] = f"{start_date}"
            datetime_args["time"] = f"{start_time}/to/{end_time}/by/1"
        
        # All data within one hour
        else:
            datetime_args["date"] = f"{start_date}"
            datetime_args["time"] = f"{start_time}"

        return {
            "class": "rd",
            "expver": "xxxx",
            "stream": "lwda",
            "aggregation_type": self.aggregation_type,
            "platform": self.platform,
            "station_id": self.internal_id,
        } | datetime_args


    def as_json(self, type="simple"):
        d = dict(
            name=self.name,
            description=self.description,
            platform=self.platform,
            external_id=self.external_id,
            internal_id=self.internal_id,   
            aggegation_type=self.aggregation_type,
            location= {"lat" : self.location.y, "lon" : self.location.x},
            time_span=self.time_span.as_json() if self.time_span is not None else None,
            authors=[a.as_json() for a in self.authors],
            mars_selection = self.mars_selection(),
        )

        if type == "full":
            d.update(
                properties=[p.as_json() for p in self.properties],
                extra=self.extra,
            )

        return d
    
    @classmethod
    def upsert(
        cls,
        session: Session,
        name: str,
        internal_id: str,
        external_id: str,
        aggregation_type: str,
        authors: list[dict],       # e.g. [{"name": "Acronet"}, {"name": "Sensor.Community"}]
        properties: list[dict],       # e.g. [{"external_id": "abc", "platform": "foo", ...}, ...]
        platform: str,
        location,
        bbox,              # a Shapely Polygon for the new bounding box
        time_span: TimeSpan,
        extra: dict = {},
        description: str | None = None,
    ):
        """
        Checks if a station with `internal_id` exists.
        - If not, creates it (plus authors, properties).
        - If yes, updates the bounding box/time span, adds new properties/authors, etc.
        Returns the station (ORM object).
        """

        # 1) Lookup existing station by internal_id
        station = (
            session.query(Station)
            .filter(Station.internal_id == internal_id)
            .one_or_none()
        )

        properties = list(Property.upsert_multiple(session, properties))
        authors = list(Author.upsert_multiple(session, authors))

        if station is None:
            # logger.debug(f"Creating a new station with internal_id={internal_id!r}")
            station = Station(
                name=name,
                internal_id=internal_id,
                external_id=external_id,
                platform=platform,
                aggregation_type=aggregation_type,
                description=description,
                location=location,
                bbox=bbox,
                time_span=time_span,
                extra=extra,
                authors=authors,
                properties=properties,
            )

            session.add(station)
            return station
        else:
            pass
            # logger.debug(f"Updating existing station with internal_id={internal_id!r}")
        
        # 2) Update the existing station    

        station.location = location
        if station.bbox is None:
            station.bbox = bbox
        else:
            station.bbox = station.bbox.union(bbox)

        if station.time_span is None:
            station.time_span = time_span
        else:
            station.time_span = station.time_span.union(time_span)

        for author in authors:
            if author not in station.authors:
                station.authors.append(author)

        for p in properties:
            if p not in station.properties:
                station.properties.append(p)

        return station

    @classmethod
    def timespan_overlaps(cls, t: TimeSpan) -> BooleanClauseList:
        time_range = func.tstzrange(t.start, t.end, '[]')
        return cls._time_span.op("&&")(time_range)
        



def init_db(globals):
    "Create or recreate the database schema"
    db_engine = globals.sql_engine

    # Clear out all the tables and recreate them
    logger.warning("Deleting all SQL tables")
    db_engine.dispose()
    Base.metadata.drop_all(db_engine)
       
    logger.warning("Running CREATE EXTENSION IF NOT EXISTS postgis;")
    with Session(db_engine) as session:
        session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))

    logger.warning("Recreating all SQL tables")
    Base.metadata.create_all(db_engine)

    logger.warning("Populating properties from the config")
    with Session(db_engine) as session:
        # Prepopulate some authors
        for source in [
            "Sensor.Community",
            "Meteotracker",
            "Acronet",
            "SmartCitizenKit",
        ]:
            author = session.query(Author).where(Author.name == source).one_or_none()
            if not author:
                logger.info(f"Adding {source!r} to Authors table")
                session.add(Author(name=source, description=""))

def orjson_serializer(obj):
    return orjson.dumps(obj, option=orjson.OPT_SERIALIZE_NUMPY).decode()


def create_sql_engine(echo = False, **kwargs):
    engine = create_engine(
        URL.create("postgresql+psycopg2", database="postgres", **kwargs),
        connect_args={
            "options": "-c timezone=utc"
        },  # Tell postgres to return all dates in UTC
        echo=echo,
        json_serializer=orjson_serializer,
        json_deserializer=orjson.loads
    )

    return engine


# def get_authors(globals):
#     with globals.sql_session.begin() as session:
#         return session.query(Author).all()
    

# def get_db_properties(globals, session : Session, keys) -> list[Property]:
#     "Given a list of observed property names, extract them from the database and return them as ORM objects"
#     properties = []
#     canonical_properties = {p.name: p for p in globals.canonical_variables}
#     for property_name in keys:

#         # Lookup the canonical variable in the database        
#         canonical_property = session.query(Property).filter_by(name = property_name).one_or_none()

#         if canonical_property is None:
#             if property_name in canonical_properties:
#                 c = canonical_properties[property_name]
#                 canonical_property = Property(
#                     name=c.name,
#                     description=c.description,
#                     unit=c.unit,
#                     url="",
#                 )
#                 session.add(canonical_property)
#             else:
#                 raise RuntimeError(f"A Property (canonical variable) with name={property_name!r} does not exist in the database")

#         properties.append(canonical_property)

#     return properties
