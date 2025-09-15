from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class Link(BaseModel):
    mime_type: str
    title: str
    href: str


class SpatialExtent(BaseModel):
    bbox: List[float]
    crs: str


class TemporalExtent(BaseModel):
    begin: datetime  # RFC3339 datetime string
    end: datetime
    trs: Optional[str]


class Extents(BaseModel):
    spatial: SpatialExtent
    temporal: Optional[TemporalExtent]


class ProviderData(BaseModel):
    source: str


class Provider(BaseModel):
    type: str
    name: str
    data: ProviderData
    id_field: str
    time_field: str
    x_field: str
    y_field: str


class Resource(BaseModel):
    type: str
    visibility: Optional[str]
    title: str
    description: str
    keywords: List[str]
    links: List[Link]
    extents: Extents
    providers: List[Provider]
