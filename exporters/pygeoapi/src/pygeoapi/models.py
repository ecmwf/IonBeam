from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class SpatialExtent(BaseModel):
    bbox: List[float]
    crs: str


class TemporalExtent(BaseModel):
    begin: datetime
    end: datetime
    trs: Optional[str] = None


class Extents(BaseModel):
    spatial: SpatialExtent
    temporal: Optional[TemporalExtent] = None


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
    visibility: Optional[str] = None
    title: str
    description: str
    keywords: List[str]
    links: List[dict]
    extents: Extents
    providers: List[Provider]
