# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

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
