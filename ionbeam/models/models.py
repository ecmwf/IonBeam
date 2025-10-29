from datetime import datetime, timedelta
from typing import List, Optional, Union
from uuid import UUID

import numpy as np
import numpy.typing
from pydantic import BaseModel, ConfigDict


class StartSourceCommand(BaseModel):
    id: UUID
    source_name: str
    start_time: datetime
    end_time: datetime


class Link(BaseModel):
    mime_type: str
    title: str
    href: str


class BaseVariable(BaseModel):
    dtype: np.typing.DTypeLike
    model_config = ConfigDict(
        arbitrary_types_allowed=True, # play nice with dtypes
        frozen=True
    )

# Eventually this can be extended describe the axis of the data
class CanonicalStandard(BaseModel):
    standard_name: str
    level: float
    method: str
    period: str
    model_config = ConfigDict(
        frozen=True
    )
    @classmethod
    def from_canonical_variable(cls, var: "CanonicalVariable") -> "CanonicalStandard":
        return cls(
            standard_name=var.standard_name,
            level=var.level,
            method=var.method,
            period=var.period
        )
    @classmethod
    def from_canonical_name(cls, name:str) -> "CanonicalStandard":
        if not isinstance(name, str) or "__" not in name:
            raise ValueError(f"Invalid canonical name: '{name}'. Must be a string with '__' separators.")

        parts = name.split("__")
        if len(parts) < 2:
            raise ValueError(f"Invalid canonical name: '{name}'. Must have at least standard_name and unit.")

        # Pad to 5 parts
        parts += [""] * (5 - len(parts))
        standard_name, cf_unit, level_str, method, period = parts[:5]

        if not standard_name or not cf_unit:
            raise ValueError(f"Invalid canonical name: '{name}'. standard_name and unit are required.")

        level = float(level_str) if level_str else 0
        method = method or "point"
        period = period or "PT0S"

        return cls(
            standard_name=standard_name,
            level=level,
            method=method,
            period=period
        )

class CanonicalVariable(CanonicalStandard, BaseVariable):
    column: str
    cf_unit: str
    level: float = 0.0
    method: str = 'point'
    period: str = 'PT0S'  # should be timedelta type?
    dtype: np.typing.DTypeLike = "float64" 
    def to_canonical_name(self):
        parts = [
            self.standard_name or "",
            self.cf_unit or "",
            str(self.level) if self.level is not None else "",
            self.method or "",
            self.period or ""
        ]
        return "__".join(parts) # {standard_name}__{unit}__{level}__{method}__{period}

    @classmethod
    def from_canonical_name(cls, name: str) -> "CanonicalVariable":
        if not isinstance(name, str) or "__" not in name:
            raise ValueError(f"Invalid canonical name: '{name}'. Must be a string with '__' separators.")

        parts = name.split("__")
        if len(parts) < 2:
            raise ValueError(f"Invalid canonical name: '{name}'. Must have at least standard_name and unit.")

        # Pad to 5 parts
        parts += [""] * (5 - len(parts))
        standard_name, cf_unit, level_str, method, period = parts[:5]

        if not standard_name or not cf_unit:
            raise ValueError(f"Invalid canonical name: '{name}'. standard_name and unit are required.")

        level = float(level_str) if level_str else 0
        method = method or "point"
        period = period or "PT0S"

        return cls(
            column=name,
            standard_name=standard_name,
            cf_unit=cf_unit,
            level=level,
            method=method,
            period=period
        )


class TimeAxis(BaseVariable):  # Ionbeam depends on these to store and process the data
    from_col: Optional[str] = None
    dtype: Optional[np.typing.DTypeLike] = "datetime64[ns, UTC]"


class LatitudeAxis(BaseVariable):  # Ionbeam depends on these to store and process the data
    standard_name: str
    cf_unit: str
    from_col: Optional[str] = None
    dtype: Optional[np.typing.DTypeLike] = "float64"


class LongitudeAxis(BaseVariable):  # Ionbeam depends on these to store and process the data
    standard_name: str
    cf_unit: str
    from_col: Optional[str] = None
    dtype: Optional[np.typing.DTypeLike] = "float64"

class MetadataVariable(BaseVariable):
    column: str
    dtype: Optional[np.typing.DTypeLike] = "string"  # Override default
    def to_canonical_name(self):
        return self.column

ColumnDefinition = Union[CanonicalVariable, MetadataVariable]


class DataIngestionMap(BaseModel):
    datetime: TimeAxis
    lat: LatitudeAxis
    lon: LongitudeAxis
    canonical_variables: List[CanonicalVariable]
    metadata_variables: List[MetadataVariable]


class DatasetMetadata(BaseModel):
    name: str
    description: str
    aggregation_span: timedelta = timedelta(days=1)
    source_links: List[Link]
    keywords: List[str]
    subject_to_change_window: timedelta = timedelta(hours=0)


class IngestionMetadata(BaseModel):
    dataset: DatasetMetadata
    ingestion_map: DataIngestionMap
    version: int = 1


class IngestDataCommand(BaseModel):
    id: UUID
    metadata: IngestionMetadata
    payload_location: str
    start_time: datetime
    end_time: datetime


class DataAvailableEvent(BaseModel):
    id: UUID
    metadata: IngestionMetadata
    start_time: datetime
    end_time: datetime


class DataSetAvailableEvent(BaseModel):
    id: UUID
    metadata: DatasetMetadata
    dataset_location: str
    start_time: datetime
    end_time: datetime
