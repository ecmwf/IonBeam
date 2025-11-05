"""
Pydantic models for the Legacy API endpoints.

This module contains all the request and response models for maintaining
the exact API contract from the legacy implementation while using modern
FastAPI patterns.
"""

from datetime import datetime as dt
from datetime import timezone
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

# Internal Models for Parquet Data Structures

class StationMetadata(BaseModel):
    """Station metadata from parquet projections."""
    platform: str
    external_id: str
    internal_id: str
    name: Optional[str] = None
    description: Optional[str] = None
    aggregation_type: str = "by_time"
    time_span_start: dt  # Always datetime64[ns, UTC] from parquet
    time_span_end: dt    # Always datetime64[ns, UTC] from parquet
    location_lat: float
    location_lon: float
    author: str  # The actual author name from the data source
    geometry_wkt: Optional[str] = None
    
    model_config = ConfigDict(extra="allow")  # Allow extra fields from parquet


class OutputFormat(str, Enum):
    """Supported output formats for data retrieval."""
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"


class StationQueryParams(BaseModel):
    """Query parameters for the stations endpoint."""
    external_id: Optional[str] = Field(
        default=None,
        description="The id of the station from whichever source it was ingested from",
        examples=["meteotracker_station_123"]
    )
    station_id: Optional[str] = Field(
        default=None,
        description="The id of the station within the IonBeam system",
        examples=["1c001400c38e9a8b"]
    )
    start_time: Optional[dt] = Field(
        default=None,
        description="The start datetime for the data retrieval in ISO 8601 format",
        examples=["2024-01-01T00:00:00Z"]
    )
    end_time: Optional[dt] = Field(
        default=None,
        description="The end datetime for the data retrieval in ISO 8601 format",
        examples=["2024-12-31T23:59:59Z"]
    )
    platform: Optional[str] = Field(
        default=None,
        description="The source of the station data",
        examples=["meteotracker"]
    )


class RetrieveQueryParams(BaseModel):
    """Query parameters for the retrieve endpoint."""
    format: OutputFormat = Field(
        default=OutputFormat.JSON,
        description="Output format for the response. Default is JSON. Options are 'json', 'csv', 'parquet'",
        examples=["json"]
    )
    start_time: Optional[dt] = Field(
        default=None,
        description="The start datetime for the data retrieval in ISO 8601 format",
        examples=["2024-01-01T00:00:00Z"]
    )
    end_time: Optional[dt] = Field(
        default=None,
        description="The end datetime for the data retrieval in ISO 8601 format",
        examples=["2024-12-31T23:59:59Z"]
    )
    station_id: Optional[str] = Field(
        default=None,
        description="The station id for the data retrieval",
        examples=["1c001400c38e9a8b"]
    )

    @field_validator('start_time', 'end_time')
    @classmethod
    def validate_timezone(cls, v):
        """Validate that datetime has timezone information."""
        if v is not None and v.tzinfo is None:
            raise ValueError(
                "datetime must be ISO formatted with a timezone like "
                "'2024-01-01T00:00:00Z' or '2024-01-01T00:00:00+00:00'"
            )
        return v


class LocationModel(BaseModel):
    """Geographic location model."""
    lat: float = Field(..., description="Latitude")
    lon: float = Field(..., description="Longitude")


class TimeSpanModel(BaseModel):
    start: dt
    end: dt

    @field_validator('start', 'end')
    @classmethod
    def to_utc(cls, v: dt) -> dt:
        return v.astimezone(timezone.utc)

    @field_serializer('start', 'end', when_used='json')
    def ser_iso_utc(self, v: dt) -> str:
        s = v.isoformat(timespec='microseconds')
        return s[:-1] + '+00:00' if s.endswith('Z') else s

class AuthorModel(BaseModel):
    """Author/platform information model."""
    id: int = Field(..., description="Author ID")
    name: str = Field(..., description="Author/platform name")


class MarsSelectionModel(BaseModel):
    """MARS selection parameters for data retrieval."""
    class_: str = Field(alias="class", default="rd", description="MARS class")
    expver: str = Field(default="xxxx", description="MARS experiment version")
    stream: str = Field(default="lwda", description="MARS stream")
    aggregation_type: str = Field(default="by_time", description="Data aggregation type")
    platform: str = Field(..., description="Platform name")
    station_id: str = Field(..., description="Station internal ID")
    date: str = Field(..., description="Date range in MARS format")
    time: Optional[str] = Field(None, description="Time or time range in MARS format (HHMM or HHMM/to/HHMM/by/1). Only included for platforms with time indexing.")

    model_config = ConfigDict(populate_by_name=True)


class StationResponse(BaseModel):
    """Response model for individual station data."""
    name: Optional[str] = Field(None, description="Station name")
    description: Optional[str] = Field(None, description="Station description")
    platform: str = Field(..., description="Data source platform")
    external_id: str = Field(..., description="External source station ID")
    internal_id: str = Field(..., description="Internal IonBeam station ID")
    aggegation_type: str = Field(default="by_time", description="Aggregation type (note: legacy spelling)")
    location: LocationModel = Field(..., description="Station geographic location")
    time_span: TimeSpanModel = Field(..., description="Data availability time span")
    authors: List[AuthorModel] = Field(..., description="List of data authors/platforms")
    mars_selection: MarsSelectionModel = Field(..., description="MARS selection parameters")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "name": "Arenella",
                "description": None,
                "platform": "acronet",
                "external_id": "arenella",
                "internal_id": "a7d22f057bb346b9",
                "aggegation_type": "by_time",
                "location": {"lat": 44.226414, "lon": 9.532785},
                "time_span": {
                    "start": "2025-01-21T12:42:00+00:00",
                    "end": "2025-10-24T09:52:00+00:00"
                },
                "authors": [{"id": 4, "name": "acronet"}],
                "mars_selection": {
                    "class": "rd",
                    "expver": "xxxx",
                    "stream": "lwda",
                    "aggregation_type": "by_time",
                    "platform": "acronet",
                    "station_id": "a7d22f057bb346b9",
                    "date": "20250121/to/20251024/by/1",
                    "time": "1200/to/0900/by/1"
                }
            }
        }
    )


class MarsRequest(BaseModel):
    """MARS request parameters."""
    class_: str = Field(alias="class", default="rd")
    expver: str = Field(default="xxxx")
    stream: str = Field(default="lwda")
    aggregation_type: str = Field(default="by_time")
    platform: Optional[str] = None
    date: Optional[str] = None
    station_id: Optional[str] = None
    time: Optional[str] = None

    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "class": "rd",
                "expver": "xxxx",
                "stream": "lwda",
                "aggregation_type": "tracked",
                "platform": "meteotracker",
                "date": "20241212",
                "station_id": "1c001400c38e9a8b",
                "time": "0735"
            }
        }
    )


class ListResult(BaseModel):
    """Response model for list endpoint results."""
    mars_request: Dict[str, str] = Field(
        ..., 
        description="Parameters used for the MARS request"
    )
    url: str = Field(
        ..., 
        description="The URL to the data file"
    )
    datetime: dt = Field(
        ...,
        description="The datetime of the data"
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "mars_request": {
                    "class": "rd",
                    "expver": "xxxx",
                    "stream": "lwda",
                    "aggregation_type": "tracked",
                    "platform": "meteotracker",
                    "date": "20241212",
                    "station_id": "1c001400c38e9a8b",
                    "time": "0735"
                },
                "url": "/api/v1/retrieve?class=rd&expver=xxxx&stream=lwda&aggregation_type=tracked&platform=meteotracker&date=20241212&station_id=1c001400c38e9a8b&time=0735",
                "datetime": "2024-12-12T07:35:00Z"
            }
        }
    )


class ApiError(BaseModel):
    message: str = Field(..., description="Error message")
    error: str = Field(..., description="Detailed error information")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "message": "Internal Server Error",
                "error": "Unable to fulfill request due to internal server error"
            }
        }
    )


class ValidationError(BaseModel):
    """Error response for validation failures."""
    detail: str = Field(..., description="Validation error details")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "detail": "start_time must be ISO formatted with a timezone like '2024-01-01T00:00:00Z' or '2024-01-01T00:00:00+00:00'"
            }
        }
    )


class DataLimitError(BaseModel):
    """Error response when data request exceeds limits."""
    detail: str = Field(..., description="Data limit error details")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "detail": "This request would return more than 200 data granules, please request a smaller time span repeatedly."
            }
        }
    )

