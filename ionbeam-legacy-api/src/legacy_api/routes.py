# (C) Copyright 2025- ECMWF and individual contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import logging
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import List

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse

from .data_service import LegacyAPIDataService, QueryError
from .models import (
    ApiError,
    DataLimitError,
    OutputFormat,
    StationMetadata,
    StationResponse,
    ValidationError,
)

logger = logging.getLogger(__name__)

# Platform configurations to satisfy legacy API behavior
PLATFORM_CONFIGS = {
    "meteotracker": {"has_time_index": True},
    "acronet": {"has_time_index": False},
    "sensor_community": {"has_time_index": False},
    "netatmo": {"has_time_index": False},
}

# Default MARS request parameters matching the legacy implementation
DEFAULT_MARS_REQUEST = {
    "class": "rd",
    "expver": "xxxx",
    "stream": "lwda",
    "aggregation_type": "by_time",
}


def _build_station_response(metadata: StationMetadata) -> dict:
    # Get platform configuration for time indexing
    platform_config = PLATFORM_CONFIGS.get(metadata.platform, {})

    # Use author from metadata with id=0
    author = {"id": 0, "name": metadata.author}

    mars_start = metadata.time_span_start.astimezone(timezone.utc)
    mars_end = metadata.time_span_end.astimezone(timezone.utc)

    start_date = mars_start.strftime("%Y%m%d")
    end_date = mars_end.strftime("%Y%m%d")

    if start_date != end_date:
        mars_date = f"{start_date}/to/{end_date}/by/1"
    else:
        mars_date = start_date

    # Build mars_selection dict
    mars_selection = {
        "class": "rd",
        "expver": "xxxx",
        "stream": "lwda",
        "aggregation_type": metadata.aggregation_type,
        "platform": metadata.platform,
        "station_id": metadata.internal_id,
        "date": mars_date,
    }

    # Conditionally add time field based on platform configuration
    if platform_config.get("has_time_index"):
        # Round to hour boundaries
        start_hour_rounded = mars_start.strftime("%H") + "00"
        end_hour_rounded = mars_end.strftime("%H") + "00"

        # Only use /to/ pattern if hours differ
        if start_hour_rounded != end_hour_rounded:
            mars_time = f"{start_hour_rounded}/to/{end_hour_rounded}/by/1"
        else:
            mars_time = start_hour_rounded

        if start_date == end_date:
            mars_selection["time"] = mars_time

    return {
        "name": metadata.name or metadata.external_id,
        "description": metadata.description,
        "platform": metadata.platform,
        "external_id": metadata.external_id,
        "internal_id": metadata.internal_id,
        "aggegation_type": metadata.aggregation_type,  # Note: legacy spelling
        "location": {
            "lat": metadata.location_lat,
            "lon": metadata.location_lon,
        },
        "time_span": {
            "start": metadata.time_span_start,
            "end": metadata.time_span_end,
        },
        "authors": [author],
        "mars_selection": mars_selection,
    }


def _normalize_datetime(df: pd.DataFrame) -> pd.DataFrame:
    if "datetime" in df.columns:
        df = df.copy()
        df["datetime"] = pd.to_datetime(
            df["datetime"], format="ISO8601", utc=True
        ).apply(lambda x: x.isoformat(timespec="milliseconds").replace("+00:00", "Z"))
    return df


def _format_to_json(df: pd.DataFrame) -> str:
    df = _normalize_datetime(df)
    return df.to_json(orient="records", double_precision=15)


def _format_to_csv(df: pd.DataFrame) -> BytesIO:
    df = _normalize_datetime(df)
    csv_string = df.to_csv(index=False)
    return BytesIO(csv_string.encode("utf-8"))


def _format_to_parquet(df: pd.DataFrame) -> BytesIO:
    df = _normalize_datetime(df)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer


def create_legacy_router(
    duckdb_service: LegacyAPIDataService, max_time_range_days: int = 31
) -> APIRouter:
    router = APIRouter()

    @router.get(
        "/api/v1/stations",
        tags=["stations"],
        summary="Get Stations",
        description=(
            "Retrieve station metadata based on the provided filters. "
            "Supports filtering by external ID, station ID, platform, and time range."
        ),
        response_description="A list of stations matching the query parameters",
        response_model=List[StationResponse],
        responses={
            200: {
                "description": "Successfully retrieved stations",
                "model": List[StationResponse],
            },
            500: {
                "description": "Internal error",
                "model": ApiError,
            },
        },
    )
    async def get_stations(
        request: Request,
        external_id: str | None = Query(
            None,
            description="The id of the station from whichever source it was ingested from",
            example="meteotracker_station_123",
        ),
        station_id: str | None = Query(
            None,
            description="The id of the station within the IonBeam system",
            example="1c001400c38e9a8b",
        ),
        start_time: datetime | None = Query(
            None,
            description="The start datetime for the data retrieval in ISO 8601 format",
            example="2024-01-01T00:00:00Z",
        ),
        end_time: datetime | None = Query(
            None,
            description="The end datetime for the data retrieval in ISO 8601 format",
            example="2024-12-31T23:59:59Z",
        ),
        platform: str | None = Query(
            None,
            description="The source of the station data",
            example="meteotracker",
        ),
    ) -> List[dict]:
        try:
            station_metadata = duckdb_service.query_stations(
                external_id=external_id,
                station_id=station_id,
                platform=platform,
                start_time=start_time,
                end_time=end_time,
            )

            stations = [
                _build_station_response(metadata) for metadata in station_metadata
            ]

            return stations
        except QueryError as e:
            logger.error(f"Data service query error: {e}")
            raise HTTPException(
                status_code=500,
                detail="Failed to retrieve stations",
            )
        except Exception as e:
            logger.error(f"Unexpected error in get_stations: {e}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail="Failed to retrieve stations",
            )

    @router.get(
        "/api/v1/retrieve",
        tags=["data"],
        summary="Retrieve Data",
        description=(
            "This endpoint retrieves observational data based on the provided filters and date range. "
            "You can specify MARS parameters via query params, an optional SQL-like filter, "
            "and choose the output format (JSON, CSV, or Parquet). "
        ),
        response_description="Data in the requested format (JSON, CSV, or Parquet)",
        responses={
            200: {
                "description": "Successfully retrieved data",
                "content": {
                    "application/json": {
                        "example": [
                            {
                                "station_id": "1c001400c38e9a8b",
                                "datetime": "2024-01-01T00:00:00Z",
                                "temperature": 15.5,
                                "humidity": 65.2,
                            }
                        ]
                    },
                    "text/csv": {
                        "example": "station_id,datetime,temperature,humidity\\n1c001400c38e9a8b,2024-01-01T00:00:00Z,15.5,65.2"
                    },
                    "application/octet-stream": {"description": "Parquet binary data"},
                },
            },
            400: {
                "description": "Invalid request parameters",
                "model": ValidationError,
            },
            403: {
                "description": "Request would return too many data granules",
                "model": DataLimitError,
            },
            500: {
                "description": "FDB or processing error",
                "model": ApiError,
            },
        },
    )
    async def retrieve(
        request: Request,
        format: OutputFormat = Query(
            OutputFormat.JSON,
            description="Output format for the response. Default is JSON. Options are 'json', 'csv', 'parquet'",
            example="json",
        ),
        start_time: datetime | None = Query(
            None,
            description="The start datetime for the data retrieval in ISO 8601 format with timezone",
            example="2024-01-01T00:00:00Z",
        ),
        end_time: datetime | None = Query(
            None,
            description="The end datetime for the data retrieval in ISO 8601 format with timezone",
            example="2024-12-31T23:59:59Z",
        ),
        station_id: str | None = Query(
            None,
            description="The station id for the data retrieval",
            example="1c001400c38e9a8b",
        ),
    ):
        try:
            if start_time is not None and start_time.tzinfo is None:
                raise HTTPException(
                    status_code=400,
                    detail="start_time must be ISO formatted with a timezone like '2024-01-01T00:00:00Z' or '2024-01-01T00:00:00+00:00'",
                )

            if end_time is not None and end_time.tzinfo is None:
                raise HTTPException(
                    status_code=400,
                    detail="end_time must be ISO formatted with a timezone like '2024-01-01T00:00:00Z' or '2024-01-01T00:00:00+00:00'",
                )

            if start_time and end_time:
                time_range_days = (end_time - start_time).days
                if time_range_days > max_time_range_days:
                    raise HTTPException(
                        status_code=403,
                        detail=f"Time range exceeds maximum allowed: {time_range_days} days (max: {max_time_range_days} days). Please request a smaller time span.",
                    )

            # Legacy behavior: end_time represents the hour boundary and should include the full hour
            # e.g., end_time=2025-10-20T01:00:00Z means include all data up to but not including
            # 2025-10-20T02:00:00Z (latest timestamp: 2025-10-20T01:59:58.011Z)
            adjusted_end_time = end_time + timedelta(hours=1) if end_time else None

            # Extract MARS parameters from query params
            mars_params = {
                k: v
                for k, v in request.query_params.items()
                if k not in {"format", "filter", "start_time", "end_time", "station_id"}
            }
            mars_request = DEFAULT_MARS_REQUEST | mars_params
            platform = mars_request.get("platform")
            if not platform:
                raise HTTPException(
                    status_code=400, detail="platform parameter is required"
                )

            result_df = duckdb_service.query_observations(
                platform=platform,
                start_time=start_time,
                end_time=adjusted_end_time,
                station_id=station_id,
            )

            if format == OutputFormat.JSON:
                json_content = _format_to_json(result_df)
                return Response(
                    content=json_content,
                    media_type="application/json",
                )

            elif format == OutputFormat.CSV:
                csv_buffer = _format_to_csv(result_df)
                return StreamingResponse(
                    csv_buffer,
                    media_type="text/csv",
                    headers={"Content-Disposition": "attachment; filename=data.csv"},
                )

            elif format == OutputFormat.PARQUET:
                parquet_buffer = _format_to_parquet(result_df)
                return StreamingResponse(
                    parquet_buffer,
                    media_type="application/octet-stream",
                    headers={
                        "Content-Disposition": "attachment; filename=data.parquet"
                    },
                )
        except QueryError as e:
            logger.error(f"Data service query error: {e}")
            raise HTTPException(
                status_code=500,
                detail="Failed to retrieve observational data",
            )
        except Exception as e:
            logger.error(f"Unexpected error in retrieve: {e}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail="Failed to retrieve observational data",
            )

    return router
