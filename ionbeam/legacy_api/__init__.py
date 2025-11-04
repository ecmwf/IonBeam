import logging
import sys
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import List

import pandas as pd
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel

from .comparison_v2 import StationsComparisonServiceV2, compare_observations_response, compare_stations_response
from .data_service import InvalidFilterError, LegacyAPIDataService, QueryError
from .models import (
    ApiError,
    DataLimitError,
    OutputFormat,
    StationMetadata,
    StationResponse,
    ValidationError,
)

# Configure logging if not already configured
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
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


class LegacyApiConfig(BaseModel):
    title: str = "Ionbeam Legacy API"
    version: str = "0.1.0"
    root_path: str = "/legacy"
    # DuckDB data layer configuration
    projection_base_path: Path = Path("./projections/ionbeam-legacy")
    metadata_subdir: str = "metadata"
    data_subdir: str = "data"
    # Comparison configuration
    legacy_api_base_url: str | None = "http://ionbeam-ichange.ecmwf-ichange.f.ewcloud.host"
    enable_comparison: bool = True
    comparison_timeout: float = 5.0


def _build_station_response(
    metadata: StationMetadata
) -> dict:
    # Get platform configuration for time indexing
    platform_config = PLATFORM_CONFIGS.get(metadata.platform, {})

    # Use author from metadata with id=0
    author = {
        "id": 0,
        "name": metadata.author
    }

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
    if 'datetime' in df.columns:
        df = df.copy()
        # Parse datetime strings with flexible ISO8601 format handling
        df['datetime'] = pd.to_datetime(df['datetime'], format='ISO8601', utc=True).apply(
            lambda x: x.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        )
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


def create_legacy_router(duckdb_service: LegacyAPIDataService) -> APIRouter:
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
                _build_station_response(metadata)
                for metadata in station_metadata
            ]
            
            comparison_service = getattr(request.app.state, 'comparison_service', None)
            if comparison_service:
                await compare_stations_response(
                    new_stations=stations,
                    request=request,
                    comparison_service=comparison_service,
                )
            else:
                logger.debug("Comparison service not available for stations endpoint")
            
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
                    "text/csv": {"example": "station_id,datetime,temperature,humidity\n1c001400c38e9a8b,2024-01-01T00:00:00Z,15.5,65.2"},
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
        filter: str | None = Query(
            None,
            description="An SQL filter to apply to the retrieved data (DuckDB syntax)",
            example="air_temperature_near_surface > 20",
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
                    detail="start_time must be ISO formatted with a timezone like '2024-01-01T00:00:00Z' or '2024-01-01T00:00:00+00:00'"
                )
            
            if end_time is not None and end_time.tzinfo is None:
                raise HTTPException(
                    status_code=400,
                    detail="end_time must be ISO formatted with a timezone like '2024-01-01T00:00:00Z' or '2024-01-01T00:00:00+00:00'"
                )

            # Legacy behavior: end_time represents the hour boundary and should include the full hour
            # e.g., end_time=2025-10-20T01:00:00Z means include all data up to but not including
            # 2025-10-20T02:00:00Z (latest timestamp: 2025-10-20T01:59:58.011Z)
            adjusted_end_time = end_time + timedelta(hours=1) if end_time else None

            # Extract MARS parameters from query params
            mars_params = {
                k: v for k, v in request.query_params.items()
                if k not in {"format", "filter", "start_time", "end_time", "station_id"}
            }
            mars_request = DEFAULT_MARS_REQUEST | mars_params
            platform = mars_request.get("platform")
            if not platform:
                raise HTTPException(
                    status_code=400,
                    detail="platform parameter is required"
                )

            result_df = duckdb_service.query_observations(
                platform=platform,
                start_time=start_time,
                end_time=adjusted_end_time,
                station_id=station_id,
                sql_filter=filter,
            )
            
            if format == OutputFormat.JSON:
                json_content = _format_to_json(result_df)
                
                comparison_service = getattr(request.app.state, 'comparison_service', None)
                if comparison_service and not result_df.empty:
                    import json
                    observations = json.loads(json_content)
                    await compare_observations_response(
                        new_observations=observations,
                        request=request,
                        comparison_service=comparison_service,
                    )
                
                return Response(
                    content=json_content,
                    media_type="application/json",
                )
            
            elif format == OutputFormat.CSV:
                csv_buffer = _format_to_csv(result_df)
                return StreamingResponse(
                    csv_buffer,
                    media_type="text/csv",
                    headers={
                        "Content-Disposition": "attachment; filename=data.csv"
                    },
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
        except InvalidFilterError as e:
            logger.error(f"Invalid SQL filter: {e}")
            raise HTTPException(
                status_code=400,
                detail="Invalid SQL filter provided",
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


def create_legacy_application(config: LegacyApiConfig | None = None) -> FastAPI:
    app_config = config or LegacyApiConfig()
    
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )

    duckdb_service = LegacyAPIDataService(
        projection_base_path=app_config.projection_base_path,
        metadata_subdir=app_config.metadata_subdir,
        data_subdir=app_config.data_subdir,
    )

    app = FastAPI(
        title=app_config.title,
        version=app_config.version,
        root_path=app_config.root_path,
        description=(
            "IonBeam API"
        ),
    )

    if app_config.enable_comparison and app_config.legacy_api_base_url:
        comparison_service = StationsComparisonServiceV2(
            legacy_base_url=app_config.legacy_api_base_url,
            significant_digits=2,
            timeout=app_config.comparison_timeout,
        )
        app.state.comparison_service = comparison_service

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(create_legacy_router(duckdb_service))

    return app


__all__ = ["LegacyApiConfig", "create_legacy_application", "create_legacy_router"]
