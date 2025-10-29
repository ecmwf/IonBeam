import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import duckdb
import pandas as pd
from fastapi import HTTPException

from .models import StationMetadata

logger = logging.getLogger(__name__)


class LegacyAPIDataService:
    def __init__(
        self,
        projection_base_path: Path,
        metadata_subdir: str = "metadata",
        data_subdir: str = "data",
    ):
        self.projection_base_path = Path(projection_base_path)
        self.metadata_dir = self.projection_base_path / metadata_subdir
        self.data_dir = self.projection_base_path / data_subdir

    def query_stations(
        self,
        external_id: Optional[str] = None,
        station_id: Optional[str] = None,
        platform: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[StationMetadata]:
        try:
            # Build the parquet file path pattern
            if platform:
                metadata_pattern = str(self.metadata_dir / f"platform={platform}" / "metadata.parquet")
            else:
                metadata_pattern = str(self.metadata_dir / "platform=*" / "metadata.parquet")

            # Check if metadata files exist
            if not self.metadata_dir.exists():
                logger.error(f"Metadata directory does not exist: {self.metadata_dir}")
                return []

            # Build WHERE clause conditions
            conditions = []
            params = {}

            if external_id:
                conditions.append("external_id = $external_id")
                params["external_id"] = external_id

            if station_id:
                conditions.append("internal_id = $station_id")
                params["station_id"] = station_id

            if platform:
                conditions.append("platform = $platform")
                params["platform"] = platform

            # Time span overlap filtering
            if start_time or end_time:
                if start_time and end_time:
                    # Check for overlap: station spans overlap with query range
                    conditions.append(
                        "(time_span_start <= $end_time AND time_span_end >= $start_time)"
                    )
                    params["start_time"] = start_time.astimezone(timezone.utc)
                    params["end_time"] = end_time.astimezone(timezone.utc)
                elif start_time:
                    conditions.append("time_span_end >= $start_time")
                    params["start_time"] = start_time.astimezone(timezone.utc)
                elif end_time:
                    conditions.append("time_span_start <= $end_time")
                    params["end_time"] = end_time.astimezone(timezone.utc)

            # Build query
            query = f"""
                SELECT
                    *
                FROM read_parquet('{metadata_pattern}', hive_partitioning=true)
            """

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            logger.debug(f"Executing station metadata query - query={query}, params={params}")

            # Execute query using DuckDB
            with duckdb.connect(":memory:") as con:
                result = con.execute(query, params).df()

            if result.empty:
                logger.info("No stations found matching criteria")
                return []

            # Parse rows into StationMetadata objects
            stations: List[StationMetadata] = []
            
            for idx, row in result.iterrows():
                row_dict = row.to_dict()
                
                try:
                    metadata = StationMetadata(**row_dict)
                    stations.append(metadata)
                except Exception as e:
                    logger.warning(f"Failed to parse metadata row: {e}, row_dict={row_dict}")
                    continue
            
            logger.info(
                f"Station query completed - station_count={len(stations)}, "
                f"filters={{external_id={external_id}, station_id={station_id}, platform={platform}}}"
            )

            return stations

        except Exception as e:
            logger.error(f"Station query failed: {e}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "Failed to query station metadata",
                    "error": str(e),
                },
            )

    def query_observations(
        self,
        platform: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        station_id: Optional[str] = None,
        sql_filter: Optional[str] = None,
    ) -> pd.DataFrame:
        try:
            # Build parquet file pattern
            data_pattern = str(self.data_dir / f"platform={platform}" / "*.parquet")

            # Build WHERE clause conditions
            conditions = []
            params = {}

            if start_time:
                conditions.append("datetime >= $start_time")
                params["start_time"] = start_time.isoformat()

            if end_time:
                conditions.append("datetime <= $end_time")
                params["end_time"] = end_time.isoformat()

            if station_id:
                conditions.append("station_id = $station_id")
                params["station_id"] = station_id

            # Build base query
            query = f"""
                SELECT *
                FROM read_parquet('{data_pattern}', hive_partitioning=true)
            """

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            logger.debug(
                f"Executing observation query - query={query}, params={params}"
            )

            # Execute query
            with duckdb.connect(":memory:") as con:
                result_df = con.execute(query, params).df()

            if result_df.empty:
                raise HTTPException(
                    status_code=404,
                    detail="No data found for the given query.",
                )

            # Apply additional SQL filter if provided
            if sql_filter:
                result_df = self._apply_sql_filter(result_df, sql_filter)

            logger.info(
                f"Observation query completed - row_count={len(result_df)}, "
                f"platform={platform}, time_range=({start_time}, {end_time})"
            )

            return result_df

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Observation query failed: {e}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "Failed to retrieve observational data",
                    "error": str(e),
                },
            )

    def _apply_sql_filter(
        self,
        df: pd.DataFrame,
        sql_filter: str,
    ) -> pd.DataFrame:
        try:
            with duckdb.connect(":memory:") as con:
                # Register the DataFrame as a table
                con.register("data", df)
                
                # Execute the filter query
                query = f"SELECT * FROM data WHERE {sql_filter}"
                filtered_df = con.execute(query).df()

            logger.debug(f"SQL filter applied - filter={sql_filter}, result_rows={len(filtered_df)}")
            return filtered_df

        except Exception as e:
            logger.error(f"SQL filter failed - filter={sql_filter}, error={e}")
            raise HTTPException(
                status_code=400,
                detail=f"Invalid SQL filter: {str(e)}",
            )