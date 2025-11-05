import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

from .models import StationMetadata

logger = logging.getLogger(__name__)


class DataServiceError(Exception):
    """Base exception for data service errors."""
    pass


class QueryError(DataServiceError):
    """Raised when a database query fails."""
    pass


class InvalidFilterError(DataServiceError):
    """Raised when an invalid SQL filter is provided."""
    pass


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
            metadata_pattern = str(self.metadata_dir / "**" / "metadata.parquet")

            if not self.metadata_dir.exists():
                logger.error(f"Metadata directory does not exist: {self.metadata_dir}")
                return []

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
            raise QueryError(f"Failed to query station metadata: {e}") from e

    def query_observations(
        self,
        platform: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        station_id: Optional[str] = None,
        sql_filter: Optional[str] = None,
    ) -> pd.DataFrame:
        try:
            query_start = time.perf_counter()
            data_pattern = str(self.data_dir / "**" / "*.parquet")

            params: Dict[str, Any] = {"platform": platform}
            predicates = ["platform = $platform"]

            if start_time:
                params.update(
                    start_time=start_time,
                    start_year=start_time.year,
                    start_month=start_time.month,
                    start_day=start_time.day,
                )
                predicates.append("datetime >= $start_time")
                predicates.append("(year, month, day) >= ($start_year, $start_month, $start_day)")

            if end_time:
                params.update(
                    end_time=end_time,
                    end_year=end_time.year,
                    end_month=end_time.month,
                    end_day=end_time.day,
                )
                predicates.append("datetime <= $end_time")
                predicates.append("(year, month, day) <= ($end_year, $end_month, $end_day)")

            if station_id:
                params["station_id"] = station_id
                predicates.append("station_id = $station_id")

            query = f"""
                SELECT *
                FROM read_parquet('{data_pattern}', hive_partitioning=true)
                WHERE {" AND ".join(predicates)}
            """

            with duckdb.connect(":memory:") as con:
                df = con.execute(query, params).df()

            if df.empty:
                return pd.DataFrame()

            if sql_filter:
                df = self._apply_sql_filter(df, sql_filter)

            # Drop partition columns
            drop_cols = [c for c in ("year", "month", "day") if c in df.columns]
            if drop_cols:
                df = df.drop(columns=drop_cols)

            logger.info(
                "Query completed in %.3fs - rows=%d, platform=%s",
                time.perf_counter() - query_start, len(df), platform
            )
            return df

        except Exception as e:
            logger.error("Observation query failed: %s", e, exc_info=True)
            raise QueryError(f"Failed to retrieve observational data: {e}") from e

    def _apply_sql_filter(
        self,
        df: pd.DataFrame,
        sql_filter: str,
    ) -> pd.DataFrame:
        try:
            with duckdb.connect(":memory:") as con:
                con.register("data", df)
                
                query = f"SELECT * FROM data WHERE {sql_filter}"
                filtered_df = con.execute(query).df()

            logger.debug(f"SQL filter applied - filter={sql_filter}, result_rows={len(filtered_df)}")
            return filtered_df

        except Exception as e:
            logger.error(f"SQL filter failed - filter={sql_filter}, error={e}")
            raise InvalidFilterError(f"Invalid SQL filter: {e}") from e