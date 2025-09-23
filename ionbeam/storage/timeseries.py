import structlog
import warnings
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import AsyncIterator, List

import pandas as pd
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.warnings import MissingPivotFunction

warnings.simplefilter("ignore", MissingPivotFunction) # We pivot locally to offload computation away from influxdb - TODO look into this, is it really any quicker?

logger = structlog.get_logger(__name__)


class TimeSeriesDatabase(ABC):
    """Interface for time series database operations."""
    
    @abstractmethod
    def query_measurement_data(
        self, 
        measurement: str,
        start_time: datetime, 
        end_time: datetime,
        slice_duration: timedelta = timedelta(minutes=10)
    ) -> AsyncIterator[pd.DataFrame]:
        """Query data for a measurement within time range, yielding paginated DataFrames."""
        pass
    
    @abstractmethod
    async def write_dataframe(
        self, 
        record: pd.DataFrame,
        measurement_name: str,
        tag_columns: List[str],
        timestamp_column: str
    ) -> None:
        """Write DataFrame to time series database."""
        pass
    
    @abstractmethod
    async def delete_measurement_data(
        self,
        measurement: str,
        start_time: datetime,
        end_time: datetime
    ) -> None:
        """Delete data for a measurement within time range."""
        pass


class InfluxDBTimeSeriesDatabase(TimeSeriesDatabase):
    """InfluxDB implementation of TimeSeriesDatabase."""
    
    def __init__(self, client: InfluxDBClientAsync, bucket: str, org: str):
        self.client = client
        self.bucket = bucket
        self.org = org
    
    async def query_measurement_data(
        self, 
        measurement: str,
        start_time: datetime, 
        end_time: datetime,
        slice_duration: timedelta = timedelta(minutes=10)
    ) -> AsyncIterator[pd.DataFrame]:
        """Query data with automatic pagination/slicing."""
        query_api = self.client.query_api()
        
        start = start_time
        while start < end_time:
            stop = min(start + slice_duration, end_time)
            
            flux = f"""
                from(bucket: "{self.bucket}")
                |> range(start: {start.isoformat()}, stop: {stop.isoformat()})
                |> filter(fn: (r) => r._measurement == "{measurement}")
            """
            
            logger.info(
                "Querying InfluxDB",
                measurement=measurement,
                start=start.isoformat(),
                stop=stop.isoformat(),
                slice=str(slice_duration),
            )
            
            # Query returns DataFrame or List[DataFrame]
            df_or_list = await query_api.query_data_frame(query=flux, use_extension_dtypes=True)
            
            # Normalize to single DataFrame
            if isinstance(df_or_list, list):
                if df_or_list:  # Only concat if list is not empty
                    df_long = pd.concat(df_or_list, ignore_index=True)
                else:
                    df_long = pd.DataFrame()
            else:
                df_long = df_or_list if df_or_list is not None else pd.DataFrame()
            
            # Only yield non-empty DataFrames
            if not df_long.empty:
                yield df_long
            
            start = stop
    
    async def write_dataframe(
        self, 
        record: pd.DataFrame,
        measurement_name: str,
        tag_columns: List[str],
        timestamp_column: str
    ) -> None:
        """Write DataFrame using shared client."""
        write_api = self.client.write_api()
        await write_api.write(
            bucket=self.bucket,
            org=self.org,
            record=record,
            data_frame_measurement_name=measurement_name,
            data_frame_tag_columns=tag_columns,
            data_frame_timestamp_column=timestamp_column
        )
    
    async def delete_measurement_data(
        self,
        measurement: str,
        start_time: datetime,
        end_time: datetime
    ) -> None:
        """Delete data for a measurement within time range."""
        delete_api = self.client.delete_api()
        await delete_api.delete(
            start=start_time,
            stop=end_time,
            predicate=f'_measurement="{measurement}"',
            bucket=self.bucket,
            org=self.org,
        )
