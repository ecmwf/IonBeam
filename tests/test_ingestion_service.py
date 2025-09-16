import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest

from ionbeam.core.constants import LatitudeColumn, LongitudeColumn
from ionbeam.models.models import (
    CanonicalVariable,
    DataAvailableEvent,
    DataIngestionMap,
    DatasetMetadata,
    IngestDataCommand,
    IngestionMetadata,
    LatitudeAxis,
    LongitudeAxis,
    MetadataVariable,
    TimeAxis,
)
from ionbeam.services.ingestion import IngestionConfig, IngestionService
from ionbeam.storage.timeseries import TimeSeriesDatabase


class MockTimeSeriesDatabase(TimeSeriesDatabase):
    def __init__(self):
        self.write_calls = []
    
    def query_measurement_data(self, measurement, start_time, end_time, slice_duration=None): # type: ignore
        pass
    
    async def write_dataframe(self, record, measurement_name, tag_columns, timestamp_column):
        self.write_calls.append({
            'record': record.copy(),
            'measurement_name': measurement_name,
            'tag_columns': tag_columns,
            'timestamp_column': timestamp_column
        })
    
    async def delete_measurement_data(self, measurement, start_time, end_time):
        pass


@pytest.fixture
def mock_timeseries_db():
    return MockTimeSeriesDatabase()


@pytest.fixture
def ingestion_service(mock_timeseries_db):
    config = IngestionConfig(parquet_chunk_size=1000)
    return IngestionService(config, mock_timeseries_db)


@pytest.fixture
def sample_metadata():
    return IngestionMetadata(
        dataset=DatasetMetadata(
            name="test_dataset",
            description="Test dataset",
            source_links=[],
            keywords=[]
        ),
        ingestion_map=DataIngestionMap(
            datetime=TimeAxis(from_col="timestamp"),
            lat=LatitudeAxis(from_col="latitude", standard_name="latitude", cf_unit="degrees_north"),
            lon=LongitudeAxis(from_col="longitude", standard_name="longitude", cf_unit="degrees_east"),
            canonical_variables=[
                CanonicalVariable(
                    column="temperature",
                    standard_name="air_temperature",
                    cf_unit="deg_C"
                )
            ],
            metadata_variables=[
                MetadataVariable(column="station_id")
            ]
        )
    )


@pytest.fixture
def sample_parquet_file():
    df = pd.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=10, freq='1min', tz='UTC'),
        'latitude': [52.5] * 10,
        'longitude': [13.4] * 10,
        'temperature': [20.0] * 10,
        'station_id': ['test_station'] * 10
    })
    
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
        df.to_parquet(tmp_file.name, index=False)
        yield Path(tmp_file.name)
        Path(tmp_file.name).unlink()


class TestIngestionService:
    @pytest.mark.asyncio
    async def test_ingestion_writes_data_to_timeseries(self, ingestion_service, sample_metadata, sample_parquet_file, mock_timeseries_db):
        """Test ingestion flow with comprehensive canonicalization and column mapping assertions."""
        command = IngestDataCommand(
            id=uuid4(),
            metadata=sample_metadata,
            payload_location=sample_parquet_file,
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 1, tzinfo=timezone.utc)
        )
        
        result = await ingestion_service.handle(command)
        
        # Assert event structure
        assert isinstance(result, DataAvailableEvent)
        assert result.id == command.id
        assert result.metadata == command.metadata
        assert result.start_time == command.start_time
        assert result.end_time == command.end_time
        
        # Assert data was written to timeseries database
        assert len(mock_timeseries_db.write_calls) == 1
        write_call = mock_timeseries_db.write_calls[0]
        
        # Assert measurement name matches dataset name
        assert write_call['measurement_name'] == 'test_dataset'
        
        # Assert timestamp column is correctly identified
        assert write_call['timestamp_column'] == 'timestamp'
        
        # Assert tag columns include metadata variables
        assert write_call['tag_columns'] == ['station_id']
        
        # Assert written dataframe structure and canonicalization
        written_df = write_call['record']
        
        # Check all expected columns are present (covers axis normalization and canonical renaming)
        expected_columns = {'timestamp', LatitudeColumn, LongitudeColumn, 'air_temperature__deg_C__0.0__point__PT0S', 'station_id'}
        assert set(written_df.columns) == expected_columns
        
        # Assert original columns were removed during transformation
        original_columns = {'latitude', 'longitude', 'temperature'}
        for col in original_columns:
            assert col not in written_df.columns
        
        # Assert data integrity and types
        assert len(written_df) == 10  # All rows preserved
        assert written_df[LatitudeColumn].iloc[0] == 52.5
        assert written_df[LongitudeColumn].iloc[0] == 13.4
        assert written_df['air_temperature__deg_C__0.0__point__PT0S'].iloc[0] == 20.0
        assert written_df['station_id'].iloc[0] == 'test_station'
        
        # Assert timestamp processing
        assert pd.api.types.is_datetime64_any_dtype(written_df['timestamp'])
        assert str(written_df['timestamp'].dt.tz) == 'UTC'
        assert written_df['timestamp'].is_monotonic_increasing
