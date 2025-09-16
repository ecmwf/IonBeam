import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest

from ionbeam.core.constants import LatitudeColumn, LongitudeColumn, ObservationTimestampColumn
from ionbeam.models.models import (
    CanonicalStandard,
    DataSetAvailableEvent,
    DatasetMetadata,
)
from ionbeam.projections.odb.projection_service import ODBProjectionService, ODBProjectionServiceConfig, VarNoMapping


@pytest.fixture
def temp_data_path() -> Generator[Path, None, None]:
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def odb_service(temp_data_path: Path) -> ODBProjectionService:
    config = ODBProjectionServiceConfig(
        input_path=temp_data_path / "input",
        output_path=temp_data_path / "output",
        variable_map=[
            VarNoMapping(
                varno=39,
                mapped_from=[CanonicalStandard(
                    standard_name="air_temperature",
                    level=1.5,
                    method="mean",
                    period="PT1M"
                )]
            ),
            VarNoMapping(
                varno=112,
                mapped_from=[CanonicalStandard(
                    standard_name="wind_speed",
                    level=10.0,
                    method="mean",
                    period="PT10M",
                )]
            ),
            VarNoMapping(
                varno=108,
                mapped_from=[CanonicalStandard(
                    standard_name="air_pressure_at_mean_sea_level",
                    level=1.0,
                    method="mean",
                    period="PT1M"
                )]
            )
        ]
    )
    return ODBProjectionService(config)


@pytest.fixture
def sample_dataset_metadata() -> DatasetMetadata:
    return DatasetMetadata(
        name="test",
        description="Test dataset",
        aggregation_span=timedelta(hours=1),
        source_links=[],
        keywords=["meteotracker", "iot", "data"],
        subject_to_change_window=timedelta(hours=1)
    )


@pytest.fixture
def sample_canonical_parquet_file(temp_data_path: Path) -> Path:
    """Create a sample parquet file with canonical column naming"""
    # Create test data matching the ODB mapping criteria
    temp_col = "air_temperature__K__1.5__mean__PT1M"                           # maps to varno 39
    wind_col = "wind_speed__m s-1__10.0__mean__PT10M"                           # maps to varno 112
    mslp_col = "air_pressure_at_mean_sea_level__Pa__1.0__mean__PT1M"          # maps to varno 110

    df = pd.DataFrame({
        ObservationTimestampColumn: pd.to_datetime(["2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z"], utc=True),
        LatitudeColumn: [50.7, 51.7],
        LongitudeColumn: [7.1, 7.2],
        "station_id": ["A", "B"],
        temp_col: [285.45, np.nan],   # K -> K (no conversion needed)
        wind_col: [5.5, 3.2],         # m s-1 -> m s-1
        mslp_col: [101240.0, np.nan], # Pa -> Pa (no conversion needed)
    })
    
    # Create input directory and save parquet file
    input_dir = temp_data_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    
    parquet_file = input_dir / "test_canonical_data.parquet"
    df.to_parquet(parquet_file, index=False)
    
    return parquet_file


class TestODBProjectionService:
    """Test suite for ODBProjectionService."""
    
    @pytest.mark.asyncio
    async def test_project_creates_odb_with_correct_mapping(
        self,
        odb_service: ODBProjectionService,
        sample_dataset_metadata: DatasetMetadata,
        sample_canonical_parquet_file: Path,
        temp_data_path: Path
    ) -> None:
        """Test that projection creates ODB file with correct canonical column mapping."""
        # Create dataset available event
        event = DataSetAvailableEvent(
            id=uuid4(),
            metadata=sample_dataset_metadata,
            dataset_location=sample_canonical_parquet_file,
            start_time=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2025, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
        )
        
        # Process the event
        await odb_service.handle(event)
        
        # Assert output directory and files were created
        output_dir = temp_data_path / "output" / "ecmwf"
        assert output_dir.exists()
        
        expected_odb_file = output_dir / "test_canonical_data.odb"
        assert expected_odb_file.exists()
        assert expected_odb_file.stat().st_size > 0
        
        expected_parquet_file = output_dir / "test_canonical_data.parquet"
        assert expected_parquet_file.exists()
        
        # Read the debug parquet file to verify mapping
        odb_df = pd.read_parquet(expected_parquet_file)
        
        # Assert expected ODB columns are present
        expected_columns = [
            "expver@desc", "class@desc", "stream@desc", "type@desc", "creaby@desc",
            "reportype@hdr", "obstype@hdr", "codetype@hdr", "groupid@hdr", 
            "statid@hdr", "lat@hdr", "lon@hdr", "date@hdr", "time@hdr",
            "varno@body", "obsvalue@body"
        ]
        
        for col in expected_columns:
            assert col in odb_df.columns, f"Expected column {col} not found in ODB output"
        
        # Assert we have the expected number of observations (temp(A), pressure(A), wind(A), wind(B))
        assert len(odb_df) == 4
        
        # Assert varno mappings are correct
        varnos = set(odb_df["varno@body"].values)
        expected_varnos = {39, 108, 112}  # temp, pressure, and wind speed
        assert varnos == expected_varnos
        
        # Assert station IDs and coordinates are preserved
        station_ids = set(odb_df["statid@hdr"].values)
        assert station_ids == {"A", "B"}
        
        assert 50.7 in odb_df["lat@hdr"].values
        assert 51.7 in odb_df["lat@hdr"].values
        assert 7.1 in odb_df["lon@hdr"].values
        assert 7.2 in odb_df["lon@hdr"].values
        
        # Assert unit conversions (temperature: K -> K, no conversion)
        temp_rows = odb_df[odb_df["varno@body"] == 39]
        assert len(temp_rows) == 1  # only station A has non-NaN temperature
        temp_value = temp_rows["obsvalue@body"].iloc[0]
        assert abs(temp_value - 285.45) < 0.01  # K -> K (no conversion)
        
        # Assert pressure values (Pa -> Pa, no conversion)
        pressure_rows = odb_df[odb_df["varno@body"] == 108]
        assert len(pressure_rows) == 1  # only station A has non-NaN pressure
        pressure_value = pressure_rows["obsvalue@body"].iloc[0]
        assert abs(pressure_value - 101240.0) < 0.01  # Pa -> Pa (no conversion)
        
        # Assert wind speed values unchanged (m/s -> m/s)
        wind_rows = odb_df[odb_df["varno@body"] == 112]
        assert len(wind_rows) == 2
        wind_values = sorted(wind_rows["obsvalue@body"].values)
        assert abs(wind_values[0] - 3.2) < 0.01
        assert abs(wind_values[1] - 5.5) < 0.01
