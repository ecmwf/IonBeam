import json
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator
from uuid import uuid4

import pandas as pd
import pytest

from ionbeam.models.models import (
    DataSetAvailableEvent,
    DatasetMetadata,
)
from ionbeam.projections.bufr.projection_service import BUFRProjectionService, BUFRProjectionServiceConfig


@pytest.fixture
def temp_data_path() -> Generator[Path, None, None]:
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def bufr_service(temp_data_path: Path) -> BUFRProjectionService:
    config = BUFRProjectionServiceConfig(
        input_path=temp_data_path / "input",
        output_path=temp_data_path / "output"
    )
    return BUFRProjectionService(config)


@pytest.fixture
def sample_dataset_metadata() -> DatasetMetadata:
    return DatasetMetadata(
        name="netatmo",
        description="Test NetAtmo dataset",
        aggregation_span=timedelta(hours=1),
        source_links=[],
        keywords=[],
        subject_to_change_window=timedelta(hours=1)
    )


@pytest.fixture
def sample_netatmo_parquet_file(temp_data_path: Path) -> Path:
    """Create a sample parquet file with NetAtmo-style data"""
    # Create realistic NetAtmo data with proper column naming
    df = pd.DataFrame({
        'datetime': pd.date_range('2024-01-01 10:00:00', periods=10, freq='1min', tz='UTC'),
        'station_id': ['netatmo_station_001'] * 10,
        'lat': [52.5] * 10,
        'lon': [13.4] * 10,
        # Use canonical column naming format: {standard_name}__{unit}__{level}__{method}__{period}
        'air_temperature__K__1.5__mean__PT1M': [293.15, 293.25, 293.35, 293.45, 293.55, 293.65, 293.75, 293.85, 293.95, 294.05],
        'air_pressure_at_mean_sea_level__Pa____mean__PT1M': [101325.0] * 10,
        'relative_humidity__%__1.5__mean__PT1M': [65.0, 66.0, 67.0, 68.0, 69.0, 70.0, 71.0, 72.0, 73.0, 74.0],
        'wind_speed__m s-1__10.0__mean__PT10M': [3.5, 3.6, 3.7, 3.8, 3.9, 4.0, 4.1, 4.2, 4.3, 4.4],
        'wind_from_direction__degree__10.0__mean__PT10M': [180.0, 185.0, 190.0, 195.0, 200.0, 205.0, 210.0, 215.0, 220.0, 225.0],
        'rainfall_amount__kg m-2____point__PT1H': [0.0, 0.1, 0.0, 0.2, 0.0, 0.0, 0.3, 0.0, 0.1, 0.0]
    })
    
    # Create input directory and save parquet file
    input_dir = temp_data_path / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    
    parquet_file = input_dir / "netatmo_test_data.parquet"
    df.to_parquet(parquet_file, index=False)
    
    return parquet_file


class TestBUFRProjectionService:
    """Test suite for BUFRProjectionService."""
    
    @pytest.mark.asyncio
    async def test_project_creates_bufr_file(
        self,
        bufr_service: BUFRProjectionService,
        sample_dataset_metadata: DatasetMetadata,
        sample_netatmo_parquet_file: Path,
        temp_data_path: Path
    ) -> None:
        """Test that projection creates a BUFR file from NetAtmo dataset."""
        # Create dataset available event
        event = DataSetAvailableEvent(
            id=uuid4(),
            metadata=sample_dataset_metadata,
            dataset_location=sample_netatmo_parquet_file,
            start_time=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        )
        
        # Process the event
        await bufr_service.project(event)
        
        # Assert output directory was created
        output_dir = temp_data_path / "output"
        assert output_dir.exists()
        assert output_dir.is_dir()
        
        # Assert BUFR file was created
        expected_bufr_file = output_dir / "netatmo_20240101_1000.bufr"
        assert expected_bufr_file.exists()
        assert expected_bufr_file.is_file()
        
        # Assert file has content (not empty)
        assert expected_bufr_file.stat().st_size > 0
        
        # Assert file contains binary data (BUFR files are binary)
        with open(expected_bufr_file, 'rb') as f:
            content = f.read(4)
            # BUFR files typically start with "BUFR" magic bytes
            assert len(content) == 4

    def _run_bufr_dump_json(self, bufr_file: Path) -> list:
        """Run bufr_dump -j and return parsed JSON messages"""
        result = subprocess.run(
            ["bufr_dump", "-j", "a" ,str(bufr_file)],
            capture_output=True,
            text=True,
            check=True
        )

        all = json.loads(result.stdout)
        return all

    @pytest.mark.asyncio
    async def test_bufr_dump_matches_reference(
        self,
        bufr_service: BUFRProjectionService,
        sample_dataset_metadata: DatasetMetadata,
        sample_netatmo_parquet_file: Path,
        temp_data_path: Path
    ) -> None:
        """Test that BUFR dump JSON matches known good reference."""
        
        # Create and process event
        event = DataSetAvailableEvent(
            id=uuid4(),
            metadata=sample_dataset_metadata,
            dataset_location=sample_netatmo_parquet_file,
            start_time=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        )
        
        await bufr_service.project(event)
        
        # Get BUFR file
        output_dir = temp_data_path / "output"
        bufr_file = output_dir / "netatmo_20240101_1000.bufr"
        
        # Get actual BUFR dump
        actual_messages = self._run_bufr_dump_json(bufr_file)
        
        # Load reference (this would be a committed file in real tests)
        reference_messages = self._get_reference_bufr_dump()
        
        # Compare message count
        assert len(actual_messages) == len(reference_messages), \
            f"Expected {len(reference_messages)} messages, got {len(actual_messages)}"
        
        # Compare each message with simple JSON string comparison
        for i, (actual, reference) in enumerate(zip(actual_messages, reference_messages)):
            self._compare_bufr_messages(actual, reference, message_index=i)

    def _get_reference_bufr_dump(self) -> list:
        """Load reference BUFR dump data from fixture file"""
        fixture_file = Path(__file__).parent / "testdata" / "reference_bufr_dump.json"
        with open(fixture_file) as f:
            return json.load(f)

    def _compare_bufr_messages(self, actual: dict, reference: dict, message_index: int):
        """Compare BUFR messages by direct JSON string comparison"""
        # Convert both to JSON strings for exact comparison
        actual_json = json.dumps(actual, sort_keys=True)
        reference_json = json.dumps(reference, sort_keys=True)
        
        assert actual_json == reference_json, \
            f"Message {message_index}: BUFR dump JSON does not match reference.\n" \
            f"Expected: {reference_json}\n" \
            f"Actual: {actual_json}"
