# Ionbeam Client

Python client for ingesting and exporting data with ionbeam.

## What is it?

The ionbeam client is a Python library that provides a simple interface for sending IoT and unconventional data to the ionbeam platform and consuming processed datasets.

## Installation

```bash
pip install ionbeam-client
```

## Configuration

```python
from ionbeam_client import IonbeamClientConfig

config = IonbeamClientConfig(
    amqp_url="amqp://guest:guest@localhost:5672/",
    arrow_store_path="./data/raw/"
)
```

## Data Ingestion

### Basic Ingestion

Use basic ingestion when you have a standalone script that fetches data on its own schedule or in response to external triggers.

```python
import asyncio
from datetime import datetime, timezone
import pyarrow as pa
from ionbeam_client import IonbeamClient, IonbeamClientConfig
from ionbeam_client.models import IngestionMetadata, DatasetMetadata, DataIngestionMap, TimeAxis, LatitudeAxis, LongitudeAxis, CanonicalVariable

config = IonbeamClientConfig(
    amqp_url="amqp://localhost:5672/",
    arrow_store_path="./data/raw/"
)

metadata = IngestionMetadata(
    dataset=DatasetMetadata(
        name="example_weather",
        description="Example weather data",
        source_links=[],
        keywords=["weather"]
    ),
    ingestion_map=DataIngestionMap(
        datetime=TimeAxis(),
        lat=LatitudeAxis(standard_name="latitude", cf_unit="degrees_north"),
        lon=LongitudeAxis(standard_name="longitude", cf_unit="degrees_east"),
        canonical_variables=[
            CanonicalVariable(
                column="air_temperature__degC__2__point__PT0S",
                standard_name="air_temperature",
                cf_unit="degC",
                level=2.0
            )
        ],
        metadata_variables=[]
    )
)

async def generate_batches():
    """Generate sample Arrow RecordBatches."""
    schema = pa.schema([
        ("datetime", pa.timestamp("ns", tz="UTC")),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
        ("air_temperature__degC__2__point__PT0S", pa.float64())
    ])
    
    data = {
        "datetime": [datetime(2024, 1, 1, i, tzinfo=timezone.utc) for i in range(24)],
        "lat": [52.5] * 24,
        "lon": [13.4] * 24,
        "air_temperature__degC__2__point__PT0S": [20.0 + i * 0.5 for i in range(24)]
    }
    
    batch = pa.RecordBatch.from_pydict(data, schema=schema)
    yield batch

async def main():
    async with IonbeamClient(config) as client:
        await client.ingest(
            batch_stream=generate_batches(),
            metadata=metadata,
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2024, 1, 2, tzinfo=timezone.utc)
        )

asyncio.run(main())
```

### Scheduled Ingestion (Trigger Handler)

Use trigger handlers when you want ionbeam to control when your data source runs. This allows ionbeam core to handle scheduling, backfilling, and re-ingestion of data sources it doesn't own. This is optional and not required - you can manage scheduling yourself using cron or other tools.

When using trigger handlers, ionbeam will invoke your handler function with the time window to fetch data for. This centralizes scheduling configuration and enables ionbeam to coordinate backfills or re-ingestion without code changes.

```python
from datetime import datetime

config = IonbeamClientConfig(
    amqp_url="amqp://localhost:5672/",
    arrow_store_path="./data/raw/"
)

client = IonbeamClient(config)

async def fetch_and_ingest(start_time: datetime, end_time: datetime):
    """Handler invoked by ionbeam scheduler with the time window to fetch."""
    async def generate_batches():
        schema = pa.schema([
            ("datetime", pa.timestamp("ns", tz="UTC")),
            ("lat", pa.float64()),
            ("lon", pa.float64()),
            ("air_temperature__degC__2__point__PT0S", pa.float64())
        ])
        
        # Fetch data for the requested time window
        data = await fetch_from_api(start_time, end_time)
        batch = pa.RecordBatch.from_pydict(data, schema=schema)
        yield batch
    
    await client.ingest(
        batch_stream=generate_batches(),
        metadata=metadata,
        start_time=start_time,
        end_time=end_time
    )

# Register handler - source_name must match ionbeam config
client.register_trigger_handler("my_source", fetch_and_ingest)

async def main():
    async with client:
        await asyncio.Event().wait()

asyncio.run(main())
```

## Data Export

Export handlers allow you to subscribe to and consume canonicalized datasets produced by ionbeam. When ionbeam completes aggregating a time window of data for a dataset, it publishes an event that your export handler receives along with a stream of Arrow batches containing the processed data.

Export handlers are used to create materialized views of ionbeam data for specific applications:

- **ODB Generation**: Transform ionbeam data into ECMWF's ODB format for numerical weather prediction
- **Parquet/DuckDB Databases**: Build queryable databases that power APIs or analytical tools
- **Custom Formats**: Export to application-specific formats or external systems

### Basic Export Handler

```python
import pyarrow as pa
from ionbeam_client import IonbeamClient, IonbeamClientConfig
from ionbeam_client.models import DataSetAvailableEvent

config = IonbeamClientConfig(
    amqp_url="amqp://localhost:5672/",
    arrow_store_path="./data/raw/"
)

async def export_handler(event: DataSetAvailableEvent, batch_stream):
    """Process aggregated dataset batches from ionbeam."""
    async for batch in batch_stream:
        df = batch.to_pandas()
        print(f"Received {len(df)} rows from {event.metadata.name}")
        print(f"Time window: {event.start_time} to {event.end_time}")
        # Export to your target system
        await write_to_parquet(df, event.metadata.name)

client = IonbeamClient(config)
client.register_export_handler(
    exporter_name="my_exporter",
    handler=export_handler
)

async def main():
    async with client:
        await asyncio.Event().wait()

asyncio.run(main())
```

### Filtered Export

Filter which datasets your exporter processes by specifying a dataset filter:

```python
async def export_netatmo(event: DataSetAvailableEvent, batch_stream):
    """Only processes netatmo datasets."""
    async for batch in batch_stream:
        df = batch.to_pandas()
        await write_to_database(df)

client.register_export_handler(
    exporter_name="netatmo_exporter",
    handler=export_netatmo,
    dataset_filter={"netatmo", "netatmo_mqtt"}
)
```
