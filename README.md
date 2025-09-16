# Ionbeam

**⚠️ PROTOTYPE SOFTWARE** - This is a prototype IoT data ingestion and processing platform for environmental monitoring data. While basic functionality exists, it has not been thoroughly tested in production environments.

## Overview

Ionbeam is an event-driven platform that collects, standardizes, and processes data from multiple IoT environmental monitoring sources including MeteoTracker, NetAtmo, and Sensor.Community.

Key capabilities:
- **Multi-source data ingestion** from various IoT environmental monitoring platforms
- **Real-time data processing** with configurable aggregation windows
- **Standardized data models** following CF (Climate and Forecast) conventions
- **Projection based Event-driven architecture** enabling decoupled, scalable read models tailored to specific query patterns and user needs

## Architecture

```mermaid
%%{init: {
  "flowchart": { "diagramPadding": 16, "nodeSpacing": 40, "rankSpacing": 48, "htmlLabels": true },
  "themeVariables": { "fontSize": "12px" }
}}%%
flowchart LR
    %% External (standalone, outside Core)
    subgraph EXTERNAL[External]
      direction TB
      EXT[IoT APIs<br/>MeteoTracker, NetAtmo, Sensor.Community, etc.]
    end

    %% Core (services + storage kept together)
    subgraph CORE[Ionbeam-next]
      direction TB
      SCHED[Scheduler<br/>Service]

      %% Data Sources (split)
      METEO[Data Source<br/>MeteoTracker]
      NETATMO[Data Source<br/>NetAtmo]
      SCOMM[Data Source<br/>Sensor.Community]

      %% Services
      ING[Ingestion<br/>Service]
      AGG[Dataset<br/>Aggregator]
      ODB[ODB<br/>Projection]
      BUFR[BUFR<br/>Projection]
      PYGEO[PyGeoApi<br/>Projection]

      %% Storage (inside Core)
      FILES[(File Store<br/>Raw Parquet data)]
      INFLUX[(InfluxDB<br/>TSDB)]
      REDIS[(Redis)]
      CANFILES[(File Store<br/>Canonicalised Parquet data)]

      %% Commands (UNCHANGED)
      SCHED -->|StartSourceCommand| METEO
      SCHED -->|StartSourceCommand| NETATMO
      SCHED -->|StartSourceCommand| SCOMM
      METEO -->|IngestDataCommand| ING
      NETATMO -->|IngestDataCommand| ING
      SCOMM -->|IngestDataCommand| ING

      %% HTTP interactions (DATA FLOWS - dotted)
      METEO -.->|HTTP Requests| EXT
      NETATMO -.->|HTTP Requests| EXT
      SCOMM -.->|HTTP Requests| EXT

      %% Data flows (dotted)
      METEO -.->|Write Parquet| FILES
      NETATMO -.->|Write Parquet| FILES
      SCOMM -.->|Write Parquet| FILES
      FILES -.->|Read Parquet| ING
      ING -.->|Write Data Points| INFLUX

      %% Events (UNCHANGED)
      ING -->|DataAvailableEvent| AGG
      AGG -->|DataSetAvailableEvent| ODB
      AGG -->|DataSetAvailableEvent| BUFR
      AGG -->|DataSetAvailableEvent| PYGEO

      %% Other data/service usage (dotted)
      AGG <-.-> |Query Data| INFLUX
      AGG <-.-> |Track Events| REDIS
      AGG -.->|Write Canonicalised Parquet| CANFILES
      CANFILES -.->|Read Parquet data| ODB
      CANFILES -.->|Read Parquet data| BUFR
      CANFILES -.->|Read Parquet data| PYGEO
    end

    %% Outputs
    subgraph OUTPUTS[Outputs]
      direction TB
      ODBF[ODB<br/>Files]
      PARQUET[Parquet<br/>Files]
      BUFRF[BUFR<br/>Files]
      PYGEOF[PyGeoApi<br/>Config + GeoParquet]
    end

    %% Output generation (data flows - dotted)
    ODB -.->|Generate| ODBF
    ODB -.->|Generate| PARQUET
    BUFR -.->|Generate| BUFRF
    PYGEO -.->|Generate| PYGEOF

    %% Styling (nodes only; unchanged)
    classDef service fill:#d0ebff,stroke:#1c7ed6,stroke-width:2px,color:#000
    classDef storage fill:#e9ecef,stroke:#495057,stroke-width:2px,color:#000
    classDef external fill:#fff3bf,stroke:#f59f00,stroke-width:2px,color:#000
    classDef output fill:#d3f9d8,stroke:#37b24d,stroke-width:2px,color:#000

    class SCHED,METEO,NETATMO,SCOMM,ING,AGG,ODB,BUFR,PARQ,PYGEO service
    class INFLUX,REDIS,FILES,CANFILES storage
    class EXT external
    class PARQUET,ODBF,BUFRF,PYGEOF output
```

### Core Ionbeam Services

- **Scheduler Service**: Manages automated data collection using configurable time windows. Triggers source data collection at intervals with optional lag compensation for data availability delays.

- **Ingestion Service**: Standardizes incoming data using CF (Climate and Forecast) conventions and stores it in InfluxDB time-series database. Handles data type coercion, column mapping, and validation of variables.

- **Dataset Aggregation Service**: Creates time-windowed datasets with configurable aggregation periods. Manages out-of-order events, detects data gaps, handles overlapping time spans, and implements subject-to-change windows to prevent processing of recent data that may still be updated.

### Data Sources (Pluggable)

- **IonCannon Source**: Synthetic data generator for load testing and performance validation. Generates data with configurable station counts, measurement frequencies to help testing

- **MeteoTracker Source**: Mobile weather station data across multiple European cities.

- **NetAtmo Source**: NetAtmo weather stations via Met Norway's IoT API.

- **Sensor.Community Source**: Air quality sensor network data from community archive. Processes daily archived CSV files with automatic compression detection and handles missing data scenarios.

### Data Projection Services (Read Models)

- **ODB Projection Service**: Creates ODB format files compatible with ECMWF and numerical weather prediction systems. Maps CF standard names to ODB variable numbers (varno).

- **PyGeoAPI Projection Service**: Generates time-partitioned GeoParquet files for OGC EDR-compliant API servers. Creates monolithic datasets with spatial indexing and automatically updates PyGeoAPI configuration files with temporal and spatial extents.

### Technology Stack

- **Python 3.12+** with asyncio for concurrent processing and streaming data operations
- **FastStream + RabbitMQ** for event-driven messaging architecture with fanout exchanges and dedicated queues
- **InfluxDB 2.x** for high-performance time-series data storage with automatic data retention policies
- **Redis** for event tracking, caching, and coordination between aggregation windows
- **Pydantic** for data validation, serialization, and configuration management
- **PyArrow + Parquet** for efficient columnar data storage and streaming processing
- **CF-Units** for meteorological unit conversions following Climate and Forecast conventions
- **Dependency Injection** using dependency-injector for clean service composition and testing


## Configuration

Ionbeam uses a YAML configuration file to define service settings, data source parameters, and projection outputs. The configuration is structured into several main sections:

### Core Infrastructure Settings

**Message Broker**: Configure RabbitMQ connection for inter-service communication
```yaml
broker:
  url: amqp://guest:guest@rabbitmq:5672/
```

**Database Connections**: Shared settings for InfluxDB and Redis
```yaml
redis:
  redis_url: redis://redis:6379

influxdb_common:
  influxdb_org: ionbeam-example
  influxdb_url: http://influxdb2:8086
  influxdb_token: your-token-here
  influxdb_bucket: ionbeam
```

### Service Configuration

**Dataset Aggregator**: Creates time-windowed datasets and manages data lifecycle
```yaml
dataset_aggregator:
  data_path: ./data                    # Output directory for aggregated datasets
  delete_after_export: false          # Whether to clean up raw data after aggregation
```

### Data Source Settings

**Scheduler**: Define automated data collection patterns using ISO 8601 durations
```yaml
sources:
  scheduler:
    windows:
      - source_name: ioncannon          # Load test data generator
        window_size: "PT5M"             # 5-minute collection windows
        trigger_interval: "PT5M"        # Trigger every 5 minutes
      - source_name: meteotracker
        window_size: "PT1H"             # 1-hour windows
        trigger_interval: "PT1H"        # Fetch every hour
        window_lag: "PT1H"              # 1-hour lag for data availability
```

**Source-Specific Configuration**:

- **IonCannon** (Load Testing): Synthetic data generator for performance testing
```yaml
ioncannon:
  data_path: ./data-raw
```

- **MeteoTracker**: 
```yaml
meteotracker:
  base_url: ''
  token_endpoint: ''
  refresh_endpoint: ''
  timeout: 30
  max_retries: 5
  max_queries: 100
  username: 'your-username'
  password: 'your-password'
  use_cache: false
  data_path: ./data-raw
```

- **NetAtmo**:
```yaml
netatmo:
  base_url: ''
  username: 'your-username'
  password: 'your-password'
  data_path: ./data-raw
```

- **Sensor.Community**: Air quality sensor network data
```yaml
sensor_community:
  base_url: ''
  timeout_seconds: 60
  concurrency: 5
  use_cache: false
  data_path: ./data-raw
```

### Projection Services

**PyGeoAPI Service**: Generates GeoParquet files for OGC EDR API compatibility
```yaml
projections:
  pygeoapi_service:
    input_path: ./data                          # Input aggregated datasets
    output_path: ./data-projections                  # Output GeoParquet files
    config_path: ./pygeoapi/local.config.yml   # PyGeoAPI configuration
```

**ODB Service**: Creates meteorological database format files with variable mapping
```yaml
projections:
  odb_service:
    input_path: ./data
    output_path: ./data-projections
    variable_map:                               # Map CF standard names to ODB variable numbers
      - varno: 108                              # Mean sea level pressure
        mapped_from:
          - standard_name: air_pressure_at_mean_sea_level
            level: 2.0
            method: mean
            period: PT1H
      - varno: 39                               # Air temperature
        mapped_from:
          - standard_name: air_temperature
            level: 2.0
            method: point
            period: PT0S
      # ... additional variable mappings
```

### Configuration Parameters

**Time Windows**: All time-related parameters use ISO 8601 duration format
- `PT5M` = 5 minutes
- `PT1H` = 1 hour  
- `P1D` = 1 day
- `window_lag`: Optional delay to account for data availability latency

**File Paths**: All paths are relative to the application working directory
- `data_path`: Where raw data files are stored
- `input_path`: Source directory for processing
- `output_path`: Destination for generated files

**Data Source Parameters**:
- `timeout_seconds`: HTTP request timeout
- `concurrency`: Maximum parallel requests
- `use_cache`: Enable/disable local caching
- `max_retries`: Number of retry attempts for failed requests

## Usage

### Local Development

For local development with external infrastructure:

```bash
# Start infrastructure services (RabbitMQ, InfluxDB, Redis)
cd deployments/local
docker compose up -d

# Run Ionbeam locally
IONBEAM_CONFIG_PATH=deployments/local/config.example.yaml python -m ionbeam
```

### Docker Compose Deployment

For complete containerized deployment:

```bash
# Deploy all services including Ionbeam
docker compose up -d

# Access services:
# - RabbitMQ Management: http://localhost:15672
# - InfluxDB: http://localhost:8086  
# - PyGeoAPI EDR API: http://localhost/edr/
```

## Event-Driven Architecture

Ionbeam uses an event-driven architecture built on FastStream and RabbitMQ for reliable message passing between services. The system uses both direct queues for commands and fanout exchanges for event broadcasting.

### Message Flow

The messaging architecture follows this pattern:
1. **Commands** are sent to specific queues to trigger actions
2. **Events** are published to fanout exchanges to notify multiple subscribers
3. **Projections** subscribe to dataset events via dedicated queues

### Messaging Topics and Exchanges

| Topic/Exchange | Type | Publisher | Subscriber | Message Type | Description |
|----------------|------|-----------|------------|--------------|-------------|
| `ionbeam.source.netatmo.start` | Queue | Scheduler | NetAtmo Source | `StartSourceCommand` | Triggers NetAtmo data collection |
| `ionbeam.source.ioncannon.start` | Queue | Scheduler | IonCannon Source | `StartSourceCommand` | Triggers load test data generation |
| `ionbeam.source.sensor_community.start` | Queue | Scheduler | Sensor.Community Source | `StartSourceCommand` | Triggers Sensor.Community data collection |
| `ionbeam.source.meteotracker.start` | Queue | Scheduler | MeteoTracker Source | `StartSourceCommand` | Triggers MeteoTracker data collection |
| `ionbeam.ingestion.ingestV1` | Queue | Data Sources | Ingestion Service | `IngestDataCommand` | Commands data ingestion with payload location |
| `ionbeam.data.available` | Fanout Exchange | Ingestion Service | Dataset Aggregator | `DataAvailableEvent` | Broadcasts when new data has been ingested |
| `ionbeam.dataset.available` | Fanout Exchange | Dataset Aggregator | Projection Services | `DataSetAvailableEvent` | Broadcasts when aggregated dataset is ready |

### Projection Service Queues

Projection services use dedicated queues bound to the dataset fanout exchange:

| Queue | Bound to Exchange | Subscriber | Purpose |
|-------|-------------------|------------|---------|
| `ionbeam.data.available.aggregation` | `ionbeam.data.available` | Dataset Aggregator | Processes ingested data into time-windowed parquet datasets |
| `ionbeam.dataset.available.pygeoapi` | `ionbeam.dataset.available` | PyGeoAPI Projection | Generates GeoParquet files for OGC EDR API |
| `ionbeam.dataset.available.odb` | `ionbeam.dataset.available` | ODB Projection | Creates ODB files |

## Data Models

Ionbeam uses a structured event-driven data model with standardized commands and events that flow through the system. All data follows CF (Climate and Forecast) conventions for meteorological variables.

### Core Command Models

**StartSourceCommand** - Triggers data collection from external sources
```python
StartSourceCommand(
    id: UUID,                    # Unique command identifier
    source_name: str,           # Source identifier (e.g., "meteotracker", "netatmo")
    start_time: datetime,       # Collection window start (UTC)
    end_time: datetime,         # Collection window end (UTC)
    use_cache: bool = False     # Whether to use cached data
)
```

**IngestDataCommand** - Commands data ingestion with standardized metadata
```python
IngestDataCommand(
    id: UUID,                           # Unique command identifier
    metadata: IngestionMetadata,        # Data schema and mapping information
    payload_location: pathlib.Path,     # Path to raw Parquet file
    start_time: datetime,               # Data time window start
    end_time: datetime                  # Data time window end
)
```

### Core Event Models

**DataAvailableEvent** - Signals when raw data has been ingested into InfluxDB
```python
DataAvailableEvent(
    id: UUID,                           # Event identifier (matches IngestDataCommand)
    metadata: IngestionMetadata,        # Schema and mapping information
    start_time: datetime,               # Data time window start
    end_time: datetime                  # Data time window end
)
```

**DataSetAvailableEvent** - Signals when aggregated datasets are ready for projections
```python
DataSetAvailableEvent(
    id: UUID,                           # Unique event identifier
    metadata: DatasetMetadata,          # Dataset description and configuration
    dataset_location: pathlib.Path,     # Path to aggregated Parquet file
    start_time: datetime,               # Dataset time window start
    end_time: datetime                  # Dataset time window end
)
```

### Event Flow Example

```python
# 1. Scheduler triggers data collection
StartSourceCommand(
    id=uuid4(),
    source_name="netatmo",
    start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    end_time=datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
)

# 2. NetAtmo source collects data and requests ingestion
IngestDataCommand(
    id=uuid4(),
    metadata=IngestionMetadata(
        dataset=DatasetMetadata(name="netatmo", aggregation_span=timedelta(hours=1)),
        ingestion_map=DataIngestionMap(...)
    ),
    payload_location=Path("/data/netatmo_20240101_1200-1300.parquet"),
    start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    end_time=datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
)

# 3. Ingestion service processes and stores data
DataAvailableEvent(
    id=uuid4(),
    metadata=ingestion_metadata,
    start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    end_time=datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
)

# 4. Dataset aggregator creates time-windowed datasets
DataSetAvailableEvent(
    id=uuid4(),
    metadata=DatasetMetadata(name="netatmo"),
    dataset_location=Path("/datasets/netatmo_20240101T120000_PT1H.parquet"),
    start_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    end_time=datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
)
```
