![Static Badge](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity/emerging_badge.svg)

> [!WARNING]
> This project is ALPHA and will be experimental for the foreseeable future. Interfaces and functionality are likely to change. DO NOT use this software in any project/software that is operational.

<p align="center">
    <img alt="IonBeam logo, showing a stylized scientific instrument with ion streams." src="https://github.com/ecmwf-projects/iot-ingester/blob/ac8c020bda2a1143d0c4ffb6a29ff58eb0e2c790/ionbeam.png">
</p>

<p align="center">
    <em>⚡️ A library for IoT data wrangling ⚡️</em>
</p>

---

## What is IonBeam?

**IonBeam** is a event-driven platform for **stream based processing of IoT observations**. It allows observational IoT data to be 
1. 🔗 Ingested from many sources: REST APIs, MQTT brokers, file servers etc.
2. 🔄 Transformed, cleaned, combined and aggregated.
3. 💾 Output into multiple storage formats.

**Ionbeam** uses an **event-sourcing** architecture Incoming IoT observations are captured as events, stored, and processed into projections - query-optimized read models tailored to a specific downstream use-case.

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
      ODB[ECMWF<br/>Projection]
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
      AGG -->|DataSetAvailableEvent| PYGEO

      %% Other data/service usage (dotted)
      AGG <-.-> |Query Data| INFLUX
      AGG <-.-> |Track Events| REDIS
      AGG -.->|Write Canonicalised Parquet| CANFILES
      CANFILES -.->|Read Parquet data| ODB
      CANFILES -.->|Read Parquet data| PYGEO
    end

    %% Outputs
    subgraph OUTPUTS[Outputs]
      direction TB
      ODBF[ODB<br/>Files]
      PARQUET[Parquet<br/>Files]
      PYGEOF[PyGeoApi<br/>Config + GeoParquet]
    end

    %% Output generation (data flows - dotted)
    ODB -.->|Generate| ODBF
    ODB -.->|Generate| PARQUET
    PYGEO -.->|Generate| PYGEOF

    %% Styling (nodes only; unchanged)
    classDef service fill:#d0ebff,stroke:#1c7ed6,stroke-width:2px,color:#000
    classDef storage fill:#e9ecef,stroke:#495057,stroke-width:2px,color:#000
    classDef external fill:#fff3bf,stroke:#f59f00,stroke-width:2px,color:#000
    classDef output fill:#d3f9d8,stroke:#37b24d,stroke-width:2px,color:#000

    class SCHED,METEO,NETATMO,SCOMM,ING,AGG,ODB,PARQ,PYGEO service
    class INFLUX,REDIS,FILES,CANFILES storage
    class EXT external
    class PARQUET,ODBF,PYGEOF output
```

### Core Ionbeam Services

- **Scheduler Service**: Manages automated data collection using configurable time windows. Triggers source data collection at intervals with optional lag compensation for data availability delays.

- **Ingestion Service**: Standardizes incoming data using CF (Climate and Forecast) conventions and stores it in InfluxDB time-series database. Handles data type coercion, column mapping, and validation of variables.

- **Dataset Aggregation Service**: Creates time-windowed datasets with configurable aggregation periods. Manages out-of-order events, detects data gaps, handles overlapping time spans, and implements subject-to-change windows to prevent processing of recent data that may still be updated.

### Data Sources

- **IonCannon Source**: Synthetic data generator for load testing and performance validation. Generates data with configurable station counts, measurement frequencies to help testing

- **MeteoTracker Source**: Mobile weather station data across multiple European cities.

- **NetAtmo Source**: NetAtmo weather stations via Met Norway's IoT API.

- **Sensor.Community Source**: Air quality sensor network data from community archive. Processes daily archived CSV files with automatic compression detection and handles missing data scenarios.

### Data Projection Services (Read Models)

- **ODB Projection Service**: Creates ODB format files compatible with ECMWF and numerical weather prediction systems. Maps CF standard names to ODB variable numbers (varno).

- **PyGeoAPI Projection Service**: Generates time-partitioned GeoParquet files for OGC EDR-compliant API service. Creates monolithic datasets with spatial indexing and automatically updates PyGeoAPI configuration files with temporal and spatial extents.

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
  username: ''
  password: ''
  data_path: ./data-raw
```

- **NetAtmo**:
```yaml
netatmo:
  base_url: ''
  username: ''
  password: ''
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
    input_path: ./data                         # Input aggregated datasets
    output_path: ./data-projections            # Output GeoParquet files
    config_path: ./pygeoapi/local.config.yml   # PyGeoAPI configuration
```

**ODB Service**: Creates ODB format files based on variable mapping
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

### Docker-Compose Deployment

For complete containerized deployment:

```bash
# Deploy all services including Ionbeam
docker compose up -d

# Access services:
# - RabbitMQ Management: http://localhost:15672
# - InfluxDB: http://localhost:8086  
# - PyGeoAPI EDR API: http://localhost/edr/
```

### Helm Deployment

```

```

## Event-Driven Architecture

Ionbeam follows an event-driven, CQRS-oriented architecture designed to decouple ingestion, aggregation, and read-based concerns.

Incoming data flows as commands that initiate ingestion processes, while events represent immutable facts like “data has been ingested” or “a dataset is now available.” These events are broadcast so multiple downstream consumers can react independently without introducing tight coupling.

This separation keeps the ingestion pipeline focused on write concerns - namely collecting, validating, and normalizing raw data — while read concerns, like generating specialized formats, are handled by dedicated projection workers. For example, projections can produce GeoParquet files for a PyGeoAPI-backed EDR API or ODB files for domain-specific workflows without impacting ingestion throughput or latency.

By decoupling these responsibilities, Ionbeam is easier to scale, safer to evolve, and inherently auditable. New data sources or projection formats can be added without disrupting existing processes, and the immutable event stream ensures historical state can always be reconstructed when needed.

### Message Flow

The messaging architecture follows this pattern:
1. **Commands** are sent to specific queues to trigger actions
2. **Events** are published to fanout exchanges to notify multiple subscribers
3. **Projections** subscribe to dataset events via dedicated queues

### Messaging Topics, Exchanges and Queues

| Name | Type | Publisher | Subscriber | Message Type | Description |
|------|------|-----------|------------|--------------|-------------|
| `ionbeam.source.netatmo.start` | Queue | Scheduler | NetAtmo Source | `StartSourceCommand` | Triggers NetAtmo data collection |
| `ionbeam.source.ioncannon.start` | Queue | Scheduler | IonCannon Source | `StartSourceCommand` | Triggers load test data generation |
| `ionbeam.source.sensor_community.start` | Queue | Scheduler | Sensor.Community Source | `StartSourceCommand` | Triggers Sensor.Community data collection |
| `ionbeam.source.meteotracker.start` | Queue | Scheduler | MeteoTracker Source | `StartSourceCommand` | Triggers MeteoTracker data collection |
| `ionbeam.ingestion.ingestV1` | Queue | Data Sources | Ingestion Service | `IngestDataCommand` | Commands data ingestion with payload location |
| `ionbeam.data.available` | Fanout Exchange | Ingestion Service | Dataset Aggregator | `DataAvailableEvent` | Broadcasts when new data has been ingested |
| `ionbeam.dataset.available` | Fanout Exchange | Dataset Aggregator | Projection Services | `DataSetAvailableEvent` | Broadcasts when aggregated dataset is ready |
| `ionbeam.data.available.aggregation` | Queue (bound to `ionbeam.data.available`) | Ingestion Service | Dataset Aggregator | — | Processes ingested data into time-windowed parquet datasets |
| `ionbeam.dataset.available.pygeoapi` | Queue (bound to `ionbeam.dataset.available`) | Dataset Aggregator | PyGeoAPI Projection | — | Generates GeoParquet files for OGC EDR API |
| `ionbeam.dataset.available.odb` | Queue (bound to `ionbeam.dataset.available`) | Dataset Aggregator | ODB Projection | — | Creates ODB files |


## Data Models

Ionbeam uses a structured event-driven data model with standardized commands and events that flow through the system.

### Command Models

**StartSourceCommand** - Triggers data collection from external sources
```python
StartSourceCommand(
    id: UUID,                   # Unique command identifier
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

### Event Models

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

# 2. NetAtmo source collects data and initiates ingestion
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
