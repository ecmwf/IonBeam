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

**IonBeam** is an event-driven platform for **stream-based processing of IoT observations**. It provides real-time collection, transformation, and distribution of environmental monitoring data from diverse IoT sources.

The platform implements a message-driven pipeline that:
- 🔗 Ingests observational data from multiple sources (REST APIs, MQTT brokers, file servers)
- 🔄 Transforms and validates observations using CF (Climate and Forecast) conventions
- 💾 Aggregates data into configurable time windows
- 📤 Exports processed data to multiple formats simultaneously (ODB, GeoParquet, APIs)

Built on event-sourcing principles, IonBeam captures incoming observations as immutable events, processed into specialized read models tailored to downstream use cases.

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

## Getting Started

Deploy the complete system using Docker Compose:

```bash
cd deployments/containers
docker compose up -d

# Access services:
# - RabbitMQ Management: http://localhost:15672 (guest/guest)
# - InfluxDB: http://localhost:8086 (admin/adminadmin)
# - PyGeoAPI EDR API: http://localhost:5000
# - Legacy API: http://localhost:8080
# - IonBeam Core Metrics: http://localhost:8000
```

This deployment includes:
- Infrastructure services (RabbitMQ, InfluxDB, Redis)
- IonBeam Core (ingestion, coordination, dataset building)
- Data sources (IonCannon, Acronet, Eumetnet, MeteoTracker, Sensor.Community)
- Exporters (ECMWF/ODB, PyGeoAPI, Legacy API)

## Configuration

Configuration files for the containers deployment are located in [`deployments/containers/config/`](deployments/containers/config/):
- [`ionbeam.yaml`](deployments/containers/config/) - Core service configuration
- Data source configs: `ioncannon.yaml`, `acronet.yaml`, `eumetnet.yaml`, `meteotracker.yaml`, `sensor_community.yaml`
- Exporter configs: `ecmwf-exporter.yaml`, `pygeoapi-exporter.yaml`, `pygeoapi-config.yaml`, `legacy-api.yaml`

Additional configuration examples are available in each component's directory:
- **Core service**: [`ionbeam/config.example.yaml`](ionbeam/config.example.yaml)
- **Data sources**: `data-sources/*/config.example.yaml`
- **Exporters**: `exporters/*/config.example.yaml`

