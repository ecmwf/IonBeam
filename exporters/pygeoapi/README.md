# PyGeoAPI GeoParquet Exporter

Exports Ionbeam datasets to GeoParquet format for consumption by PyGeoAPI.

## Features

- Consumes dataset available events from Ionbeam
- Writes time-partitioned GeoParquet files
- Automatically updates PyGeoAPI configuration with temporal extents
- Maintains dataset-specific directories

## Installation

```bash
uv pip install -e exporters/pygeoapi
```

## Configuration

Create a `config.yaml` file based on `config.example.yaml`:

```yaml
exporter_name: "pygeoapi"
dataset_filter:
  - "netatmo"
  - "sensor_community"

ionbeam:
  amqp_url: "amqp://guest:guest@localhost:5672/"
  arrow_store_type: "local_filesystem"
  arrow_store_path: "./data/raw/"

pygeoapi_exporter:
  output_path: "./data/pygeoapi"
  config_path: "./pygeoapi.yaml"
```

## Usage

```bash
pygeoapi-exporter --config config.yaml
```

Or using the module:

```bash
python -m pygeoapi --config config.yaml
```

## Output Structure

Data is organized by dataset:

```
data/pygeoapi/
├── netatmo/
│   ├── 20251010_0000_20251010_0100.parquet
│   ├── 20251010_0100_20251010_0200.parquet
│   └── ...
└── sensor_community/
    ├── 20251010_0000_20251010_0100.parquet
    └── ...
```

Each GeoParquet file contains:
- Point geometries (EPSG:4326)
- Observation timestamp
- All canonical variables
- Unique ID per observation

## PyGeoAPI Integration

The exporter updates the PyGeoAPI configuration file with:
- Resource definitions for each dataset
- Spatial and temporal extents
- Provider configuration pointing to the dataset directory