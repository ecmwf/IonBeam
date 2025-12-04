# IonCannon - Synthetic Data Generator for Ionbeam

IonCannon is a configurable synthetic data generator designed for load testing and performance evaluation of the Ionbeam platform. It generates realistic-looking meteorological data with controlled characteristics.

## Features

- **Configurable Station Count**: Generate data from any number of synthetic stations
- **Flexible Measurement Frequency**: Control how often measurements are generated
- **Geographic Bounds**: Define the geographic area where stations are located
- **Controlled Cardinality**: Prevent high cardinality issues by limiting unique metadata values
- **Realistic Data Structure**: Generates data compatible with standard meteorological observations

## Installation

From the monorepo root:

```bash
uv pip install -e data-sources/ioncannon
```

## Configuration

Create a `config.yaml` file based on `config.example.yaml`:

```yaml
# Data source name - must match  scheduler configuration
source_name: "ioncannon"

# Ionbeam client configuration
ionbeam:
  amqp_url: "amqp://guest:guest@localhost:5672/"
  routing_key: "ionbeam.ingestion.ingestV1"
  arrow_store_path: "./data/raw/"

# IonCannon source configuration
ioncannon:
  # Number of synthetic stations to generate
  num_stations: 1000
  
  # Measurement frequency in minutes
  measurement_frequency_minutes: 5
  
  # Geographic bounds for station placement (Central Europe by default)
  min_lat: 47.0
  max_lat: 55.0
  min_lon: 5.0
  max_lon: 15.0
  
  # Cardinality control for metadata variables
  metadata_cardinality: 10
```

## Usage

### As a Standalone Service

Run the IonCannon data source:

```bash
ioncannon --config config.yaml
```

Or using Python module syntax:

```bash
python -m ioncannon --config config.yaml
```

### Integration with 

IonCannon automatically registers with  and responds to scheduled triggers. Configure the scheduler in 's configuration:

```yaml
scheduler:
  sources:
    - name: ioncannon
      schedule: "*/5 * * * *"  # Every 5 minutes
      lookback: 1h
```

## Generated Data

IonCannon generates synthetic meteorological observations with the following structure:

### Canonical Variables
- `temperature`: Air temperature (°C)
- `pressure`: Air pressure (Pa)
- `humidity`: Relative humidity (dimensionless, 0-1)
- `wind_speed`: Wind speed (m/s)

### Metadata Variables
- `station_id`: Unique station identifier (format: SYNTH_####)
- `sensor_type`: Type of sensor (controlled cardinality)
- `location_type`: Location classification (controlled cardinality)

### Coordinate Information
- `observation_timestamp`: Timestamp of observation (UTC)
- `latitude`: Station latitude (degrees north)
- `longitude`: Station longitude (degrees east)

## Performance Tuning

### Station Count
The `num_stations` parameter directly affects the volume of data generated. For load testing:
- Small scale: 100-500 stations
- Medium scale: 500-2000 stations
- Large scale: 2000+ stations

### Measurement Frequency
Lower frequency values generate more data points per time window:
- High frequency: 1-5 minutes
- Medium frequency: 5-15 minutes
- Low frequency: 15+ minutes

### Metadata Cardinality
The `metadata_cardinality` parameter controls how many unique values exist for metadata fields. This is crucial for preventing high cardinality issues in time-series databases:
- Low cardinality: 5-10 unique values
- Medium cardinality: 10-50 unique values
- High cardinality: 50+ unique values (use with caution)

## Use Cases

1. **Load Testing**: Generate high volumes of data to test system performance
2. **Development**: Create test data without external dependencies
3. **Benchmarking**: Compare performance across different configurations
4. **Integration Testing**: Validate data pipeline functionality

## Architecture

IonCannon follows the standard Ionbeam data source pattern:

```
ioncannon/
├── __init__.py          # Package exports
├── __main__.py          # CLI entry point
├── app.py               # Application bootstrap
├── client.py            # IonCannonSource implementation
└── models.py            # Configuration models
```

The source integrates with  through:
- AMQP messaging for ingestion commands
- Arrow format for efficient data transfer
- Trigger-based scheduling for coordinated data generation

## License

See the main repository LICENSE file.