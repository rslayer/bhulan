# Bhulan GPS Data Ingestion Subsystem

## Overview

The Bhulan ingestion subsystem provides a comprehensive solution for accepting GPS data from multiple sources (files, webhooks, streams), normalizing it into a canonical schema, validating data quality, and persisting it to MongoDB.

## Features

- **Multiple Ingestion Sources**
  - File ingestion (CSV, JSON, Excel, Parquet)
  - Webhook ingestion via REST API
  - Stream ingestion (Kafka, MQTT)
  
- **Canonical Schema**
  - Normalized TrackPoint model with validation
  - Support for required and optional GPS fields
  - Automatic unit conversions (speed, altitude)
  
- **Data Quality**
  - Validation rules for coordinates, timestamps, speed
  - Quality flags for suspicious data
  - Error tracking and reporting
  
- **Vendor Adapters**
  - Generic adapter with fuzzy field matching
  - Geotab adapter
  - Samsara adapter
  - Extensible for additional vendors
  
- **Storage Abstraction**
  - MongoDB repository with 2dsphere indexing
  - Deduplication using deterministic hashing
  - Job registry for tracking ingestion operations
  
- **Observability**
  - Structured JSON logging
  - Job status tracking
  - Error sampling

## Architecture

```
bhulan/
├── config/          # Configuration and settings
├── core/            # Core utilities (logging, metrics)
├── storage/         # Storage abstraction (MongoDB)
├── models/          # Data models (canonical schema, vendors)
├── ingestion/       # Ingestion pipelines and sources
└── api/             # FastAPI REST API
```

## Quick Start

### Installation

```bash
# Install with basic dependencies
pip install -e .

# Install with all optional dependencies
pip install -e ".[all]"

# Install specific extras
pip install -e ".[kafka,mqtt,s3,parquet]"
```

### Configuration

Set environment variables or create a `.env` file:

```bash
# MongoDB
MONGO_URI=mongodb://localhost:27017
MONGO_DB_NAME=bhulan

# API
API_HOST=0.0.0.0
API_PORT=8080
API_KEY=your-secret-key

# Stream ingestion (optional)
ENABLE_KAFKA=false
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=gps.raw

ENABLE_MQTT=false
MQTT_BROKER=localhost
MQTT_PORT=1883
MQTT_TOPIC=devices/+/gps
```

### Running the API

```bash
# Start the FastAPI server
python -m bhulan.api.app

# Or with uvicorn directly
uvicorn bhulan.api.app:app --host 0.0.0.0 --port 8080
```

## Usage Examples

### File Ingestion

```python
from bhulan.ingestion.files import ingest_file

# Ingest CSV file (mapping inferred automatically)
result = ingest_file('data/gps_tracks.csv')
print(f"Accepted: {result.accepted}, Rejected: {result.rejected}")

# Ingest with specific vendor
result = ingest_file('data/geotab_export.csv', vendor='geotab')
```

### Webhook Ingestion

```bash
# POST GPS data to webhook endpoint
curl -X POST "http://localhost:8080/ingest/trackpoints?vendor=generic" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-secret-key" \
  -d '[
    {
      "device_id": "TRK-101",
      "timestamp": "2024-05-01T12:00:00Z",
      "lat": 37.7749,
      "lon": -122.4194,
      "speed_kph": 45.0,
      "heading": 90
    }
  ]'
```

### Check Job Status

```bash
# Get ingestion job status
curl "http://localhost:8080/jobs/{ingest_id}"
```

## Data Model

### Canonical TrackPoint Schema

```python
{
  "device_id": str,        # Required: Device identifier
  "ts_utc": datetime,      # Required: Timestamp in UTC
  "lat": float,            # Required: Latitude (-90 to 90)
  "lon": float,            # Required: Longitude (-180 to 180)
  "speed_mps": float,      # Optional: Speed in m/s
  "heading_deg": float,    # Optional: Heading (0-359)
  "alt_m": float,          # Optional: Altitude in meters
  "hdop": float,           # Optional: Horizontal dilution of precision
  "src": str,              # Optional: Source/vendor identifier
  "raw": dict,             # Optional: Original raw data
  "ingest_id": str,        # Ingestion job ID
  "seq_no": int            # Sequence number within batch
}
```

## Validation Rules

- **Coordinates**: Latitude [-90, 90], Longitude [-180, 180]
- **Timestamp**: Between 1970-01-01 and now + 2 days
- **Speed**: Non-negative, capped at 120 m/s (432 km/h)
- **Heading**: 0-359 degrees
- **Deduplication**: Based on (device_id, timestamp, lat, lon) hash

## Vendor Adapters

### Generic Adapter

Supports common field name variations:
- `device_id`: device, unit, vehicle_id, tracker_id, asset_id
- `ts_utc`: timestamp, time, datetime, event_time, ts
- `lat`: lat, latitude
- `lon`: lon, lng, longitude
- `speed_mps`: speed, speed_kph, speed_mph
- `heading_deg`: heading, course, bearing

### Geotab Adapter

Maps Geotab-specific fields:
- `id` → device_id
- `dateTime` → ts_utc
- `speed` (km/h) → speed_mps

### Samsara Adapter

Maps Samsara-specific fields:
- `device.id` → device_id
- `timestamp` (epoch ms) → ts_utc
- `location.lat`, `location.lng` → lat, lon
- `vehicle.speedKph` → speed_mps

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=bhulan --cov-report=html

# Run specific test file
pytest tests/unit/test_validation.py
```

## API Endpoints

- `GET /health/ready` - Health check
- `GET /config` - Get configuration (non-sensitive)
- `POST /ingest/trackpoints` - Ingest GPS data via webhook
- `GET /jobs/{ingest_id}` - Get job status
- `GET /metrics` - Prometheus metrics (if enabled)

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run linter
ruff check bhulan/

# Format code
black bhulan/

# Type checking
mypy bhulan/
```

## License

MIT License - See LICENSE file for details
