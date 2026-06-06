# Ingestion Module Architecture

> **Current scope:** This module was designed for multi-source ingestion from
> live APIs (London transport, environment). On the current tree, the **active
> ingestion method is Data Caterer** (`ingestion/data_caterer/`) for TPC-DI
> benchmark data generation. The London source adapters described below are
> **reference architecture** — the adapter files are not deployed.

## Overview

The `ingestion` module provides data ingestion capabilities for NEXUS with two main paradigms:
- **Batch Processing**: REST API calls, CSV downloads, CSV file ingestion, Parquet ingestion
- **Streaming**: Kafka-based real-time data pipeline, REST API polling, GTFS Realtime feed ingestion

## Directory Structure

```
ingestion/
├── base/               # SHARED infrastructure (reusable across batch/streaming)
│   ├── __init__.py     # Public API exports
│   ├── core.py         # SourceRun, DownloadContext, SourceFailure
│   ├── contracts.py    # RetryPolicy, TimeoutPolicy, RateLimitPolicy, etc.
│   ├── http.py         # HTTP client with retry, rate-limiting, error masking
│   └── utils.py        # Config loading, date utilities, borough centroids
│
├── batch/              # Batch Processing Pipeline
│   ├── __init__.py     # Public API exports
│   ├── api_ingestion.py # REST API ingestion with pagination
│   ├── csv_ingestion.py # CSV file → raw landing zone
│   ├── csv_download_ingestion.py # CSV download from URL → landing zone
│   ├── parquet_ingestion.py # Parquet file/URL → raw landing zone
│   └── common.py       # write_jsonl, read_csv_records
│
├── streaming/           # Streaming Pipeline (Kafka + API + GTFS)
│   ├── __init__.py     # Public API exports
│   ├── kafka_config.py  # Kafka configuration dataclasses
│   ├── producer.py     # Kafka producer (real Kafka + simulated)
│   ├── consumer.py     # Kafka consumer → raw landing zone
│   ├── api_stream.py   # REST API polling stream
│   └── gtfs_realtime.py # GTFS Realtime protobuf feed ingestion
│
├── sources/            # Source Adapters (London-specific)
│   └── __init__.py     # Re-exports from downloaders/sources/
│
├── canonical/          # Data Contracts (shared format)
│   ├── __init__.py
│   ├── envelope.py     # Raw envelope format builder
│   ├── parser.py       # Multi-format parser (JSONL/JSON/CSV)
│   └── writer.py       # Raw envelope writer
│
└── downloaders/        # London Downloader Entry Point
    ├── __init__.py     # Re-exports from base/
    ├── london_downloader.py # CLI entry point
    ├── raw_adapter.py   # Convert published run → raw envelope
    ├── schema_inference.py # Optional schema inference
    ├── validation.py    # Validation & DLQ routing
    └── sources/        # London-specific source adapters
        ├── londonair.py   # London Air Quality API
        ├── openaq.py      # OpenAQ Global AQ API
        ├── ncei.py        # NOAA/NCEI Climate Data
        ├── openmeteo.py   # Open-Meteo Weather/AQ
        ├── realtime.py     # WAQI, OpenWeather, TfL
        └── transport.py    # STATS19, NaPTAN, TfL, DFT
```

## Usage

### Batch Ingestion

```python
# REST API ingestion
from ingestion.batch import ingest_api_records, batch_api_source_run
records = ingest_api_records(url="https://api.example.com/data", api_key="...")

# CSV ingestion
from ingestion.batch import ingest_csv
ingest_csv(dataset="my_dataset", source=Path("data.csv"))

# CSV download from URL
from ingestion.batch import download_csv
download_csv(url="https://example.com/data.csv")
```

### Parquet Ingestion

```python
# Local Parquet ingestion
from ingestion.batch import ingest_parquet
ingest_parquet(dataset="my_dataset", source=Path("data.parquet"))

# Parquet download from URL
from ingestion.batch import ingest_parquet_download
ingest_parquet_download(dataset="my_dataset", url="https://example.com/data.parquet")
```

### Kafka Streaming

```python
# Using configuration
from ingestion.streaming import (
    KafkaConfig,
    STREAM_TOPICS,
    run_producer,
    consume_events,
)

# Producer - publish events to Kafka
result = run_producer(
    source="openaq",
    topic="environment-openaq",
    events=10,
    bootstrap_servers="localhost:29092",
)

# Consumer - consume from Kafka to raw layer
result = consume_events(
    topic="transport-events",
    dataset="transport",
    bootstrap_servers="localhost:29092",
    group_id="nexus-streaming",
    max_messages=100,
)

# Real Kafka with SSL/SASL
kafka_config = KafkaConfig(
    bootstrap_servers="my-kafka-broker:9093",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username="myuser",
    sasl_plain_password="mypassword",
    ssl_cafile="/path/to/ca.crt",
)
```

### API Stream

```python
# Long-running API polling stream
from ingestion.streaming import ApiStreamConfig, run_api_stream

config = ApiStreamConfig(
    source_key="openaq",
    dataset="openaq_locations",
    api_url="https://api.openaq.org/v3/locations",
    api_key="your-api-key",
    poll_interval_seconds=300.0,
    max_iterations=100,
)
result = run_api_stream(config)

# CLI
python -m ingestion.streaming.api_stream --source openaq --dataset aqi --api-url https://api.openaq.org/v3/locations
```

### GTFS Realtime

```python
# GTFS Realtime feed polling
from ingestion.streaming import GTFSRealtimeConfig, run_gtfs_stream

config = GTFSRealtimeConfig(
    source_key="tfl_gtfs",
    dataset="tfl_gtfs_realtime",
    feed_url="https://api.tfl.gov.uk/gtfs/trip-updates",
    feed_type="trip_update",
    poll_interval_seconds=60.0,
)
result = run_gtfs_stream(config)

# CLI
python -m ingestion.streaming.gtfs_realtime --source tfl --dataset transit --feed-url https://api.tfl.gov.uk/gtfs/trip-updates --feed-type trip_update
```

### CLI Usage

```bash
# Producer
python -m ingestion.streaming.producer --source openaq --events 10 --bootstrap-servers localhost:29092

# Consumer
python -m ingestion.streaming.consumer --topic transport-events --dataset transport --bootstrap-servers localhost:29092
```

### Using Base Infrastructure

```python
from ingestion.base import (
    DownloadContext,
    SourceRun,
    SourceFailure,
    request_json,
    load_config,
)
```

### London Data Sources (via downloaders)

> ⚠️ **Legacy — not deployed on current tree.** Source adapters for London
> data sources and the `london_downloader.py` entry point are reference
> implementations only.

```python
# Using the London downloader CLI
python ingestion/downloaders/london_downloader.py --source openmeteo --mode full_demo

# Or import directly
from ingestion.sources import download_openmeteo
from ingestion.base import DownloadContext, SourceRun

context = DownloadContext(...)
run = SourceRun("openmeteo", context, "openmeteo")
download_openmeteo(run, context)
```

## Key Design Decisions

1. **base/ is shared**: Core infrastructure (HTTP client, retry logic, checkpointing) lives in `base/` and is used by both `batch/` and `streaming/`

2. **data_caterer/ is the active generator**: TPC-DI benchmark data is generated via `ingestion/data_caterer/` using Data Caterer or Spark.

3. **canonical/ is the contract**: All ingestion paths write to the same raw envelope format defined in `canonical/`

4. **sources/ and downloaders/ (legacy)**: London-specific source adapters were built for the environment/transport domain model. They are reference architecture — not deployed on the current tree.

5. **streaming/ (inactive on current tree)**: Kafka streaming infrastructure is available for real-time use cases but is not part of the active TPC-DI benchmark flow.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `localhost:29092` |
| `KAFKA_SECURITY_PROTOCOL` | Security protocol | `PLAINTEXT` |
| `KAFKA_SASL_USERNAME` | SASL username | - |
| `KAFKA_SASL_PASSWORD` | SASL password | - |
| `KAFKA_SASL_MECHANISM` | SASL mechanism | `PLAIN` |
| `KAFKA_SSL_CAFILE` | SSL CA certificate | - |
| `KAFKA_SSL_CERTFILE` | SSL certificate | - |
| `KAFKA_SSL_KEYFILE` | SSL key file | - |
| `NEXUS_CONSUMER_GROUP` | Kafka consumer group | `nexus-streaming` |
| `NEXUS_DLQ_TOPIC` | Dead Letter Queue topic | `nexus.dlq` |
| `NEXUS_STREAM_RUN_ID` | Run ID for streaming pipeline | auto-generated |

## Kafka Topics

| Source | Topic | Description |
|--------|-------|-------------|
| transport | transport-events | Traffic and transport events |
| openaq | environment-openaq | Global air quality measurements |
| waqi | environment-waqi | WAQI air quality stations |
| tfl | transport-tfl | TfL line status |
| gtfs | transport-gtfs | GTFS realtime feeds |
| londonair | environment-londonair | London Air Quality Index |
| openmeteo | environment-openmeteo | Open-Meteo weather/AQ |
| openweather | environment-openweather | OpenWeather current weather |
