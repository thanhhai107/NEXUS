# NEXUS

NEXUS is a local-first data lakehouse scaffold for open data. It ingests
batch files and API streams, runs quality and governance checks, moves records
through Raw/Bronze/Silver/Gold layers, and exposes metadata through FastAPI,
Trino, Superset, and Airflow.

The generated source inventory is integrated in this repository under
`assets/source_discovery/`.

## Pipeline Overview

Nexus implements the canonical lakehouse pipeline:

1. **Data Sources** described by domain catalogs in `domains/<domain>/datasets.yml`.
2. **Ingestion patterns**: batch CSV/API, downloaded CSV, REST API, simulated and real Kafka streaming. Airflow orchestrates schedules, retries, backfill and dependencies via DAGs in `orchestration/airflow/dags/`.
3. **Streaming and failure handling**: the Kafka producer retries failed publishes and routes permanent failures to a Dead Letter Queue (Kafka topic `nexus.dlq` plus `runtime/dlq/`). DLQ captures **operational** failures; the Quarantine Zone captures **invalid data records**.
4. **Source governance**: every dataset is exposed as a Source Registry entry (`python -m cli.nexus registry list`) and a Data Contract (`python -m cli.nexus contract show --dataset <name>`) that combines the JSON Schema, quality rules, freshness and ownership.
5. **Validation gate**: `governance/quality/checks.py` runs schema, type, format and Great Expectations rules. Valid records flow to Bronze; invalid records go to Quarantine.
6. **Quarantine Zone**: invalid records are written to `runtime/quarantine/` with the failure reason for triage.
7. **Medallion architecture**: Bronze (`processing/bronze/raw_to_bronze.py`), Silver (`processing/silver/bronze_to_silver.py`), Gold (`transform/dbt/models/gold/`). dbt is the canonical Silver→Gold tool; the Spark `silver_to_gold.py` script remains for ad hoc backfills.
8. **Metadata, governance, lineage**: OpenMetadata catalogs datasets, owners, schemas, quality results and contracts; OpenLineage events from Spark, dbt and audit hooks land in Marquez.
9. **Serving layer**: Trino, FastAPI and Superset consume Gold tables.
## What Is Included

- Batch CSV/API ingestion and simulated Kafka streaming ingestion.
- Transport and Environment domain catalogs, schemas, and quality rules.
- Raw JSONL landing, Spark medallion jobs, dbt model scaffolding, and serving assets.
- Governance audit logs, Great Expectations validation, lineage, schema history, quarantine, quality metrics, and a lightweight governance agent.
- Optional OpenMetadata catalog and OpenLineage backend through Marquez.
- Generated source discovery metadata from `Akapi895/data-bigdata`.
- Optional GCP VM provisioning for Nexus only.

## Repository Map

```text
cli/                  Operational CLI
common/               Shared config loading
config/               Quality, governance, and Spark defaults
domains/              Dataset catalogs, schemas, and quality rules
governance/            Quality checks, policy, lineage, audit, agent logic
ingestion/             Batch and streaming ingestion
infra/docker/          Local Docker Compose stack
infra/terraform/gcp/   Optional GCP VM cluster for Nexus
orchestration/         Airflow DAGs
processing/            Spark Bronze/Silver/Gold jobs
runtime/               Local generated outputs (mirrors /data/ on VM)
assets/samples/        Sample files for each configured dataset (around 10 rows each)
serving/               FastAPI, Trino, and Superset assets
assets/source_discovery/ Generated source inventory and schema metadata
tests/                 Unit tests
transform/             dbt project
```

`runtime/` is generated output and mirrors `/data/` on the VM. Do not commit logs,
raw data, metrics, quarantine files, or synced source-discovery exports from that
directory.

## Runtime Directory Structure (`runtime/`)

The `runtime/` directory (equivalent to `/data/` on VM) follows the Medallion
Architecture with additional pipeline infrastructure:

```text
runtime/
├── lake/                                    # DATA LAKE - Nơi lưu trữ chính
│   ├── bronze/                              # BRONZE - Raw data gốc (append-only)
│   │   └── {dataset}/
│   │       └── run_id={run_id}/
│   │           ├── metadata/                # Checkpoint, profile, request log
│   │           ├── published/               # Published manifest
│   │           ├── raw/                     # Downloaded raw files (same as Bronze raw/)
│   │           └── staging/                  # Temporary files during download
│   │
│   ├── silver/                              # SILVER - Đã clean, validate, envelope
│   │   └── {domain}/
│   │       └── {dataset}/
│   │           └── year={YYYY}/month={MM}/
│   │               └── {dataset}_{run_id}.jsonl
│   │
│   ├── gold/                               # GOLD - Business aggregates (optional)
│   │   └── {domain}/
│   │       └── {dataset}/
│   │           └── year={YYYY}/month={MM}/
│   │
│   └── schemas/                           # SCHEMAS - JSON schemas cho mỗi dataset
│       └── {domain}/
│           └── {dataset}.schema.json
│
├── warehouse/                              # ANALYTICAL ENGINE - Query engines
│   ├── trino/                             # Trino (distributed SQL engine)
│   │   └── catalogs/
│   │       ├── lake.properties            # Bronze/Silver/Gold catalogs
│   │       └── system.properties
│   │
│   ├── minio/                            # MinIO (S3-compatible object store)
│   │   └── data/                         # Optional: nếu dùng object storage
│   │       ├── bronze/
│   │       ├── silver/
│   │       └── gold/
│   │
│   └── postgres/                         # PostgreSQL (metadata catalog)
│       └── data/                         # PostgreSQL data directory
│
├── pipeline/                               # ORCHESTRATION - Pipeline definitions
│   ├── airflow/                          # Apache Airflow
│   │   ├── dags/
│   │   ├── logs/
│   │   └── config/
│   │
│   └── spark/                            # Apache Spark
│       ├── conf/
│       ├── jars/
│       └── logs/
│
├── staging/                               # STAGING - Files đang download
│   └── {dataset}/
│       └── {run_id}/
│           └── (temporary files)
│
├── tmp/                                  # TMP - Temp files (dọn tự động)
│
├── logs/                                 # LOGS - Application logs
│   └── {service}/
│       ├── downloader/
│       ├── pipeline/
│       └── airflow/
│
├── metrics/                              # METRICS - Prometheus metrics
│
├── dlq/                                  # DLQ - Dead Letter Queue (shared)
│   └── {domain}/
│       └── {dataset}/
│           └── {error_type}/
│               └── {timestamp}.json
│
└── quarantine/                           # QUARANTINE - Invalid records (shared)
    └── {domain}/
        └── {dataset}/
            └── {batch_id}/
                └── records.jsonl
```

### Layer Descriptions

| Layer | Purpose | Transform? | Schema? |
| --- | --- | --- | --- |
| **Bronze** | Lưu raw gốc từ source | Không | Không |
| **Silver** | Envelope wrap, validate, clean | Có | Có |
| **Gold** | Business aggregates | Có | Có |

### BRONZE Layer - Raw Data Structure

```
runtime/lake/bronze/{dataset}/
└── run_id={run_id}/
    ├── metadata/                    # Metadata của run này
    │   ├── run_manifest.json     # Tổng hợp: profile + checkpoint + source config + coverage
    │   ├── checkpoint.json       # Resume state: chunks completed/failed/skipped
    │   ├── request_log.jsonl    # Audit log: từng HTTP request (append-only)
    │   └── inferred_schema.json # Schema được infer từ data (optional)
    │
    ├── published/                   # Published manifest
    │   └── published_manifest.json  # Mark đã publish với checksums
    │
    ├── raw/                        # FILES GỐC TẢI VỀ - Source format
    │   ├── entity=average_annual_daily_flow/   # Partitioned by entity
    │   │   └── year=2024/
    │   │       └── *.csv, *.json          # Files gốc từ API
    │   │
    │   ├── group=latest_final_year/        # Partitioned by data group
    │   │   └── table=collisions/
    │   │       └── period=2024/
    │   │           └── stats19_collisions_2024.csv
    │   │
    │   ├── date=2026-05-27/               # Partitioned by date (realtime data)
    │   │   └── hour=14/
    │   │       └── status.json
    │   │
    │   ├── snapshot=current/               # Snapshot data (không có date partition)
    │   │   └── london_journeys.csv
    │   │
    │   └── metadata/                      # Metadata files riêng (site lists, species)
    │       ├── health_advice.json
    │       └── species.json
    │
    └── staging/                    # FILES ĐANG XỬ LÝ - Tạm thời
        ├── entity=average_annual_daily_flow/
        ├── group=latest_final_year/
        └── date=2026-05-27/
```

**BRONZE File Types:**

| File/Folder | Mục đích |
|-------------|----------|
| `run_manifest.json` | **Tổng hợp**: profile + checkpoint + source config + coverage (thay thế 4 file cũ) |
| `checkpoint.json` | Resume state: chunks đã completed/failed/skipped |
| `request_log.jsonl` | Audit log: từng HTTP request (append-only) |
| `inferred_schema.json` | Schema được tự động infer từ dữ liệu thực tế |
| `published_manifest.json` | Mark data đã publish với checksums |
| `raw/*.csv, *.json` | Files gốc từ API - giữ nguyên format, không transform |
| `staging/*` | Files đang download - sẽ move sang `raw/` khi complete |
| `raw/metadata/*` | Metadata riêng của source (site lists, species codes) |

**BRONZE Partitioning Patterns:**

| Pattern | Dùng cho | Ví dụ |
|---------|----------|-------|
| `entity=X/` | Multiple entity types trong 1 dataset | DFT: count_points, average_annual_daily_flow |
| `group=X/` + `table=Y/` | Stats19: final/provisional groups, collisions/vehicles/casualties | |
| `date=X/` + `hour=Y/` | Realtime data (refresh mỗi giờ) | TfL, LondonAir, WAQI |
| `year=X/` | Yearly data files | DFT, Stats19 |
| `site=X/` | Multiple monitoring sites | LondonAir species per site |
| `snapshot=current/` | Single snapshot không có time partition | London Journeys, NaPTAN |

### SILVER Layer - Normalized Data Structure

```
runtime/lake/silver/{dataset}/
└── {dataset}_{run_id}.jsonl    # 1 FILE CHÍNH chứa tất cả records
```

**Ví dụ:** `silver/stats19_collisions/stats19_collisions_test011.jsonl` (1.2 GB)

**SILVER Record Structure (Envelope Format):**

```json
{
  // === NEXUS METADATA HEADERS (prefix _nexus_) ===
  "_nexus_ingestion_type": "download",      // download | api | stream | csv
  "_nexus_source_id": "stats19_collisions",   // Dataset ID
  "_nexus_source_key": "stats19",             // Source key (short name)
  "_nexus_source_type": null,                 // Source type
  "_nexus_dataset_id": "stats19_collisions",  // Dataset ID (same as source_id)
  "_nexus_run_id": "test011",                 // Run ID - để track lineage
  "_nexus_chunk_id": "stats19:latest_final_year:casualties:2024", // Chunk ID - partition info
  "_nexus_record_id": "1fecd06205817ef...",   // SHA256 hash - deduplication key
  "_nexus_entity_key": null,                  // Entity key (optional grouping)
  "_nexus_event_time": null,                  // Event timestamp (nullable)
  "_nexus_ingested_at": "2026-05-27T11:24:37.385855+00:00",  // Ingest timestamp
  "_nexus_published_at": "2026-05-27T11:24:37.348067Z",      // Publish timestamp
  "_nexus_schema_version": null,              // Schema version
  "_nexus_trace_id": "08a3275a-...",         // Trace ID cho debugging
  "_nexus_runtime_version": "raw-envelope-v1", // Envelope version
  "_nexus_source_path": "D:\\...\\stats19_casualties_2024.csv", // Original file path
  "_nexus_source": "stats19_collisions",     // Source name
  "_nexus_dataset": "stats19_collisions",    // Dataset name
  
  // === PAYLOAD - DỮ LIỆU GỐC ===
  "payload": {
    // Các fields gốc từ source file
    "collision_index": "2024991534042",
    "collision_year": "2024",
    "casualty_severity": "3",
    ...
  }
}
```

**SILVER Fields Explained:**

| Field Group | Field | Mục đích |
|-------------|-------|----------|
| **Ingestion** | `_nexus_ingestion_type` | Loại ingestion: download, api, stream, csv |
| **Identity** | `_nexus_source_id`, `_nexus_source_key` | Identifiers của source |
| **Lineage** | `_nexus_run_id`, `_nexus_chunk_id` | Track data từ đâu đến |
| **Deduplication** | `_nexus_record_id` | SHA256 hash của record - tránh duplicate |
| **Timestamps** | `_nexus_ingested_at`, `_nexus_published_at` | Khi nào được ingest/publish |
| **Traceability** | `_nexus_trace_id`, `_nexus_source_path` | Debugging và audit |
| **Data** | `payload` | Dữ liệu gốc được wrap trong envelope |

**Tại sao dùng Envelope Pattern?**

1. **Lineage đầy đủ**: Biết record đến từ source nào, run nào, chunk nào
2. **Deduplication**: `_nexus_record_id` cho phép deduplicate chính xác
3. **Audit**: Timestamps và trace_id cho debugging
4. **Schema Evolution**: `_nexus_schema_version` track schema changes
5. **Multi-source**: Một file có thể chứa data từ nhiều sources

### So sánh Bronze vs Silver

| Aspect | Bronze | Silver |
|--------|--------|--------|
| **Format** | Source format (CSV, JSON) | JSONL với envelope |
| **Transform** | Không | Normalize, clean, wrap |
| **Schema** | Không (raw) | Có (inferred + validated) |
| **Partitioning** | Multiple files/folders | 1 file chính + partitions |
| **Metadata** | Limited | Full lineage headers |
| **Deduplication** | Không | Có (_nexus_record_id) |
| **Use case** | Audit, reprocess | Analytics, serving |

### Directory Purposes

- **`lake/bronze/`**: Raw files gốc từ downloader, không transform, append-only
  - `metadata/`: Checkpoint, profile, request log
  - `published/`: Published manifest
  - `raw/`: Downloaded files (source format)
  - `staging/`: Temporary files during download
- **`lake/silver/`**: Records đã được wrap envelope với metadata, validated
- **`lake/gold/`**: Business-level aggregates cho analytics/BI
- **`lake/schemas/`**: JSON Schema definitions cho từng dataset
- **`warehouse/`**: Query engines (Trino, MinIO, PostgreSQL) configs
- **`pipeline/`**: Orchestration (Airflow DAGs, Spark configs)
- **`staging/`**: Temporary files đang trong quá trình download
- **`dlq/`**: Dead Letter Queue cho operational failures
- **`quarantine/`**: Invalid records cần triage

## Local Setup

```powershell
Copy-Item .env.example .env
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
docker compose --env-file .env -f infra/docker/docker-compose.yml up -d
```

Local service URLs:

| Service | URL |
| --- | --- |
| FastAPI | <http://localhost:8000/docs> |
| Airflow | <http://localhost:8080> |
| Trino | <http://localhost:8085> |
| Superset | <http://localhost:8088> |
| MinIO Console | <http://localhost:9001> |
| Kafka bootstrap | `localhost:29092` |

Optional metadata stack URLs after starting the `metadata` profile:

| Service | URL |
| --- | --- |
| OpenMetadata | <http://localhost:8585> |
| OpenMetadata Ingestion Airflow | <http://localhost:8090> |
| Marquez UI | <http://localhost:3000> |
| Marquez OpenLineage API | <http://localhost:5000/api/v1/lineage> |

## Source Registry & Data Contracts

The Source Registry exposes every dataset together with derived ingestion method
and update frequency. Data Contracts bundle the schema, required columns,
primary keys, freshness column and quality thresholds for one dataset.

```powershell
python -m cli.nexus registry list
python -m cli.nexus registry show --dataset us_accidents
python -m cli.nexus contract show --dataset openaq_measurements
```

The registry derives `ingestion_method` from `source_type` and
`update_frequency` from `poll_seconds`/`freshness_hours`. Override either field
on a dataset entry in `domains/<domain>/datasets.yml` when needed.

## Data Source Catalog

The current source set is split by how the pipeline should operate on it:

- **Batch**: one-off or slow-changing files/snapshots. Good for backfill,
  reference tables, and historical fact tables.
- **Mini-batch**: API pulls over bounded windows, pages, sensors, stations, or
  grid cells. Good for incremental Bronze loads and Silver normalization.
- **Stream / polling**: frequent snapshots from live APIs. These land as raw
  snapshots/events and need event-time, deduplication, and watermark handling in
  later phases.
- **Reference / fallback**: sample or synthetic datasets retained for local
  demos, compatibility, or tests.

### Implemented Sources

These sources have downloader and/or producer code in this repository and can be
selected with `python scripts/download_data.py --source <source_key>` unless the
notes say they are producer-only.

| Source key | Dataset / output | Domain | Mode | Access and raw format | Cadence | Meaning for downstream phases |
| --- | --- | --- | --- | --- | --- | --- |
| `stats19` | `stats19_collisions` plus raw `collisions`, `vehicles`, `casualties` files | Transport | Batch | GOV.UK / DfT CSV downloads discovered from the road-safety open-data page | Yearly or ad hoc backfill | Historical road-collision facts. Use as the main road-safety base for Silver collision/vehicle/casualty tables and Gold safety summaries. |
| `naptan` | `naptan_stops` | Transport | Batch reference | DfT NaPTAN API CSV for ATCO area `490` | Slow-changing snapshot | Public-transport stop reference data. Join TfL arrivals and transport events by stop / NaPTAN identifiers. |
| `london_journeys` | `london_journeys` | Transport | Batch aggregate | London Datastore CSV | Periodic snapshot | Aggregate passenger journeys by mode and period. Use for long-term demand context, not event-level movement. |
| `dft` | `dft_road_traffic` | Transport | Mini-batch API | DfT Road Traffic REST API, paginated JSONL | Historical backfill by year | London count points and AADF traffic volumes. Join with road safety and noise/road exposure work. |
| `tfl_line_status` | `tfl_line_status` | Transport | Stream / polling | TfL Unified API JSON (`/Line/{ids}/Status`, plus route/disruption endpoints), optional `TFL_API_KEY` | 300s | Operational status per TfL line, severity code, and disruption reason. Useful for real-time service health and disruption features. |
| `tfl_arrivals` | `tfl_arrivals` | Transport | Stream / polling | TfL Unified API JSON (`/StopPoint/{stopId}/Arrivals`), optional `TFL_API_KEY` | 60s | Vehicle arrival predictions at selected stops. Deduplicate downstream on `vehicleId + lineId + expectedArrival`. |
| `tfl` | `tfl_transport` raw combined snapshot | Transport | Stream / polling wrapper | TfL Unified API JSON for line status and arrivals | Mixed 60s/300s depending on caller | Convenience wrapper for combined TfL snapshots; prefer `tfl_line_status` and `tfl_arrivals` for explicit phase work. |
| `openaq` | `openaq_measurements` | Environment | Mini-batch API | OpenAQ v3 API JSON/JSONL, requires `OPENAQ_API_KEY` | Sensor/date-window backfill | Air-quality locations, sensors, parameters, and hourly measurements. Strong source for pollutant time series and station metadata. |
| `londonair` | `londonair_monitoring` | Environment | Hybrid mini-batch + polling | LondonAir JSON endpoints, no key | Metadata + hourly/daily latest + historical site windows | London-specific monitoring sites, species metadata, AQI indexes, health advice, and site time series. Good local authority/station context. |
| `openmeteo` | `openmeteo_air_quality` | Environment | Batch / mini-batch API | Open-Meteo air-quality and archive APIs, JSON | Date-range backfill by borough centroid | Historical weather and air-quality features for each London borough centroid. Useful as exogenous features for transport and air-quality models. |
| `openmeteo_historical_weather` | `openmeteo_historical_weather` | Environment | Batch / mini-batch API | Open-Meteo archive API CSV over generated bbox grid points | Date-range backfill by grid batch | High-coverage weather history over Greater London grid cells. Useful for spatial feature engineering before Silver/Gold joins. |
| `ukair_air_quality_archive` | `ukair_air_quality_archive` | Environment | Batch archive | UK-AIR flat-file site pages, discovered CSV downloads | Annual/site archive | DEFRA UK-AIR historical site pollutant CSVs. Useful for long-range air-quality backfill and cross-checking LondonAir/OpenAQ coverage. |
| `ncei` | `ncei_cdo_climate` | Environment | Mini-batch API | NOAA/NCEI CDO API JSON, requires `NCEI_API_TOKEN` | Monthly windows by station | Daily climate observations for selected London-area stations. Useful for weather/climate enrichment and validation. |
| `waqi` | `waqi_air_quality` | Environment | Stream / polling | WAQI API JSON, requires `WAQI_API_TOKEN` | 300s | Live AQI station feed. Use for current air-quality snapshots and streaming quality checks. |
| `openweather` | `openweather_current` | Environment | Stream / polling | OpenWeather API JSON, requires `OPENWEATHER_API_KEY` | 300s | Current weather, forecast, day summary, and air-pollution snapshots by borough centroid. Useful for live context features. |
| `transport` | `transport_events` | Transport | Reference / producer-only fallback | Synthetic producer events unless `TRANSPORT_EVENTS_API_URL` is configured; no downloader source | Demo stream | Local streaming fallback for Kafka, DLQ, and quality checks when no real transport API is configured. |
| `gtfs` | `gtfs_realtime_events` | Transport | Reference / producer-only | GTFS Realtime URL from `GTFS_REALTIME_URL` when configured | 30s target | Placeholder wrapper for a future GTFS Realtime feed. Not in default London download groups. |

### New Sources Traced From The Uncommitted Worktree

`git status` currently shows the following newly added or expanded source work:

| Source | Evidence in files | Current status | Notes |
| --- | --- | --- | --- |
| `ukair_air_quality_archive` | `ingestion/sources/ukair.py`, downloader registry, `config/download_defaults.yml` | Implemented downloader | Discovers UK-AIR site CSV links and downloads archive files without reshaping. |
| `openmeteo_historical_weather` | `ingestion/sources/openmeteo_historical_weather.py`, downloader registry, `config/download_defaults.yml` | Implemented downloader | Generates bbox grid points and downloads Open-Meteo archive CSV batches. |
| `tfl_line_status` | `ingestion/sources/tfl.py`, streaming config, transport dataset/schema/quality/semantic files | Implemented downloader + producer config | TfL API key is optional; default line ids match current TfL line naming. |
| `tfl_arrivals` | `ingestion/sources/tfl.py`, streaming config, transport dataset/schema/quality/semantic files | Implemented downloader + producer config | Default stops are verified live: King's Cross, Waterloo, and Trafalgar Square bus stop. |
| `tfl_live_traffic_disruptions` | `config/download_defaults.yml` | Planned only | Config captures candidate TfL road-disruption endpoints; no adapter is wired into `SOURCE_REGISTRY` yet. |
| `tfl_bikepoint_occupancy` | `config/download_defaults.yml` | Planned only | Candidate BikePoint/cycle-hire source; no downloader yet. |
| `ea_hydrology_rainfall_river` | `config/download_defaults.yml` | Planned only | Candidate Environment Agency hydrology/rainfall source; no downloader yet. |
| `defra_noise_mapping` | `config/download_defaults.yml` | Planned only | Candidate batch geospatial noise source; no downloader yet. |

### Source Groups

The default source groups in `config/download_defaults.yml` are:

| Group | Sources | Intended use |
| --- | --- | --- |
| `core_historical` | `openmeteo`, `naptan`, `london_journeys`, `dft`, `ncei`, `stats19`, `openaq`, `londonair` | Main historical/reference lake bootstrap. |
| `latest_update` | `openmeteo`, `naptan`, `london_journeys`, `dft`, `ncei`, `openaq`, `londonair` | Smaller refresh-oriented historical pull. |
| `realtime_snapshot` | `waqi`, `openweather`, `tfl_line_status`, `tfl_arrivals` | One-shot current snapshot for live sources. |
| `realtime_polling` | `tfl_line_status`, `tfl_arrivals` | Repeated TfL polling jobs. |
| `expanded_historical` | `londonair`, `ukair_air_quality_archive`, `openmeteo`, `openmeteo_historical_weather`, `openaq`, `ncei`, `dft`, `stats19` | Wider batch/mini-batch backfill set for data expansion work. |

## Dead Letter Queue

The DLQ stores operational failures (Kafka publish/consume failures, job
crashes, timeouts). Bad data records continue to flow to `runtime/quarantine/`.

```powershell
python -m cli.nexus dlq list
python -m cli.nexus dlq replay --target stdout --category streaming_publish_failed
python -m cli.nexus dlq replay --target kafka --topic transport-events
```

Local DLQ files live under `runtime/dlq/`; when
`NEXUS_GOVERNANCE_STORAGE=postgres`, events stream into the governance Postgres
table instead. Airflow exposes the same flow via the `nexus_dlq_replay` DAG.

## Streaming Consumer & Reprocessing

- `ingestion/streaming/consumer.py` reads Kafka events, lands them in `runtime/lake/bronze/<dataset>/run_id=<run_id>/raw/`, and auto-converts to Silver. Decode/operational failures are routed to the DLQ.
- `nexus_streaming_pipeline` runs producer → consumer → quality → lineage.
- `nexus_reprocess_pipeline` replays raw landing files through Bronze, Silver and Gold for backfill/recovery (parameters: `dataset`, `raw_glob`, target tables).
- `nexus_dlq_replay` re-emits DLQ events to a Kafka topic (or stdout for inspection).

## OpenLineage Integration

When `OPENLINEAGE_URL` is set, the Spark wrapper at
`infra/spark/spark-submit-wrapper.sh` injects the OpenLineage Spark listener
with the configured namespace and endpoint. dbt tasks fall back to `dbt-ol`
automatically when both `OPENLINEAGE_URL` and `dbt-ol` are available, otherwise
they run plain `dbt`. Audit-emitted lineage events from `cli.nexus lineage record`
still land alongside Spark/dbt events in Marquez/OpenMetadata.
## Useful Commands

```powershell
python ingestion/batch/csv_ingestion.py --dataset us_accidents --source assets/samples/us_accidents_sample.csv
python -m cli.nexus batch run --dataset us_accidents --batch-id latest
python -m cli.nexus agent review --dataset us_accidents --batch-id latest
python -m cli.nexus quality stream --source transport --sample-events 25
python scripts/download_data.py --source-group core_historical --mode small_demo
python scripts/download_data.py --source tfl_line_status --source tfl_arrivals --mode small_demo
python scripts/download_data.py --poll --source-group realtime_polling --duration-days 0.1 --interval-minutes 1
python ingestion/streaming/producer.py --source tfl_arrivals --events 5
python -m pytest
```

Quality check against a local CSV:

```powershell
python -m cli.nexus quality check `
  --dataset us_accidents `
  --source assets/samples/us_accidents_sample.csv `
  --required-columns ID Severity Start_Time Start_Lat Start_Lng State `
  --primary-keys ID `
  --freshness-column Start_Time `
  --max-age-hours 24
```

## Source Discovery

`assets/source_discovery/` is a normal tracked directory containing the generated
source catalog, endpoint verification report, and schema JSON files used by
Nexus. Collector scripts are not kept in this repo.

Inspect the integrated inventory:

```powershell
python -m cli.nexus source-discovery summary
python -m cli.nexus source-discovery schemas
python -m cli.nexus source-discovery coverage
```

Sync selected discovery schemas into `runtime/source_discovery/`:

```powershell
python -m cli.nexus source-discovery sync `
  --schema OpenAQ_OpenAQ_Location `
  --schema TfL_Unified_API_Tfl.Api.Presentation.Entities.LineStatus
```

The sync output is generated runtime data and is ignored by Git.
The coverage command writes `assets/source_discovery/ingestion_coverage_map.json`.

## Data Flow

Batch flow:

1. Read local CSV/API records.
2. Apply configured auto-fix rules and JSON Schema coercion.
3. Write raw JSONL envelopes under `runtime/raw/<dataset>/`.
4. Run Great Expectations plus missing-value, duplicate, schema, freshness, and readiness checks.
5. Quarantine invalid records under `runtime/quarantine/`.
6. Write audit, quality metric, schema-history, and lineage events.
7. Let the governance agent return `PASS`, `WARNING`, or `FAIL`.
8. Continue to Bronze/Silver/Gold Spark jobs when the batch is allowed.

Streaming flow:

1. Produce normalized events from a real API when configured.
2. Fall back to simulated events when an API URL/token is missing.
3. Run streaming quality checks against sampled events.
4. Record metadata and quarantine invalid samples.

Supported producer sources:

| Source | Dataset/topic target | Real API behavior |
| --- | --- | --- |
| `transport` | `transport_events` | Uses `TRANSPORT_EVENTS_API_URL` when configured; otherwise synthetic fallback. |
| `tfl` | `transport-tfl` / line-status events | TfL line status JSON; optional `TFL_API_KEY`. |
| `tfl_line_status` | `transport-tfl-line-status` | TfL line status JSON every 300s; optional `TFL_API_KEY`. |
| `tfl_arrivals` | `transport-tfl-arrivals` | TfL StopPoint arrivals JSON every 60s; optional `TFL_API_KEY`. |
| `gtfs` | `gtfs_realtime_events` | Requires `GTFS_REALTIME_URL`; otherwise no real events. |
| `openaq` | `openaq_measurements` | Uses OpenAQ API when `OPENAQ_API_KEY` and URL are configured; otherwise simulated environment fallback. |
| `waqi` | `waqi_air_quality` | Uses WAQI API when `WAQI_API_TOKEN`/URL are configured; otherwise simulated environment fallback. |
| `londonair` | `environment-londonair` | Pulls LondonAir hourly monitoring index JSON. |
| `openmeteo` | `environment-openmeteo` | Pulls current Open-Meteo air-quality JSON for London. |
| `openweather` | `environment-openweather` | Pulls OpenWeather JSON when `OPENWEATHER_API_KEY` is configured. |

## Governance

NEXUS governance is metadata-driven and safe to run locally:

- `governance/quality/checks.py`: quality checks and readiness scoring.
- `governance/quality/gx_validation.py`: GX Core validation using the existing domain quality rules.
- `governance/quality/schema.py`: JSON Schema validation and type coercion.
- `governance/quality/quarantine.py`: invalid-record quarantine.
- `governance/audit.py`: auditable pipeline events.
- `governance/lineage.py`: OpenLineage-compatible events.
- `governance/schema_history.py`: schema snapshots and fingerprints.
- `governance/agents/governance_agent.py`: deterministic or optional LLM-backed decisions.
- `config/semantic_defaults.yml` and `domains/<domain>/semantic_rules.yml`: semantic contracts for glossary mappings, units, time roles, CRS, grain, definitions, and entity matching.

Set `GEMINI_API_KEY` only if you want the governance agent to call an LLM.
Without it, the agent uses deterministic rules.

Great Expectations is installed as a Python dependency, not as a separate
service. Airflow quality tasks run GX Core in-process and record the validation
summary in audit and quality metrics. Set `NEXUS_GX_ENABLED=false` to disable
GX locally while keeping the rest of the quality gate active.

Semantic contracts can be inspected from the CLI:

```powershell
python -m cli.nexus semantic list --domain environment
python -m cli.nexus semantic show --dataset openaq_measurements
python -m cli.nexus semantic export --kind openmetadata --domain environment
python -m cli.nexus semantic export --kind glossary --domain transport
python -m cli.nexus semantic match-entities `
  --dataset openaq_measurements `
  --source assets/samples/openaq_measurements.csv
```

The dbt seed `transform/dbt/seeds/unit_mapping.csv` is the canonical unit
mapping table. Gold models use it for deterministic conversions such as miles
to kilometers and pollutant readings to canonical concentration units. See
`docs/semantic_issues.md` for the full semantic checklist.

## Metadata Stack

OpenMetadata and OpenLineage are installed as an optional Docker Compose
profile so the default Nexus stack stays smaller.

Start Nexus with OpenMetadata and Marquez:

```powershell
docker compose --env-file .env -f infra/docker/docker-compose.yml --profile metadata up -d
```

OpenLineage emission is opt-in. For Docker-based Airflow runs, set this in
`.env` before starting the stack:

```text
OPENLINEAGE_URL=http://marquez:5000
OPENLINEAGE_ENDPOINT=/api/v1/lineage
OPENLINEAGE_NAMESPACE=nexus
```

For a lineage event recorded from the host shell, use `http://localhost:5000`
instead:

```powershell
$env:OPENLINEAGE_URL = "http://localhost:5000"
python -m cli.nexus lineage record --job-name demo --inputs raw.demo --outputs silver.demo
```

OpenMetadata requires an Elasticsearch service for its internal search index.
That service lives only in the `metadata` profile and is separate from the old
docker/elk stack.

Reset only the OpenMetadata/Marquez state:

```powershell
docker compose --env-file .env -f infra/docker/docker-compose.yml --profile metadata down
docker volume rm nexus_openmetadata-postgres-data nexus_openmetadata-es-data nexus_openmetadata-ingestion-dag-airflow nexus_openmetadata-ingestion-dags nexus_openmetadata-ingestion-tmp nexus_marquez-db-data
```

## Adding A Dataset

1. Add metadata to `domains/<domain>/datasets.yml`.
2. Add quality rules to `domains/<domain>/quality_rules.yml`.
3. Add a JSON Schema under `domains/<domain>/schemas/`.
4. Add ingestion code under `ingestion/batch/` or `ingestion/streaming/`.
5. Add Spark transformations only when the dataset needs medallion outputs.
6. Add focused tests for schema, quality, and transformation behavior.

## GCP VM Cluster

Terraform for optional GCP VMs lives in `infra/terraform/gcp/`. It now
provisions Nexus infrastructure only:

- 1 public master VM.
- Private worker VMs.
- Docker and Docker Compose plugin.
- Nexus repo checkout at `/opt/nexus/nexus` when `nexus_repo_url` is set.
- Helper scripts on the master:
  - `start-nexus-compose`
  - `stop-nexus-compose`

Apply from the Terraform directory:

```bash
cd infra/terraform/gcp
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan -var-file="terraform.tfvars"
terraform apply -var-file="terraform.tfvars"
```

Start Nexus on the master VM:

```bash
ssh ubuntu@<MASTER_PUBLIC_IP>
start-nexus-compose
```

See `infra/terraform/gcp/README.md` for VM details.

## Notes

- Keep `.env` and real `terraform.tfvars` out of Git.
- `.env.example` intentionally contains placeholders only.
- Do not commit real API tokens, service keys, raw source datasets, or generated runtime outputs.
