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
│   │           ├── raw/                     # Downloaded raw files
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

- `ingestion/streaming/consumer.py` reads Kafka events, lands them in `runtime/raw/<dataset>/`, and routes decode/operational failures to the DLQ.
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
python ingestion/streaming/producer.py --source tfl --events 5
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

Supported streaming sources:

| Source | Dataset |
| --- | --- |
| `transport` | `transport_events` |
| `tfl` | `tfl_transport_status` |
| `gtfs` | `gtfs_realtime_events` |
| `singapore` | `sg_traffic` |
| `openaq` | `openaq_measurements` |
| `waqi` | `waqi_air_quality` |

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
