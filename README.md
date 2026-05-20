# NEXUS

NEXUS is a local-first data lakehouse scaffold for open data. It ingests
batch files and API streams, runs quality and governance checks, moves records
through Raw/Bronze/Silver/Gold layers, and exposes metadata through FastAPI,
Trino, Superset, and Airflow.

The generated source inventory is integrated in this repository under
`assets/source_discovery/`.

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
runtime/               Local generated outputs
assets/samples/        Sample files for each configured dataset (around 10 rows each)
serving/               FastAPI, Trino, and Superset assets
assets/source_discovery/ Generated source inventory and schema metadata
tests/                 Unit tests
transform/             dbt project
```

`runtime/` is generated output. Do not commit logs, raw data, metrics,
quarantine files, or synced source-discovery exports from that directory.

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

Set `GEMINI_API_KEY` only if you want the governance agent to call an LLM.
Without it, the agent uses deterministic rules.

Great Expectations is installed as a Python dependency, not as a separate
service. Airflow quality tasks run GX Core in-process and record the validation
summary in audit and quality metrics. Set `NEXUS_GX_ENABLED=false` to disable
GX locally while keeping the rest of the quality gate active.

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
