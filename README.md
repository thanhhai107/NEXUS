# NEXUS: Network for Extracting, Unifying, and Supervising Open Data

NEXUS is a student-project-friendly data lakehouse scaffold for ingesting, governing, processing, and serving heterogeneous open data. It uses a medallion architecture with MinIO, Iceberg, Spark, dbt, Trino, Superset, Airflow, and an optional FastAPI metadata service.

## What It Does

- Ingests CSV files, public API data, and simulated Kafka events.
- Starts with the Transport domain using the Kaggle US Accidents dataset.
- Stores Raw, Bronze, Silver, and Gold data in a MinIO-backed lakehouse.
- Runs Spark and dbt transformations for analytics-ready outputs.
- Serves data through Trino, Superset, and FastAPI.
- Adds governance controls: auto-fix, quality checks, quarantine, audit logs, schema history, lineage, readiness scoring, and quality-history tracking.
- Adds a lightweight NEXUS Governance Agent that reviews metadata, decides whether each batch should continue, and writes a remediation plan.

## Architecture

NEXUS is a local-first data lakehouse scaffold for open data. It follows a medallion layout:

```text
Sources -> Ingestion -> Raw -> Bronze -> Silver -> Gold -> Serving
                                  |
                                  v
                         Governance + Quality
```

Core layers:

- **Sources**: Transport, Environment, and Education open-data APIs/files, with Transport as the first runnable domain.
- **Ingestion**: Python loaders write source-preserving raw JSONL envelopes.
- **Storage**: MinIO provides S3-compatible object storage for local development.
- **Tables**: Apache Iceberg manages Bronze, Silver, and Gold datasets.
- **Processing**: Spark transforms raw records through the Bronze, Silver, and Gold medallion layers.
- **Analytics**: dbt defines BI-facing marts and SQL models on top of Silver or Gold tables; Trino serves queries, and Superset serves dashboards.
- **Services**: FastAPI exposes lightweight metadata, quality, and readiness endpoints.
- **Governance**: Auto-fix, quality checks, schema history, audit logs, lineage, quarantine, readiness scoring, quality history, and agent decisions supervise pipeline state.

## Repository Map

```text
config/           Shared runtime config such as Spark defaults and quality thresholds
domains/          Domain dataset catalog, quality rules, and schemas
orchestration/    Airflow-specific DAGs and scheduling definitions
ingestion/        Batch and streaming ingestion code
processing/       Spark Bronze, Silver, and Gold transformation jobs
transform/        dbt BI marts and SQL models
serving/          FastAPI, Trino, and Superset serving assets
governance/       Quality, lineage, audit, schema history, and agent code
infra/            Terraform and Docker Compose deployment assets
common/           Shared config-loading helpers
cli/              Unified local and Airflow operational CLI
samples/          Small checked-in files for local demos
runtime/          Local generated raw, quarantine, metrics, schema-history, and log output
tests/            Unit tests
```

Keep Airflow DAGs focused on scheduling and dependencies. Pipeline behavior should stay in `ingestion/`, `processing/`, and `governance/`.

`domains/` describes datasets; it does not store data. Local sample files belong under `samples/`; generated local outputs belong under `runtime/`.

## Quick Start

```powershell
Copy-Item .env.example .env
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
docker compose --env-file .env -f infra/docker/docker-compose.yml up -d
```

Useful commands:

```powershell
python ingestion/batch/csv_ingestion.py --dataset us_accidents --source samples/us_accidents_sample.csv
python -m cli.nexus quality check --dataset us_accidents --source samples/us_accidents_sample.csv --required-columns ID Severity Start_Time Start_Lat Start_Lng State --primary-keys ID --freshness-column Start_Time --max-age-hours 24
python -m cli.nexus batch run --dataset us_accidents --batch-id latest
python -m cli.nexus agent review --dataset us_accidents --batch-id latest
python ingestion/streaming/producer.py --source transport --events 5 --bootstrap-servers localhost:29092
python -m cli.nexus quality stream --source transport --sample-events 25
pytest
```

Local services:

- Airflow: <http://localhost:8080>
- MinIO Console: <http://localhost:9001>
- Trino: <http://localhost:8085>
- Superset: <http://localhost:8088>
- FastAPI: <http://localhost:8000/docs>

## Data Flow

Batch flow:

1. Airflow starts `nexus_batch_ingestion_pipeline`.
2. CSV ingestion reads the local US Accidents sample, or a Kaggle-exported US Accidents CSV with the same key columns, and writes raw JSONL envelopes to `runtime/raw/us_accidents/`.
3. `python -m cli.nexus quality check` applies configured auto-fix rules, then evaluates missing values, duplicates, schema presence, freshness, and readiness.
4. Invalid records are written to `runtime/quarantine/`.
5. `python -m cli.nexus agent review` runs the NEXUS Governance Agent against quality and governance metadata.
6. Airflow branches on the agent decision: `PASS` and `WARNING` continue, `FAIL` stops after quarantine.
7. Spark loads raw envelopes into Bronze Iceberg tables.
8. Spark flattens and standardizes Bronze records into Silver.
9. Spark builds Gold aggregates; dbt can define BI-facing marts over Silver or Gold tables.
10. Trino, Superset, and FastAPI serve analytics and metadata.

For a config-driven local run, use:

```powershell
python -m cli.nexus batch run --dataset us_accidents --batch-id latest
```

This reads `domains/*/datasets.yml` and `domains/*/quality_rules.yml`, applies auto-fix rules, lands raw data, records audit/schema/metric metadata, and calls the Governance Agent unless `--skip-agent` is set.

Streaming flow:

1. Airflow triggers `nexus_streaming_pipeline`.
2. `ingestion/streaming/producer.py` polls the selected source and sends normalized JSON events to Kafka.
3. If the selected API URL is missing or unavailable, the producer uses the appropriate simulated fallback.
4. A future Spark Structured Streaming job should consume Kafka events into Bronze.
5. `python -m cli.nexus quality stream` records streaming quality metadata and quarantines malformed sample events.

Supported streaming sources:

| Source | Domain | Default topic | Notes |
| --- | --- | --- | --- |
| `openaq` | Environment | `environment-openaq` | Poll-based OpenAQ air-quality stream. |
| `waqi` | Environment | `environment-waqi` | Requires WAQI token/API URL. |
| `transport` | Transport | `transport-events` | Generic Transport API with simulated fallback. |
| `tfl` | Transport | `transport-tfl` | Transport for London line status. |
| `gtfs` | Transport | `transport-gtfs` | GTFS Realtime feed wrapper; set `GTFS_REALTIME_URL`. |
| `singapore` | Transport | `transport-sg-traffic` | Data.gov.sg traffic images. |
| `education_sim` | Education | `education-events` | Optional simulated attendance/enrollment events. |

Examples:

```powershell
python ingestion/streaming/producer.py --source openaq --events 5
python ingestion/streaming/producer.py --source tfl --events 5
python ingestion/streaming/producer.py --source singapore --events 5
python ingestion/streaming/producer.py --source education_sim --events 5
python -m cli.nexus quality stream --source openaq --sample-events 25
```

To run a specific source in Airflow, set `NEXUS_STREAM_SOURCE` in `.env`. Education remains batch-first; `education_sim` is only for optional streaming demonstrations.

The Transport catalog also includes NYC TLC Trip Record Data as a secondary batch source. It is configured in `domains/*/datasets.yml`, but the default DAG starts with US Accidents to keep the first Transport pipeline small.

Environment sources are configured:

- OpenAQ poll-based streaming measurements
- World Air Quality Index project air-quality API stream
- NOAA NCEI Climate Data Online

Education catalog sources are configured:

- World Bank Education indicators
- UNESCO UIS Education indicators
- College Scorecard
- Optional simulated attendance/enrollment events

Environment and Transport streaming sources are cataloged with schemas and quality rules. Education data remains batch-first because it is not naturally real-time.

Raw records use this envelope shape:

```json
{
  "_nexus_ingested_at": "2026-04-27T00:00:00+00:00",
  "_nexus_source": "samples/us_accidents_sample.csv",
  "payload": {
    "id": "A-1",
    "severity": "2",
    "state": "CA"
  }
}
```

## Governance

The current governance implementation is rule-based and designed to run locally while supporting a Postgres-backed control plane in Docker:

- `governance/quality/checks.py`: missing value ratio, duplicate ratio, required-column checks, JSON Schema enforcement, freshness score, readiness score, all configured quality thresholds, and readiness-drop anomaly detection.
- `governance/quality/schema.py`: JSON Schema normalization for auto-fixed column names, type coercion for CSV/API text values, and strict validation with `jsonschema`.
- `governance/quality/auto_fix.py`: configured string trimming, column normalization, and missing-value filling before quality scoring.
- `governance/storage.py`: governance event storage abstraction. Local development writes JSONL; Docker defaults to Postgres with `NEXUS_GOVERNANCE_STORAGE=postgres`.
- `governance/quality/metrics.py`: quality metric history with batch, run, source, actor, threshold, and schema-coercion context.
- `governance/quality/quarantine.py`: invalid record quarantine with dataset, reason, batch id, run id, source path, actor, timestamp, and original item.
- `governance/audit.py`: governance audit events with batch id, run id, source path, and actor.
- `governance/schema_history.py`: schema fingerprints and snapshots in `runtime/schemas/history/`, also emitted to governance storage when enabled.
- `governance/lineage.py`: OpenLineage-compatible source-to-target lineage events.
- `governance/metadata.py` and `governance/policy.py`: owner, steward, sensitivity, retention, access policy, and role-based access checks.

`domains/*/quality_rules.yml` can include `auto_fix` settings such as trimming strings, normalizing column names, and filling missing values.
`config/governance_defaults.yml` supplies default governance metadata for every dataset; per-dataset overrides live under `governance:` in `domains/*/datasets.yml`.

## NEXUS Governance Agent

The NEXUS Governance Agent is a lightweight decision layer between the quality gate and Spark medallion processing. It exists to explain whether a batch should continue after quality checks, while keeping actual data processing in Spark and orchestration in Airflow.

The agent does not process large datasets directly. It only reads metadata and governance outputs:

- `runtime/logs/audit.jsonl`
- `runtime/logs/lineage.jsonl`
- `runtime/quarantine/`
- `runtime/metrics/quality.jsonl`
- `runtime/schemas/history/`
- quality metrics such as readiness score, missing ratio, duplicate ratio, freshness score, and schema validity

For each batch, the agent writes one auditable decision and remediation plan to `runtime/logs/agent_decisions.jsonl`:

- `PASS`: continue the pipeline.
- `WARNING`: continue the pipeline, but mark the batch with a warning.
- `FAIL`: stop the pipeline and keep data in quarantine.

The agent can call an LLM when `GEMINI_API_KEY` is configured. If no key is configured, or if the LLM response is invalid, it falls back to deterministic rules:

- Quality gate failed or readiness score below 50: `FAIL`
- Readiness score from 50 to below 80: `WARNING`
- Breaking schema changes: `FAIL`
- Quarantine records exist but valid records remain: `WARNING`
- Otherwise: `PASS`

Each decision also includes `issues`, `root_causes`, `recommended_fixes`, and `reprocess_required`.

Run it manually:

```powershell
python -m cli.nexus agent review --dataset us_accidents --batch-id latest
```

Airflow uses the agent in the batch DAG:

```text
ingest_csv
  -> run_quality_gate
  -> agent_review
  -> branch_on_agent_decision
      -> PASS/WARNING: raw_to_bronze -> bronze_to_silver -> silver_to_gold
      -> FAIL: stop_after_quarantine
```

The agent is intentionally limited: it cannot modify raw data, cannot write Iceberg tables, and cannot replace Spark transformations. It only produces decisions and explanations.

## API

The optional FastAPI service in `serving/api/` exposes:

- `GET /health`
- `GET /datasets`
- `GET /datasets/{dataset_name}/quality`
- `GET /datasets/{dataset_name}/readiness`
- `GET /datasets/{dataset_name}/quality-history`
- `GET /agent/decisions`
- `GET /datasets/{dataset_name}/agent-decision`
- `GET /datasets/{dataset_name}/remediation-plan`
- `GET /governance/summary`
- `GET /datasets/{dataset_name}/governance-summary`

The API enforces dataset access with the `X-NEXUS-Role` header. `admin` and `steward` can read governance-wide agent decisions; dataset endpoints are filtered or blocked based on each dataset `access_policy`.

Docker also mounts Trino file-based access-control rules from `serving/query/trino/rules.json`. Superset is configured without public/guest access and with dashboard RBAC enabled; dataset grants still need to be assigned to Superset roles when dashboards are created.

## Platform Notes

MinIO stores raw files, medallion outputs, governance artifacts, and Iceberg warehouse data during local development. The expected local bucket is `nexus-lakehouse`, with prefixes such as `raw/`, `bronze/`, `silver/`, `gold/`, `warehouse/`, `governance/`, and `quarantine/`.

Iceberg is configured as a local Hadoop-style catalog backed by MinIO-compatible S3 paths. For production, replace this with a persistent catalog such as Hive Metastore, Nessie, AWS Glue, or another Iceberg-compatible catalog.

## Adding a Dataset

1. Add metadata to `domains/*/datasets.yml`.
2. Add quality rules to `domains/*/quality_rules.yml`.
3. Add a schema in `domains/<domain>/schemas/<dataset>.schema.json`.
4. Add or update ingestion code under `ingestion/batch/` or `ingestion/streaming/`.
5. Add Spark transformations under `processing/bronze/`, `processing/silver/`, or `processing/gold/`.
6. Add dbt marts under `transform/dbt/models/` only when the dataset needs BI-facing SQL models beyond the Spark medallion tables.
7. Add focused tests for quality rules, schema behavior, and transformation assumptions.

## Cluster Deployment

This profile maps NEXUS to the provided 1-master/4-worker Ubuntu infrastructure. It is production-like enough for a student project while keeping operations understandable.

### Infrastructure

| Role | Count | OS | Machine | CPU / RAM | Disk | Network |
| --- | ---: | --- | --- | --- | --- | --- |
| Master | 1 | Ubuntu 22.04 LTS | `e2-custom-12-16384` | 12 vCPU / 16 GB | 200 GB `pd-balanced` | default VPC + public IP |
| Worker | 4 | Ubuntu 22.04 LTS | `e2-custom-12-16384` | 12 vCPU / 16 GB | 200 GB `pd-balanced` | default VPC + public IP |

### Service Placement

| Node group | Recommended services | Purpose |
| --- | --- | --- |
| Master | Airflow webserver, Airflow scheduler, Airflow metadata DB, Spark master, Trino coordinator, Superset, FastAPI, lightweight monitoring | Control plane, orchestration, metadata, user-facing services |
| Worker 1-4 | Spark workers, Trino workers, MinIO distributed nodes | Data processing, SQL execution, object storage |
| Worker 1-3 | Kafka brokers | Streaming ingestion with broker redundancy |
| Worker 1 | Kafka controller or ZooKeeper/KRaft controller | Streaming cluster coordination for student-scale setup |

### Resource Budget

Each node has 12 vCPU and 16 GB RAM. Keep memory allocations conservative because Spark, Trino, Kafka, and MinIO can compete heavily.

Suggested starting point:

| Component | Master | Each worker |
| --- | ---: | ---: |
| OS reserve | 2 GB | 2 GB |
| Spark | 1 GB master | 8 GB worker memory, 8 cores |
| Trino | 2 GB coordinator | 3-4 GB worker |
| Airflow | 3-4 GB | none |
| Superset | 2 GB | none |
| FastAPI | 512 MB-1 GB | none |
| MinIO | none or client only | 1-2 GB |
| Kafka | none | 1-2 GB on worker 1-3 |

For classroom demos, avoid running large Spark jobs and large Trino queries at the same time. If Trino workloads become important, reduce Spark worker memory or run Spark jobs in scheduled windows.

### Storage Layout

Use the 200 GB disks as local persistent storage for each service:

```text
/opt/nexus                  repository checkout
/data/minio                 MinIO object data
/data/kafka                 Kafka broker data
/data/airflow               Airflow metadata/log data on master
/data/trino                 optional Trino spill directory
/data/spark                 Spark local scratch
```

MinIO should run in distributed mode across the four workers. Create one lakehouse bucket:

```text
nexus-lakehouse
```

Expected prefixes:

```text
raw/
bronze/
silver/
gold/
warehouse/
governance/
quarantine/
```

### Network and Firewall

All machines have public IPs, but only user-facing services should be exposed publicly.

Recommended public access:

| Port | Service | Exposure |
| ---: | --- | --- |
| 22 | SSH | restrict to team IPs |
| 8080 | Airflow | restrict to team IPs |
| 8085 | Trino coordinator | restrict to team IPs |
| 8088 | Superset | restrict to team IPs |
| 8000 | FastAPI | optional, restrict or put behind reverse proxy |
| 9001 | MinIO console | restrict to admins |

Keep internal-only where possible:

- Kafka broker ports
- Spark worker ports
- MinIO data API between workers
- Airflow metadata database
- Trino worker ports

### Deployment Approach

Use Terraform to create the GCP VM cluster, then run Docker Compose on the provisioned servers. This project does not require Kubernetes or Ansible.

Suggested phases:

1. Provision Ubuntu nodes with `infra/terraform/gcp/`.
2. Let the Terraform startup script install Docker and the Docker Compose plugin.
3. Clone this repository to `/opt/nexus` on every node.
4. Bring up core storage first: MinIO distributed mode.
5. Bring up Kafka.
6. Bring up Spark master and workers.
7. Bring up Trino coordinator and workers.
8. Bring up Airflow, Superset, and FastAPI on the master.
9. Run the sample CSV ingestion and quality check before adding real open-data sources.

### Operational Notes

- Use `.env` for credentials and endpoints; do not commit real secrets.
- Keep Airflow DAGs source-controlled and deploy them from this repository.
- Record audit and lineage events for every pipeline transition.
- Store quarantined records separately from trusted Bronze/Silver/Gold data.
- Add monitoring before increasing data volume. At minimum, track disk usage, container restarts, Spark job failures, Kafka lag, and MinIO health.

## Terraform

GCP VM provisioning lives in [infra/terraform/gcp](infra/terraform/gcp). Terraform creates the servers, firewall rules, and VM service account; Docker Compose remains the runtime layer.

Keep Terraform definitions and `terraform.tfvars.example` in source control. Local state files, `.terraform/`, and real `terraform.tfvars` values are runtime artifacts and are ignored by `.gitignore`.

## Status

This repository is an initial backbone. Governance features are implemented as small rule-based modules with optional LLM review for agent decisions.

Do not commit real credentials, API tokens, or large source datasets.
