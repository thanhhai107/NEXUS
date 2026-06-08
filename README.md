# NEXUS

NEXUS is a local-first lakehouse scaffold for open data. It ingests batch files,
downloaded API data, and streaming snapshots; validates records with data
contracts, JSON Schema, Great Expectations, schema-drift policy, and semantic
rules; then routes data through Bronze, Silver, Gold, Quarantine, audit, lineage,
and optional serving/metadata services.

The project is intentionally runnable from a single Ubuntu working tree with
`.venv`, while Docker Compose can start the heavier Airflow, Kafka, Spark, Trino,
Superset, OpenMetadata, and Marquez services when needed.

> **Current scope:** NEXUS is actively configured for the **TPC-DI benchmark**
> (`domains/tpc/`). The environment/transport domains described in earlier docs
> are reference architecture and are not deployed on the current tree. Data is
> generated synthetically via Data Caterer rather than ingested from live APIs.

## Current Capabilities

- Domain catalogs, JSON Schemas, quality rules, and semantic contracts under
  `domains/`.
- Source registry and data contract CLI for every configured dataset.
- Batch/API/download/streaming ingestion modules with raw envelope support.
- Great Expectations Core validation, JSON Schema validation, schema coercion,
  readiness scoring, quality metrics, audit logs, and quarantine routing.
- Schema drift detection for missing fields, unknown fields, dropped downstream
  fields, rename candidates, and type changes.
- Generated Great Expectations suite payloads from data contracts.
- Bronze validation CLI using contract, quality, schema drift, quarantine, and
  OpenMetadata-compatible DQ payloads.
- Semantic governance for glossary terms, aliases, units, timestamps, CRS, grain,
  metric definitions, and entity matching.
- Spark Bronze/Silver/Gold scripts plus dbt Gold model scaffolding.
- Optional OpenMetadata/OpenLineage/Marquez integration.

## Repository Map

```text
cli/                    Operational CLI entrypoint
common/                 Config, registry, contracts, semantic helpers
config/                 Defaults for download, quality, governance, semantic, Spark
docs/                   Design notes and implementation checklists
domains/                Dataset catalogs, schemas, quality rules, semantic rules
governance/             Quality, schema drift, quarantine, audit, lineage, metadata
ingestion/              Batch, download, streaming, source adapters
orchestration/airflow/  Airflow DAGs
processing/             Spark Bronze, Silver, Gold jobs
serving/                FastAPI, Trino, Superset configs
transform/dbt/          dbt project, seeds, Gold models
benchmark/              TPC-DI correctness, performance, and resource audits
tests/                  Unit and workflow tests
runtime/                Generated local outputs; do not commit
```

## Quick Start On Ubuntu

Use the project virtual environment:

```bash
cd /opt/nexus/nexus
source .venv/bin/activate
python -m cli.nexus --help
python -m pytest -q
```

If `.venv` is missing:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

The expected tested environment includes:

- Python 3.10
- `pytest==8.2.2`
- `great_expectations==1.16.1`
- `jsonschema==4.23.0`
- `pyspark==3.5.1`
- `dbt-trino==1.8.1`

## Local Smoke Test

These commands exercise the main local system without Docker services. Most
commands read from config and need no data files. Commands that require a CSV
source (`bronze-validate`, `quality check`, `match-entities`) need generated
TPC-DI data first (see [`nexus generate tpcdi`](#generate-tpc-di-benchmark-data)):

```bash
source .venv/bin/activate

python -m cli.nexus registry list
python -m cli.nexus contract show --dataset tpcdi_dim_customer
python -m cli.nexus semantic show --dataset tpcdi_dim_customer

python -m cli.nexus quality gx-suite --dataset tpcdi_dim_customer

python -m pytest -q
```

On the current tree, the full test suite is expected to pass. You may still see
non-blocking pytest warnings from older tests that return `True` instead of
using `assert`, plus a PySpark/pandas deprecation warning.

## Operational CLI

The CLI entrypoint is:

```bash
python -m cli.nexus --help
```

Top-level commands:

| Command | Purpose |
| --- | --- |
| `registry` | List or show source registry entries. |
| `contract` | Show data contracts assembled from catalog, schema, quality, semantic config. |
| `quality check` | Validate a local CSV against explicit quality arguments. |
| `quality bronze-validate` | Validate a file using the configured data contract. |
| `quality gx-suite` | Generate a Great Expectations suite payload from a contract. |
| `quality stream` | Validate sampled streaming events. |
| `semantic` | Inspect contracts, export OpenMetadata/glossary payloads, match entities. |
| `batch run` | Run config-driven batch ingestion and quality gate. |
| `lineage record` | Write OpenLineage-compatible lineage events. |
| `dlq` | List or replay operational dead-letter events. |
| `agent review` | Run deterministic or optional LLM-backed governance review. |

## Data Contracts

Data contracts are assembled from:

- `domains/<domain>/datasets.yml`
- `domains/<domain>/schemas/*.schema.json`
- `domains/<domain>/quality_rules.yml`
- `domains/<domain>/semantic_rules.yml`
- `config/quality_defaults.yml`
- `config/semantic_defaults.yml`

Show a contract:

```bash
python -m cli.nexus contract show --dataset tpcdi_dim_customer
```

Each contract exposes required columns, primary keys, freshness policy, schema
path, thresholds, auto-fix rules, semantic dedup keys, late-data policy, owner,
steward, source type, ingestion method, and target tables.

## Quality And Schema Drift

Quality validation includes:

- Required column existence and not-null checks.
- Primary key uniqueness and compound uniqueness.
- Freshness scoring.
- JSON Schema validation with type coercion.
- Great Expectations Core checks.
- Semantic unit checks for allowed source units and derived canonical values.
- Threshold-based pass/fail decisions.
- Quarantine routing for invalid records.
- Audit and quality metric logs.
- OpenMetadata-compatible DQ result payloads.

Schema drift detection covers:

- Required or optional missing fields.
- New unknown fields preserved for Bronze review.
- Dropped fields used downstream.
- Rename candidates from configured aliases or name similarity.
- Type changes, with safe-cast vs quarantine action.

Run a contract-driven Bronze validation (requires generated TPC-DI data):

```bash
python -m cli.nexus quality bronze-validate \
  --dataset tpcdi_dim_customer \
  --source runtime/datasets/tpcdi/tpcdi_dim_customer.csv \
  --no-exit-on-fail
```

Generate a Great Expectations suite payload:

```bash
python -m cli.nexus quality gx-suite --dataset tpcdi_dim_customer
```

OpenMetadata DQ payloads are logged locally by default. To POST them to a
service, set:

```bash
export OPENMETADATA_DQ_ENDPOINT="http://<host>/api/..."
export OPENMETADATA_AUTH_TOKEN="<token>"  # optional
```

## Semantic Governance

Semantic config lives in:

- `config/semantic_defaults.yml`
- `domains/<domain>/semantic_rules.yml`
- `transform/dbt/seeds/unit_mapping.csv`
- `transform/dbt/models/gold/schema.yml`

Inspect and export semantic metadata (active domain: `tpc`):

```bash
python -m cli.nexus semantic list --domain tpc
python -m cli.nexus semantic show --dataset tpcdi_dim_customer
python -m cli.nexus semantic export --kind glossary --domain tpc
```

Create canonical entity IDs and crosswalk output (requires generated data):

```bash
python -m cli.nexus semantic match-entities \
  --dataset tpcdi_prospect \
  --source runtime/datasets/tpcdi/tpcdi_prospect.csv
```

Entity matching supports exact, rule-based, fuzzy, and probabilistic local
matching. Config may list Splink or LLM-assisted review, but those are deferred
methods unless explicitly integrated.

## Medallion Processing

Spark jobs:

```bash
python processing/bronze/raw_to_bronze.py --help
python processing/silver/bronze_to_silver.py --help
python processing/gold/silver_to_gold.py --help
```

Bronze keeps raw envelope payloads and metadata. Silver flattens payloads,
trims strings, adds contract-based missing-field flags, and writes idempotently
using semantic dedup keys. Gold is primarily dbt-driven under
`transform/dbt/models/gold/`, with `processing/gold/silver_to_gold.py` kept for
generic backfills.

dbt assets:

```bash
cd transform/dbt
dbt --version
dbt parse --profiles-dir .
```

Running dbt models requires a reachable Trino/Iceberg profile.

## Generate TPC-DI Benchmark Data

TPC-DI data is generated via Data Caterer (Docker-based Spark) with 6 error
injection profiles (none → extreme) to test the quality pipeline:

```bash
# Generate SF=1 with moderate errors (CSV + Parquet)
python -m cli.nexus generate tpcdi --scale-factor 1 --error-profile moderate

# Dry-run to preview the command without executing
python -m cli.nexus generate tpcdi --dry-run

# List available Data Caterer plans
python -m cli.nexus generate list-plans
```

Generated data lands in `runtime/datasets/tpcdi/`. Operational failures go to
the DLQ. Invalid data records go to quarantine.

## Runtime Outputs

`runtime/` is generated and should not be committed. Important locations:

```text
runtime/raw/                 Local raw JSONL from batch CLI
runtime/lake/bronze/         Download/streaming Bronze landing
runtime/lake/silver/         Silver outputs
runtime/lake/gold/           Gold outputs
runtime/quarantine/          Invalid records for triage
runtime/dlq/                 Operational dead-letter events
runtime/metrics/             Quality metrics
runtime/logs/                Audit, lineage, OpenMetadata DQ logs
runtime/schemas/history/     Schema snapshots
```

## Docker Stack

The Docker stack is optional for local development.

Prepare `.env`:

```bash
cp .env.example .env
```

Validate compose config:

```bash
docker compose --env-file .env -f infra/docker/docker-compose.yml config --quiet
```

Start the default stack:

```bash
docker compose --env-file .env -f infra/docker/docker-compose.yml up -d
```

On Terraform VMs, run `start-nexus-compose` on the master. Worker VMs start a
host-network Spark worker with `start-nexus-worker`, and Spark jobs should be
submitted from the master with `nexus-spark-submit` so remote executors can
connect back to the driver.

Start with metadata services:

```bash
docker compose --env-file .env -f infra/docker/docker-compose.yml --profile metadata up -d
```

Local URLs:

| Service | URL |
| --- | --- |
| FastAPI | <http://localhost:8000/docs> |
| Airflow | <http://localhost:8080> |
| Trino | <http://localhost:8085> |
| Superset | <http://localhost:8088> |
| MinIO Console | <http://localhost:9001> |
| Hive Metastore thrift | `localhost:9083` |
| OpenMetadata, metadata profile | <http://localhost:8585> |
| Marquez UI, metadata profile | <http://localhost:3000> |

Stop services:

```bash
docker compose --env-file .env -f infra/docker/docker-compose.yml down
```

## OpenLineage

Lineage is local by default and can be emitted to Marquez/OpenLineage when
configured:

```bash
export OPENLINEAGE_URL="http://localhost:5000"
export OPENLINEAGE_ENDPOINT="/api/v1/lineage"
export OPENLINEAGE_NAMESPACE="nexus"

python -m cli.nexus lineage record \
  --job-name demo \
  --inputs raw.demo \
  --outputs silver.demo
```

Spark jobs use `infra/spark/spark-submit-wrapper.sh` to inject OpenLineage
settings when the environment variables are present.

## Governance Agent

The governance agent can run without external services using deterministic
rules:

```bash
python -m cli.nexus agent review --dataset tpcdi_dim_customer --batch-id manual
```

Set AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_DEFAULT_REGION`) and grant Bedrock model access to the IAM user/role
only if you want optional LLM-backed review. Without credentials, the agent
stays local and deterministic.

## Source Discovery

> **Note:** Source discovery was designed for the environment/transport domain
> model. It is not active on the current tree (TPC-DI). The CLI subcommands
> remain available for when real-world sources are re-enabled.

## Troubleshooting

Use `.venv`; system Python may have older dependencies:

```bash
source .venv/bin/activate
python -m pip show pytest great_expectations jsonschema
```

If Great Expectations is too slow or unavailable in a constrained shell:

```bash
export NEXUS_GX_ENABLED=false
```

If quality commands fail because of old `jsonschema`, confirm the active
interpreter:

```bash
which python
python -c "import jsonschema; print(jsonschema.__version__)"
```

Expected version is `4.23.0` or newer.

## TPC-DI Benchmark Pipeline

NEXUS includes a complete TPC-DI benchmark pipeline using DIGen-generated data (SF=3 at `runtime/tpcdi/sf3/`).

### Quick start

```bash
# Clean baseline
python -c "from benchmark.tpcdi.runner import TpcdiRunner; r=TpcdiRunner(scale_factor=3).run_milestone4(clean_outputs=True); print('is_valid=%s errors=%s' % (r.is_valid, r.errors))"

# E2E scenario: inject → detect → recover → score
python -c "from benchmark.tpcdi.scenario_runner import TpcdiScenarioRunner; r=TpcdiScenarioRunner(scale_factor=3).run_scenario('demo','extra_field','trade',line_numbers=[100,200,300]); print(r['scoring_report']['summary'])"
```

### Pipeline architecture

| Layer | Module | Function |
|---|---|---|
| Error injection | `ingestion/tpcdi/error_injection/source_injector.py` | `TpcdiSourceInjector.create_scenario()` |
| Source config | `domains/tpc/tpcdi_sources.yml` | DIGen source file definitions |
| I/O | `common/tpcdi_io.py` | Streaming iterators (`iter_tpcdi_records`, `iter_tpcdi_chunks`) |
| Bronze validation | `governance/quality/bronze_validator.py` | `validate_bronze_tpcdi_file()` |
| Processing | `processing/{bronze,silver,gold}/tpcdi_*.py` | TPC-DI-specific Bronze → Silver → Gold |
| Correctness audits | `benchmark/tpcdi/correctness.py` | 8 audits: row_count, pk_duplicate, trade_holding, prospect_customer |
| Recovery | `governance/recovery/engine.py` | `RecoveryEngine.run()` — repair source files |
| Scoring | `benchmark/tpcdi/scoring.py` | TP/FP/FN, detection_rate, recovery_rate, leakage_rate |
| E2E runner | `benchmark/tpcdi/scenario_runner.py` | `TpcdiScenarioRunner.run_scenario()` |

### Supported error scenarios

| Scenario | Detection | Recovery rate |
|---|---|---|
| `extra_field` | Bronze validation | 100% |
| `type_error` | Bronze validation | 100% |
| `duplicate_pk` | Gold audit | 100% |

### Artifacts (`runtime/tpcdi/scenarios/{id}/`)

- `injection_manifest.json` — ground truth mutations
- `detected_errors.json` — pipeline detection results
- `recovery_log.json` — repair actions
- `scoring_report.json` — TP/FP/FN + metrics

### Documentation

See `docs/tpcdi_flow.md` for detailed usage.

## Development Checks

Run before handing off changes:

```bash
source .venv/bin/activate
python -m compileall -q cli common governance ingestion processing tests benchmark
python -m pytest -q
python -m cli.nexus quality gx-suite --dataset tpcdi_dim_customer >/tmp/nexus_gx_suite.json
```

Current expected result:

```text
117 passed
```

Warnings in `tests/test_heterogeneity_adaptability.py` about tests returning
`True` are non-blocking today, but should eventually be converted to `assert`
statements.
