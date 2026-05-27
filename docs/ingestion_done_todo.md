# NEXUS Ingestion Done / Todo

Updated: 2026-05-26

## 1. Current Status

P0, P1, P2 and P3 from the ingestion roadmap have now been implemented and
re-tested, including Docker/Spark/Iceberg integration checks.

Verification:

- After P0 + P1:
  - `python -m pytest tests\test_downloader_runtime.py` -> 9 passed
  - `python -m pytest tests\test_streaming_producer.py tests\test_streaming_consumer.py` -> 4 passed
  - `python -m py_compile ...` on touched ingestion and Airflow files -> passed
- After P2:
  - `python -m pytest tests\test_downloader_runtime.py` -> 11 passed
  - `python -m pytest tests\test_dlq.py tests\test_streaming_producer.py tests\test_streaming_consumer.py` -> 7 passed
  - `python -m pytest tests\test_quality_checks.py` -> 11 passed
  - `python -m pytest` -> 54 passed
- After P3:
  - `python -m py_compile common\data_contract.py processing\common\idempotency.py processing\bronze\raw_to_bronze.py processing\silver\bronze_to_silver.py processing\gold\silver_to_gold.py processing\streaming\event_time_to_gold.py orchestration\airflow\dags\batch_ingestion_dag.py orchestration\airflow\dags\reprocess_pipeline_dag.py` -> passed
  - `python -m pytest tests\test_source_registry.py tests\test_processing_idempotency.py` -> 8 passed
  - `python -m pytest` -> 57 passed
- Next integration checks:
  - `docker compose --env-file .env -f infra/docker/docker-compose.yml up -d spark spark-worker` -> passed after updating Spark image to `bitnamilegacy/spark:3.5`
  - Spark standalone cluster smoke -> `COMPOSE_SPARK_COUNT 3`
  - Dockerized Spark/Iceberg Bronze -> Silver -> Gold integration, executed twice to exercise retry/idempotent MERGE -> `bronze_count=3`, `silver_count=2`, `gold_count=2`, `gold_record_sum=2`
  - Streaming event-time job with Iceberg checkpoint and `availableNow` trigger -> `gold_count=2`, `gold_record_sum=2`
  - Final `python -m pytest` -> 57 passed

One non-ingestion compatibility issue was also fixed during full-suite testing:
`governance/quality/gx_validation.py` now has a deterministic fallback validator
when `great_expectations` is not importable in the local runtime.

## 2. Pipeline Boundary

The current ingestion and processing flow is:

```text
SourceSpec / Data Contract
  -> DownloadPlan or expected chunk set
  -> Chunk execution
  -> Checkpoint
  -> RunManifest
  -> PublishedManifest
  -> Canonical RawEnvelope
  -> Validation / Quarantine / DLQ
  -> Bronze idempotent MERGE
  -> Silver semantic dedup MERGE
  -> Gold event-time/window aggregation
```

Important boundary rules now enforced:

- Downstream downloader flow only proceeds when `published_manifest.json`
  exists.
- Missing expected or required chunks block publish.
- Parser failures are operational failures and go to DLQ.
- Schema-invalid raw-envelope records are policy-gated and go to quarantine
  when downloader schema validation is enabled.
- Bronze uses physical ingestion id by default.
- Silver can use semantic dedup keys from Data Contract.
- Event-time aggregation can use watermark and allowed lateness instead of
  ingestion time.

## 3. Implemented P0

### 3.1 Raw Adapter Integration Bug Fixed

`ingestion/downloaders/london_downloader.py` now matches the current
`published_run_to_raw_envelope()` API.

Previous broken call:

```python
published_run_to_raw_envelope(..., schema_validation_enabled=...)
```

Current behavior:

- Calls `published_run_to_raw_envelope(run.published_manifest_path)`.
- Reads `record_count`, `parser_failures` and `parser_failure_details`.
- Attaches validation and DLQ routing results to the returned summary.

### 3.2 Expected Chunk Enforcement Added

`SourceRun` now supports:

- `expect_chunk(...)`
- `expect_chunks(...)`
- `register_plan(DownloadPlan)`

Coverage now includes:

- completed chunks
- failed chunks
- skipped chunks
- expected chunks
- `CoveragePolicy.required_chunks`

If an expected/required chunk is missing, the run manifest records it as:

```text
status = failed
error = expected_chunk_missing
```

and `PublishedManifest` is not written.

### 3.3 Required Chunk Policy Enforced

`CoveragePolicy.required_chunks` is now loaded into coverage evaluation. A run
cannot publish when any required chunk is absent or failed.

Tests added:

- missing expected chunk blocks publish
- missing `required_chunks` entry blocks publish

## 4. Implemented P1

### 4.1 Airflow Downloader DAG Added

New file:

- `orchestration/airflow/dags/download_ingestion_dag.py`

It adds:

- downloader task per configured source
- `execution_timeout`
- bounded retries
- `retry_exponential_backoff`
- `max_retry_delay`
- Airflow pool support via:
  - `NEXUS_AIRFLOW_POOL_<SOURCE>`
  - `NEXUS_AIRFLOW_API_POOL`
  - fallback: `default_pool`
- a publish gate that checks:
  - enough `published_manifest.json` files exist
  - each manifest is actually published
  - stamped `downstream_raw_path` exists when raw envelope output is required

### 4.2 Batch / Streaming API Paths Use Shared Resilient Runtime

Updated:

- `ingestion/batch/api_ingestion.py`
- `ingestion/batch/csv_download_ingestion.py`
- `ingestion/streaming/producer.py`

These paths now use the downloader runtime for bounded retry, timeout, backoff,
request logging, checkpoint/run manifest artifacts and atomic file download
where applicable.

### 4.3 Retry-After Handling Added

`ingestion/downloaders/http.py` now parses HTTP `Retry-After` on retryable 429
responses and sleeps for the greater of:

- exponential backoff + jitter
- `min_delay_on_429_seconds`
- parsed `Retry-After`

### 4.4 TfL / WAQI Chunk Tracking Improved

`ingestion/downloaders/sources/realtime.py` now registers TfL status, TfL
arrivals and WAQI map bounds chunks before the request, so those chunks are
visible to checkpoint/coverage.

## 5. Implemented P2

### 5.1 Downloader Validation Stage Added

New file:

- `ingestion/downloaders/validation.py`

It provides:

- `validate_raw_envelope_file(...)`
- `route_parser_failures_to_dlq(...)`
- `route_run_failures_to_dlq(...)`
- `route_source_failure_to_dlq(...)`

Schema validation is policy-gated through:

```yaml
resilient_runtime:
  validation_policy:
    schema_validation_enabled: false
    quarantine_invalid_records: true
```

The default remains `false` because current domain schemas describe normalized
dataset records, while downloader artifacts can still be raw source payloads.
When enabled, invalid raw-envelope payloads are written to quarantine.

### 5.2 Parser And Operational Failures Routed To DLQ

Downloader parser failures are routed as:

```text
category = download_parser_failed
```

Downloader source failures are routed as:

```text
category = download_source_failed
```

Failed chunks in a completed run manifest are routed as:

```text
category = download_chunk_failed
```

This keeps the distinction from quarantine:

- DLQ = operational failure
- Quarantine = invalid data record

## 6. Implemented P3

### 6.1 Semantic Dedup Keys In Data Contract

Updated:

- `common/data_contract.py`

Each `DataContract` now exposes:

- `semantic_dedup_keys`
- `late_data_policy`

Dedup key derivation:

1. Use `semantic_dedup_keys` from dataset config when explicitly provided.
2. Otherwise use dataset `primary_keys`.
3. Otherwise fallback to `_nexus_record_id`.

Late-data policy derivation:

- `event_time_field`: quality freshness column when available, otherwise
  `_nexus_event_time`.
- `watermark`: default `2 hours` for streaming/API-stream datasets, `24 hours`
  for batch-oriented datasets.
- `allowed_lateness`: same as watermark unless configured.
- `aggregation_window`: default `1 hour`.
- `late_record_action`: default `retain_for_reprocess`.

Example for `openaq_measurements`:

```text
semantic_dedup_keys = location_id, parameter, datetime
event_time_field = datetime
watermark = 2 hours
aggregation_window = 1 hour
```

### 6.2 Idempotent Bronze Writes

Updated:

- `processing/bronze/raw_to_bronze.py`
- `processing/common/idempotency.py`

Bronze now:

- keeps raw envelope payload and metadata
- deduplicates by `_nexus_record_id` by default
- writes with Iceberg MERGE when the table exists
- creates the table when it does not exist
- supports `--write-mode replace` for explicit full replacement

Default CLI:

```powershell
python processing/bronze/raw_to_bronze.py `
  --raw-path runtime/raw/<dataset>/*.jsonl `
  --bronze-table nexus.bronze.<dataset>
```

Optional:

```powershell
--dedup-keys _nexus_record_id
--write-mode merge
```

### 6.3 Idempotent Silver Writes With Semantic Dedup

Updated:

- `processing/silver/bronze_to_silver.py`

Silver now:

- preserves useful envelope metadata:
  - `_nexus_record_id`
  - `_nexus_event_time`
  - `_nexus_ingested_at`
  - `_nexus_run_id`
  - `_nexus_chunk_id`
  - `_nexus_source`
  - `_nexus_dataset`
- trims string columns
- uses semantic dedup keys from Data Contract when `--dataset` is provided
- supports explicit `--dedup-keys`
- writes with Iceberg MERGE by default

Airflow batch and reprocess DAGs now pass `--dataset` to Silver processing.

### 6.4 Event-Time Gold Aggregation

Updated:

- `processing/gold/silver_to_gold.py`

Gold now supports two modes:

1. Existing generic aggregate by dimension.
2. Event-time/window aggregate when `--event-time-column` is provided.

Event-time mode:

```powershell
python processing/gold/silver_to_gold.py `
  --silver-table nexus.silver.openaq_measurements `
  --gold-table nexus.gold.openaq_air_quality_hourly `
  --group-by location_id,parameter `
  --metric-column value `
  --event-time-column datetime `
  --watermark-delay "2 hours" `
  --window-duration "1 hour"
```

Output merge keys:

```text
window_start, window_end, <group_by dimensions>
```

This lets reprocessing or replay update the same analytical window without
creating duplicate Gold rows.

### 6.5 Streaming Watermark Job Added

New file:

- `processing/streaming/event_time_to_gold.py`

This is the dedicated Spark Structured Streaming path for late/out-of-order
data:

```powershell
python processing/streaming/event_time_to_gold.py `
  --silver-table nexus.silver.openaq_measurements `
  --gold-table nexus.gold.openaq_air_quality_hourly `
  --checkpoint-location runtime/checkpoints/openaq_air_quality_hourly `
  --event-time-column datetime `
  --group-by location_id,parameter `
  --metric-column value `
  --watermark-delay "2 hours" `
  --window-duration "1 hour"
```

It uses:

```text
withWatermark(event_time_column, watermark_delay)
groupBy(window(event_time_column, window_duration), dimensions)
foreachBatch(...)
Iceberg MERGE by window_start, window_end and dimensions
```

This is the core implementation for the Late-arriving / Out-of-order Data
painpoint.

Integration follow-up changed this job from native Iceberg streaming
`toTable(...).outputMode("update")` to `foreachBatch + MERGE`, because Iceberg
does not support native Update mode as a streaming sink. The job now keeps
update-mode semantics inside Spark and writes each micro-batch idempotently.

Additional runtime options:

```powershell
--trigger-available-now
--processing-time "1 minute"
--await-timeout-seconds 60
--process-overwrite-snapshots
--process-delete-snapshots
```

By default the streaming reader skips Iceberg overwrite/delete snapshots to
avoid query failure on non-append snapshots. For exact continuous streaming,
the preferred Silver input is append-only or changelog-like; batch MERGE Silver
tables are still better consumed by the batch Gold event-time job.

## 7. Integration Check Results

Date: 2026-05-26

### 7.1 Spark Runtime

The original Compose image `bitnami/spark:3.5` no longer resolves from Docker
Hub. `infra/docker/docker-compose.yml` now uses:

```text
bitnamilegacy/spark:3.5
```

This preserves the existing Bitnami `SPARK_MODE` environment contract.

Validated:

- Spark master starts on `7077` and UI on `8081`.
- Spark worker registers with master.
- Cluster smoke job submitted through `spark://spark:7077` returned
  `COMPOSE_SPARK_COUNT 3`.

Notes:

- Compose now sets runtime defaults needed for ad-hoc PySpark checks:
  - `HOME=/tmp/nexus-spark-home`
  - `PYSPARK_PYTHON=/opt/bitnami/python/bin/python3.12`
  - `PYSPARK_DRIVER_PYTHON=/opt/bitnami/python/bin/python3.12`
  - `HADOOP_USER_NAME=spark`
- The Bitnami image can still log a Hadoop user lookup warning for the
  container's numeric user, but the standalone Spark job succeeds.
- Host-local PySpark works with Python 3.11. Python 3.12 caused worker crashes
  in the local Windows smoke test, and local Iceberg-on-Windows requires
  `winutils.exe`. The containerized Linux checks are the reliable integration
  baseline.

### 7.2 Batch Bronze / Silver / Gold

Dockerized Spark 3.5.6 with Iceberg 1.5.2 executed the full path twice:

```text
raw JSONL
  -> processing/bronze/raw_to_bronze.py
  -> processing/silver/bronze_to_silver.py
  -> processing/gold/silver_to_gold.py
```

The fixture included:

- duplicate physical raw record id
- duplicate semantic event id
- out-of-order event timestamps

Verified result after the second run:

```text
bronze_count = 3
silver_count = 2
gold_count = 2
gold_record_sum = 2
```

This confirms idempotent retry behavior for Bronze/Silver/Gold MERGE and
event-time Gold aggregation.

### 7.3 Streaming Event-Time Gold

Initial check exposed a real issue:

```text
Iceberg streaming sink does not support Update mode
```

Fix applied:

- `processing/streaming/event_time_to_gold.py` now uses `foreachBatch`.
- Each micro-batch writes through `write_idempotent_iceberg(...)`.
- `--trigger-available-now` and `--await-timeout-seconds` make bounded
  integration checks possible.
- Iceberg overwrite/delete snapshot skip options were added for operational
  resilience.

Validated result with append-only Silver input:

```text
STREAMING_INTEGRATION_CHECKS {'gold_count': 2, 'gold_record_sum': 2}
```

## 8. Painpoint Coverage After P3

| Painpoint | Status | Notes |
| --- | --- | --- |
| API Failure & Timeout | Implemented for downloader, batch API/CSV download, and streaming API polling | Shared resilient client covers retryable 429/5xx, timeout and connection errors. Airflow DAG adds task timeout and bounded retry. |
| Rate Limit Handling | Implemented at client and Airflow level | Client-side source delay, 429 backoff and `Retry-After` exist. Airflow pool names are configurable per source. |
| Partial Download | Implemented for declared/observed chunks | Checkpoint, expected chunks, required chunks, run manifest and published manifest gate prevent publishing incomplete declared coverage. |
| Retry Duplicate / Non-idempotent Write | Implemented for ingestion/processing files | Downloader/raw writes are atomic and checkpointed. Bronze/Silver/Gold now support idempotent Iceberg MERGE. |
| Poison Record / Bad Message | Implemented as policy-gated downloader validation plus existing CLI quarantine | Parser failures go to DLQ. Schema-invalid downloader raw-envelope records can be quarantined when validation is enabled. |
| Late-arriving / Out-of-order Data | Implemented and integration-tested | Data Contract exposes late-data policy. Gold supports event-time window aggregation. Streaming job uses watermark, checkpoint, `foreachBatch` and idempotent Iceberg MERGE. |

## 9. Remaining Work

No P0-P3 roadmap items remain as code-level scaffolding.

Recommended next runtime hardening:

1. Run Airflow DAGs end-to-end against live or mocked API sources with pools
   enabled.
2. Confirm the production Silver streaming source is append-only or
   changelog-like. If it is a MERGE table, use the batch event-time Gold job for
   exact re-aggregation or accept skip-overwrite semantics for streaming.
3. Tune per-dataset `semantic_dedup_keys` and `late_data_policy` explicitly in
   `domains/*/datasets.yml` if source-specific lateness differs from defaults.
4. Consider deeper internal decomposition of `SourceRun` into checkpoint store,
   manifest store, output writer and request logger after runtime integration.

## 10. Current Verdict

Ingestion and early processing now follow the intended scalable contract:

```text
Plan / expected chunks
  -> resilient execution
  -> checkpoint
  -> manifest
  -> publish gate
  -> raw envelope
  -> validation / DLQ / quarantine
  -> idempotent Bronze/Silver/Gold
  -> event-time watermark aggregation
```

The six reliability/failure-handling painpoints are now represented in code.
The remaining risk is integration/runtime validation against actual Spark,
Iceberg, Airflow pools and live API behavior.
