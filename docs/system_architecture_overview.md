# NEXUS System Architecture Overview

Tài liệu này mô tả tổng quan kiến trúc hiện tại của NEXUS, một Intelligent Data Platform local-first cho dữ liệu mở, tập trung vào bài toán tích hợp nhiều nguồn dữ liệu giao thông và môi trường, kiểm soát chất lượng, quản trị dữ liệu, xử lý lakehouse theo mô hình medallion, orchestration, serving và quan sát vận hành.

Ngày rà soát kiến trúc: 2026-05-24.

## 1. Mục Tiêu Hệ Thống

NEXUS được xây dựng như một nền tảng dữ liệu thông minh có thể:

- Tích hợp nhiều nguồn dữ liệu mở từ API, CSV download, snapshot realtime và Kafka streaming.
- Quản lý metadata nguồn dữ liệu thông qua domain catalog, source registry và data contract.
- Đưa dữ liệu qua pipeline Raw, Bronze, Silver, Gold với Spark, Iceberg, dbt và Trino.
- Áp dụng quality gate trước khi dữ liệu đi vào các lớp tin cậy hơn.
- Lưu audit, quality metrics, schema history, lineage, quarantine và DLQ để hỗ trợ governance.
- Cung cấp API metadata, query layer và dashboard layer cho người dùng downstream.
- Hỗ trợ agent governance dựa trên luật deterministic, có thể mở rộng sang LLM nếu cấu hình Gemini.
- Chạy local bằng Docker Compose và có scaffold Terraform để triển khai VM trên GCP.

Hệ thống hiện tại thiên về một data lakehouse scaffold đầy đủ để demo, thử nghiệm và mở rộng, hơn là một nền tảng production hoàn chỉnh.

## 2. Kiến Trúc Tổng Thể

Kiến trúc logic có thể nhìn theo các lớp sau:

```text
External Open Data Sources
  |
  |-- CSV/API downloaders, REST ingestion, Kafka producers
  v
Raw Landing Zone
  |-- runtime/raw/<dataset>/
  |-- runtime/downloads/<source_id>/run_id=<run_id>/
  |
  |-- Quality Gate, Schema Validation, Auto-fix, Quarantine, DLQ
  v
Bronze Layer
  |-- Spark raw_to_bronze
  |-- Iceberg tables
  v
Silver Layer
  |-- Spark bronze_to_silver
  |-- normalized, deduplicated records
  v
Gold Layer
  |-- dbt gold models
  |-- optional Spark silver_to_gold for reprocess/ad hoc jobs
  v
Serving
  |-- Trino
  |-- FastAPI Metadata API
  |-- Superset

Cross-cutting:
  Airflow orchestration
  Governance storage
  Quality metrics
  Schema history
  Lineage and OpenLineage
  OpenMetadata and Marquez optional profile
  Docker Compose and Terraform infrastructure
```

Các module Python và YAML hiện tại đóng vai trò là control plane. `domains/*/datasets.yml`, `domains/*/quality_rules.yml`, `config/*.yml` quyết định phần lớn hành vi pipeline. Code ingestion, quality, governance và serving đọc các catalog này để chạy thống nhất theo dataset.

## 3. Luồng Hoạt Động Chính

### 3.1 Batch Ingestion Flow

Luồng batch điển hình được thể hiện trong `cli/nexus.py` và Airflow DAG `nexus_batch_ingestion_pipeline`:

1. Đọc catalog dataset từ `domains/<domain>/datasets.yml`.
2. Xác định nguồn dữ liệu theo `source_type`: local CSV, CSV download, REST API hoặc API stream fallback.
3. Đọc records, áp dụng auto-fix và chuẩn hóa field theo quality rules.
4. Coerce kiểu dữ liệu dựa trên JSON Schema.
5. Ghi raw JSONL envelope vào `runtime/raw/<dataset>/`.
6. Chạy quality checks:
   - missing ratio
   - duplicate ratio
   - required columns
   - JSON Schema validation
   - freshness score
   - readiness score
   - Great Expectations validation
7. Ghi records lỗi vào `runtime/quarantine/`.
8. Ghi audit event, quality metric và schema snapshot.
9. Governance agent trả quyết định `PASS`, `WARNING` hoặc `FAIL`.
10. Nếu được phép, Airflow chạy Spark Raw to Bronze, Bronze to Silver, sau đó dbt Silver to Gold.
11. Ghi lineage event sau mỗi bước xử lý chính.

### 3.2 Download-Oriented Flow

Downloader hiện đại nằm trong `ingestion/downloaders/` và entrypoint là `scripts/download_data.py`, trỏ vào `ingestion/downloaders/london_downloader.py`.

Mỗi source được mô tả bằng `SourceSpec`, chạy trong `SourceRun`:

- Tạo thư mục `runtime/downloads/<source_id>/run_id=<run_id>/`.
- Ghi raw payload vào `raw/`.
- Ghi metadata vào `metadata/`.
- Duy trì `checkpoint.json` để resume chunk đã tải.
- Ghi `request_log.jsonl` cho từng request.
- Ghi `profile.json` với row count, file count, size, timestamp range và trạng thái.
- Ghi `source_manifest.json` để mô tả run.

Downloader hỗ trợ các source London/Greater London như OpenMeteo, LondonAir, OpenAQ, NCEI, WAQI, OpenWeather, STATS19, NaPTAN, London journeys, DfT road traffic, TfL status và TfL arrivals.

`scripts/consolidate_downloads.py` tạo một derived run `run_id=consolidated` bằng cách chọn file tốt nhất từ nhiều run, merge request logs, tạo dedupe index, duplicate groups, profile và checkpoint. Đây là bước hữu ích trước khi đưa dữ liệu download vào processing ổn định.

### 3.3 Streaming Flow

Streaming hiện tại dùng Kafka qua `ingestion/streaming/producer.py` và `ingestion/streaming/consumer.py`.

Luồng chính:

1. Producer lấy dữ liệu từ API nếu có URL/token, hoặc fallback sang event mô phỏng.
2. Producer normalize event theo source: transport, OpenAQ, WAQI, TfL, GTFS, Singapore traffic, LondonAir, OpenMeteo, OpenWeather.
3. Producer gửi event vào Kafka topic tương ứng.
4. Nếu publish lỗi sau retry, event được đẩy vào Kafka DLQ topic `nexus.dlq` và local/governance DLQ store.
5. Consumer đọc Kafka topic, decode JSON, ghi records hợp lệ vào `runtime/raw/<dataset>/`.
6. Decode/write failure được ghi vào DLQ.
7. Quality stream checkpoint lấy sample events để validate.
8. Lineage event mô tả luồng Kafka to Bronze được ghi lại.

Operational failures đi vào DLQ. Bad data records đi vào Quarantine. Hai khái niệm này được tách riêng rõ ràng.

### 3.4 Processing Medallion Flow

Processing nằm trong `processing/`:

- `processing/bronze/raw_to_bronze.py`: đọc raw JSONL envelope, giữ payload và metadata ingestion, ghi Iceberg Bronze table.
- `processing/silver/bronze_to_silver.py`: flatten `payload_struct`, trim string fields, drop duplicate, ghi Iceberg Silver table.
- `processing/gold/silver_to_gold.py`: job Spark generic để aggregate Silver sang Gold, chủ yếu phục vụ ad hoc/backfill.
- `transform/dbt/models/gold/`: dbt là hướng canonical cho Gold layer.

Các model dbt hiện có:

- `us_accidents_summary`
- `transport_events_hourly`
- `openaq_air_quality_hourly`

`transform/dbt/profiles.yml.example` cấu hình dbt kết nối Trino tại `localhost:8085`, database `iceberg`, schema `gold`.

## 4. Cấu Trúc Project

```text
assets/
  samples/
  source_discovery/
cli/
common/
config/
docs/
domains/
governance/
infra/
ingestion/
notebooks/
orchestration/
processing/
runtime/
scripts/
serving/
tests/
transform/
```

### `assets/`

Chứa dữ liệu hỗ trợ, không phải code pipeline chính.

- `assets/samples/`: sample CSV cho các dataset, thường dùng cho demo, test và local quality checks.
- `assets/source_discovery/`: source inventory đã generate từ repo discovery bên ngoài, bao gồm schema metadata, endpoint verification report và coverage map.

Vai trò: cung cấp dữ liệu mẫu và metadata discovery để bootstrap catalog, schema và thử nghiệm ingestion.

### `cli/`

Chứa CLI vận hành `python -m cli.nexus`.

Các nhóm lệnh chính:

- `batch run`: chạy ingestion theo catalog.
- `quality check` và `quality stream`: chạy quality gate.
- `agent review`: chạy governance agent.
- `lineage record`: ghi lineage.
- `source-discovery`: inspect, sync, coverage, integrate discovery schemas.
- `registry`: xem source registry.
- `contract`: xem data contract.
- `dlq`: list và replay DLQ.

Vai trò: operational control plane cho local/dev workflows và Airflow BashOperator.

### `common/`

Module dùng chung:

- `config.py`: load YAML config, merge domain catalogs và quality rules.
- `source_registry.py`: tạo source registry entry từ dataset catalog.
- `data_contract.py`: xây data contract từ registry, schema và quality rules.
- `source_discovery.py`, `source_coverage.py`: xử lý metadata discovery và coverage map.

Vai trò: thống nhất cách các thành phần đọc metadata và hiểu dataset.

### `config/`

Chứa defaults toàn hệ thống:

- `download_defaults.yml`: mode tải dữ liệu, source group, spatial scope Greater London, rate limit, retry, endpoint config.
- `quality_defaults.yml`: threshold mặc định cho quality gate.
- `governance_defaults.yml`: owner, steward, sensitivity, retention, access policy mặc định.
- `spark-defaults.conf`: cấu hình Spark bổ trợ.

Vai trò: cấu hình hành vi nền tảng, tách khỏi code.

### `domains/`

Chứa domain-specific metadata.

Hiện có:

- `domains/transport/`
- `domains/environment/`

Mỗi domain có:

- `datasets.yml`: mô tả dataset, source URI, source type, schema path, freshness, primary keys, governance, target table.
- `quality_rules.yml`: required columns, freshness column, auto-fix rules.
- `schemas/*.schema.json`: JSON Schema cho từng dataset.

Vai trò: catalog chính của hệ thống. Khi thêm dataset mới, đây là nơi cần cập nhật đầu tiên.

### `ingestion/`

Chứa ingestion code.

- `ingestion/batch/`: CSV ingestion, CSV download ingestion, REST API ingestion, raw writer.
- `ingestion/streaming/`: Kafka producer/consumer.
- `ingestion/downloaders/`: downloader framework có checkpoint/profile/manifest.
- `ingestion/downloaders/sources/`: source-specific downloaders cho OpenAQ, realtime APIs, transport APIs, LondonAir, NCEI, OpenMeteo.

Vai trò: đưa dữ liệu từ nguồn ngoài vào raw landing zone hoặc download run store.

### `processing/`

Chứa Spark jobs theo medallion:

- `bronze/raw_to_bronze.py`
- `silver/bronze_to_silver.py`
- `gold/silver_to_gold.py`

Vai trò: xử lý dữ liệu từ raw envelope sang Iceberg table, chuẩn hóa và aggregate.

### `transform/`

Chứa dbt project:

- `transform/dbt/dbt_project.yml`
- `transform/dbt/models/gold/*.sql`
- `transform/dbt/models/gold/sources.yml`
- `transform/dbt/models/gold/schema.yml`

Vai trò: định nghĩa Gold models bằng SQL trên Trino/Iceberg.

### `governance/`

Chứa logic quản trị dữ liệu:

- `quality/checks.py`: quality metrics, readiness score, threshold evaluation.
- `quality/gx_validation.py`: Great Expectations in-memory validation.
- `quality/schema.py`: JSON Schema validation và type coercion.
- `quality/quarantine.py`: ghi invalid records.
- `quality/auto_fix.py`: normalize/trim/default transform.
- `audit.py`: audit JSONL/Postgres events.
- `lineage.py`: OpenLineage-compatible events, optional emit tới Marquez.
- `schema_history.py`: lưu schema snapshots và phát hiện thay đổi.
- `dlq.py`: record/list/replay DLQ events.
- `metadata.py`, `policy.py`: governance metadata và role-based access checks.
- `agents/`: governance agent, prompt, tool readers, decision schema.
- `storage.py`: abstraction local JSONL hoặc Postgres governance events.

Vai trò: cross-cutting governance layer cho quality, audit, lineage, policy, DLQ và agent decision.

### `orchestration/`

Chứa Airflow DAGs:

- `batch_ingestion_dag.py`: batch pipeline us_accidents qua quality, agent, Spark, dbt.
- `streaming_pipeline_dag.py`: produce Kafka events, consume raw, quality checkpoint, lineage.
- `reprocess_pipeline_dag.py`: replay raw qua Bronze, Silver, Gold.
- `dlq_replay_dag.py`: replay DLQ tới Kafka hoặc stdout.

Vai trò: orchestration, scheduling, dependency và recovery workflows.

### `serving/`

Chứa lớp phục vụ:

- `serving/api/main.py`: FastAPI Metadata API.
- `serving/api/Dockerfile`: container cho API.
- `serving/query/trino/`: Trino Iceberg catalog và access-control config.
- `serving/dashboards/superset/`: Superset config.

FastAPI expose:

- `/health`
- `/datasets`
- `/datasets/{dataset_name}/quality`
- `/datasets/{dataset_name}/readiness`
- `/datasets/{dataset_name}/quality-history`
- `/datasets/{dataset_name}/agent-decision`
- `/datasets/{dataset_name}/remediation-plan`
- `/governance/summary`
- `/datasets/{dataset_name}/governance-summary`

Vai trò: metadata API, query layer config và dashboard integration.

### `infra/`

Chứa hạ tầng local và cloud:

- `infra/docker/docker-compose.yml`: MinIO, Kafka, Spark, Trino, Airflow, governance Postgres, Superset, API, optional OpenMetadata/Marquez.
- `infra/spark/spark-submit-wrapper.sh`: inject OpenLineage Spark listener khi `OPENLINEAGE_URL` được set.
- `infra/terraform/gcp/`: scaffold VM cluster trên GCP, firewall, NAT, startup script và outputs service URLs.

Vai trò: chạy stack local và chuẩn bị deployment VM.

### `runtime/`

Thư mục output sinh ra trong quá trình chạy:

- `raw/`: raw JSONL landing.
- `downloads/`: downloader run output.
- `logs/`: audit, lineage, agent decisions.
- `metrics/`: quality metrics.
- `quarantine/`: invalid records.
- `dlq/`: operational failure events.
- `schemas/history/`: schema snapshots.
- `source_discovery/`: sync output từ discovery metadata.
- `analysis/`: phân tích cục bộ.

Vai trò: generated state. Không nên commit dữ liệu runtime thật.

### `scripts/`

Các script vận hành:

- `download_data.py`: compatibility wrapper cho downloader.
- `consolidate_downloads.py`: hợp nhất nhiều downloader runs thành canonical run.

### `tests/`

Unit tests cho source registry, discovery, coverage, quality checks, governance tools, governance agent, DLQ, auto-fix và streaming producer/consumer.

### `notebooks/`

Notebook profiling, hiện có `01_downloaded_data_profiling.ipynb`.

## 5. Dependency Chính

Python dependencies trong `requirements.txt`:

- `requests`: gọi REST APIs.
- `kafka-python`: Kafka producer/consumer.
- `PyYAML`: đọc YAML config/catalog.
- `jsonschema`: JSON Schema validation.
- `great_expectations`: quality validation.
- `psycopg[binary]`: governance event storage trong Postgres.
- `pytest`: test runner.
- `fastapi`, `uvicorn[standard]`: Metadata API.
- `pyspark`: Spark processing jobs.
- `dbt-trino`: dbt adapter cho Trino.
- `openlineage-dbt`: lineage cho dbt khi dùng `dbt-ol`.

Docker services chính:

- MinIO: object storage local cho lakehouse.
- Kafka/Zookeeper: streaming backbone.
- Spark master/worker: distributed processing.
- Trino: SQL query engine trên Iceberg.
- Airflow/Postgres: orchestration.
- Governance Postgres: lưu governance events khi `NEXUS_GOVERNANCE_STORAGE=postgres`.
- Superset: dashboarding.
- FastAPI: metadata/governance API.
- OpenMetadata/Elasticsearch/Marquez: optional metadata and lineage profile.

## 6. Cách Các Thành Phần Kết Nối

### Metadata-Driven Connection

`domains/*/datasets.yml` là điểm nối trung tâm:

- Ingestion biết source URI, source type, API key env, topic và local sample.
- Quality biết schema path, primary keys và freshness window.
- Governance biết owner, steward, sensitivity, retention và access policy.
- Processing biết target raw path, Bronze/Silver/Gold table.
- API biết dataset nào được expose cho role nào.
- Source registry và data contract được derive từ chính catalog này.

### Raw and Runtime Connection

Batch và streaming ingestion đều ghi raw JSONL envelope:

```json
{
  "_nexus_ingested_at": "...",
  "_nexus_source": "...",
  "payload": { "...": "..." }
}
```

Spark Bronze đọc envelope này. Silver flatten `payload_struct`. Gold đọc Silver qua dbt hoặc Spark.

### Governance Connection

Quality gate và agent ghi events vào local JSONL hoặc Postgres thông qua `governance/storage.py`.

Các consumer của governance data:

- FastAPI đọc quality, quarantine, schema history, lineage, agent decisions.
- Airflow branch đọc agent decision để quyết định chạy tiếp hay dừng.
- CLI đọc registry, contract, DLQ và quality history.
- Optional Marquez nhận OpenLineage events khi bật `OPENLINEAGE_URL`.

### Serving Connection

Trino dùng Iceberg Hadoop catalog với warehouse `s3a://nexus-lakehouse/warehouse` và MinIO endpoint `http://minio:9000`.

dbt kết nối tới Trino để build Gold tables.

Superset phụ thuộc Trino để tạo dashboard.

FastAPI hiện phục vụ metadata/governance, chưa phải data query API trực tiếp.

## 7. AI/ML Và Intelligent Governance

AI/ML hiện tại nằm ở lớp governance agent, không phải model training hay inference trên dữ liệu domain.

`governance/agents/governance_agent.py`:

- Thu thập evidence từ quality report, quarantine, schema history, audit events, lineage và quality history.
- Nếu có `GEMINI_API_KEY`, gọi Gemini API để sinh decision JSON.
- Nếu không có API key hoặc LLM lỗi, fallback sang rule-based decision.
- Sinh remediation plan gồm issues, root causes, recommended fixes và reprocess flag.

Điểm thông minh hiện tại:

- Readiness score từ quality metrics.
- Anomaly detection đơn giản dựa trên readiness-score drops.
- Agent decision kết hợp quality, quarantine và schema history.
- Remediation recommendations giúp operator biết bước tiếp theo.

Chưa có:

- Feature store.
- Model training pipeline.
- Model registry.
- Batch/online prediction service.
- Drift detection cho ML model.

## 8. Monitoring, Observability Và Governance Signals

Hệ thống hiện có các tín hiệu quan sát sau:

- Downloader `request_log.jsonl`: request URL, status code, duration, retry count, record count.
- Downloader `profile.json`: row count, file count, size, timestamps, failed requests, status.
- Audit log: quality check events.
- Quality metrics: status, readiness, missing, duplicate, freshness, threshold violations.
- Schema history: schema snapshot và breaking changes.
- Quarantine records: invalid data records.
- DLQ events: operational failures.
- Lineage JSONL and OpenLineage: job inputs/outputs.
- Airflow task status: orchestration-level monitoring.
- Optional Marquez UI: lineage visualization.
- Optional OpenMetadata: catalog and metadata management.

Chưa có monitoring production-grade như Prometheus metrics, alerting rules, SLO dashboard, centralized logs hoặc distributed tracing.

## 9. Infra Và Deployment

### Local Docker Compose

`infra/docker/docker-compose.yml` là deployment chính hiện tại.

Services mặc định:

- `minio`
- `zookeeper`
- `kafka`
- `spark`
- `spark-worker`
- `trino`
- `airflow-db`
- `governance-db`
- `airflow`
- `airflow-scheduler`
- `superset`
- `api`

Optional `metadata` profile:

- `openmetadata-postgresql`
- `openmetadata-elasticsearch`
- `openmetadata-migrate`
- `openmetadata-server`
- `openmetadata-ingestion`
- `marquez-db`
- `marquez`
- `marquez-web`

### GCP Terraform

`infra/terraform/gcp/` tạo:

- Một master VM public.
- Nhiều worker VM private.
- Firewall cho SSH và UI ports.
- NAT cho worker nodes.
- Service account VM.
- Startup script cài Docker/Docker Compose và checkout repo nếu có `nexus_repo_url`.
- Outputs cho API, Airflow, Trino, Superset và MinIO console.

Triển khai này là VM-based, chưa phải Kubernetes hoặc managed services.

## 10. Danh Sách Dataset Hiện Tại

### Transport

- `us_accidents`: Kaggle CSV sample, hiện đánh dấu deprecated/out-of-scope cho London demo.
- `nyc_tlc_trips`: parquet batch reference, deprecated/out-of-scope.
- `transport_events`: simulated/API stream fallback.
- `tfl_transport_status`: TfL line status/disruption stream.
- `gtfs_realtime_events`: GTFS Realtime wrapper, chờ feed URL.
- `sg_traffic`: Singapore traffic images, reference/out-of-scope.
- `stats19_collisions`: UK road collision open data.
- `naptan_stops`: London NaPTAN stops.
- `london_journeys`: TfL public transport journey counts.
- `dft_road_traffic`: DfT road traffic counts.

### Environment

- `openaq_measurements`: OpenAQ air-quality measurements.
- `waqi_air_quality`: WAQI realtime station snapshot/feed.
- `ncei_cdo_climate`: NOAA NCEI climate observations.
- `londonair_monitoring`: London Air Quality Network monitoring.
- `openmeteo_air_quality`: Open-Meteo air quality.
- `openweather_current`: OpenWeather current weather.

## 11. Điểm Mở Rộng Tiềm Năng

### Thêm Dataset Mới

Quy trình nên chuẩn hóa:

1. Thêm entry trong `domains/<domain>/datasets.yml`.
2. Thêm quality rules trong `domains/<domain>/quality_rules.yml`.
3. Thêm JSON Schema trong `domains/<domain>/schemas/`.
4. Thêm downloader hoặc ingestion mapping nếu source shape mới.
5. Thêm Bronze/Silver normalization nếu không dùng generic flattening.
6. Thêm dbt Gold model nếu dataset phục vụ analytics.
7. Thêm tests cho schema, quality, ingestion mapping và transformation.

### Mở Rộng Domain

Có thể thêm `domains/energy`, `domains/health`, `domains/economy`, `domains/urban_planning` mà không cần đổi kiến trúc lõi, miễn là giữ format `datasets.yml`, `quality_rules.yml`, `schemas/`.

### Productionize Storage

Các hướng mở rộng:

- Đồng bộ raw/download outputs lên MinIO/S3 thay vì chỉ local runtime.
- Chuẩn hóa Iceberg warehouse creation và bucket bootstrap.
- Thêm partitioning strategy theo dataset.
- Thêm table evolution policy và compaction.

### Data Quality Nâng Cao

Có thể bổ sung:

- Dataset-specific expectation suites.
- Referential integrity checks.
- Distribution checks.
- Spatial validity checks cho London bbox.
- Outlier detection.
- SLA/freshness alerting.

### AI/ML Nâng Cao

Các hướng phù hợp:

- Forecast air quality hoặc traffic congestion.
- Entity resolution giữa transport/environment datasets theo thời gian và địa điểm.
- Anomaly detection trên sensor readings.
- Natural language metadata assistant trên source registry/data contracts.
- Automated schema mapping suggestion khi thêm source discovery schema.

### API Và Serving

Có thể mở rộng FastAPI thành:

- Query API đọc Gold tables qua Trino.
- Dataset detail endpoint có contract/schema đầy đủ.
- Lineage graph endpoint.
- Quarantine review/update API.
- DLQ replay API có kiểm soát role.

## 12. Technical Debt Và Rủi Ro Hiện Tại

### Processing Logic Còn Generic

`bronze_to_silver.py` flatten payload và trim string chung cho mọi dataset. Điều này chưa đủ cho source-specific normalization, nested API payloads, units, timestamp parsing, geospatial fields và domain semantics.

Khuyến nghị:

- Tạo registry transformation theo dataset.
- Tách common cleaning và dataset-specific logic.
- Thêm contract tests cho từng Silver table.

### Gold Layer Chưa Đồng Nhất

dbt là hướng canonical cho Gold, nhưng `processing/gold/silver_to_gold.py` vẫn tồn tại như generic Spark aggregate. Hai hướng này dễ gây mơ hồ.

Khuyến nghị:

- Dùng dbt làm chuẩn cho Gold analytics.
- Giữ Spark Gold chỉ cho backfill đặc biệt hoặc loại bỏ nếu không cần.
- Document rõ owner của từng Gold model.

### Một Số dbt SQL Có Khả Năng Sai Quote

Một số SQL trong `transform/dbt/models/gold/` đang dùng `date_trunc(''hour'', ...)`. Trong SQL/dbt thông thường, literal này nên là `'hour'`. Đây có thể là lỗi cú pháp khi chạy dbt.

Khuyến nghị:

- Chạy `dbt compile` và `dbt run`.
- Sửa quoting nếu compile fail.
- Thêm CI test cho dbt compile.

### Airflow DAG Batch Đang Hard-code `us_accidents`

`batch_ingestion_dag.py` tập trung vào `us_accidents`, trong khi demo chính đang chuyển hướng Greater London.

Khuyến nghị:

- Parameterize DAG theo dataset.
- Dùng target metadata từ `domains/*/datasets.yml`.
- Tạo dynamic tasks theo domain/catalog.

### Downloader Và Raw Pipeline Chưa Nối Hoàn Toàn

Downloader ghi vào `runtime/downloads/<source_id>/run_id=...`, trong khi Spark Bronze job hiện đọc raw JSONL envelopes từ `runtime/raw/<dataset>`. Chưa có bước chuẩn hóa chính thức từ downloader raw payload sang raw envelope hoặc Bronze table.

Khuyến nghị:

- Thiết kế ingestion adapter từ downloader profile/consolidated run sang `runtime/raw/<dataset>/`.
- Hoặc cho Bronze job đọc trực tiếp downloader manifest và raw files.
- Chuẩn hóa naming giữa `source_id` và `dataset`.

### Storage Local Và Object Storage Chưa Được Đồng Bộ Rõ

Catalog target dùng `s3a://nexus-lakehouse/...`, nhưng nhiều ingestion path hiện ghi local `runtime/`.

Khuyến nghị:

- Quyết định raw landing canonical là local runtime, MinIO, hay cả hai.
- Nếu dùng MinIO, thêm upload/sync stage và bucket initialization.
- Tách dev path và production path bằng config.

### Governance Storage Có Hai Mode

`governance/storage.py` hỗ trợ local JSONL và Postgres. Điều này linh hoạt, nhưng có thể tạo khác biệt hành vi giữa host shell và Docker/Airflow.

Khuyến nghị:

- Document rõ mode mặc định từng môi trường.
- Thêm migration/schema init cho governance Postgres.
- Thêm API/debug endpoint cho storage mode hiện tại.

### Auth Và Secret Management Còn Demo

Docker Compose chứa nhiều default credentials local như MinIO, Airflow, governance DB, Superset.

Khuyến nghị:

- Giữ `.env.example` placeholder.
- Không dùng default credentials khi deploy thật.
- Thêm secret management cho cloud deployment.

### Error Handling Và Retry Chưa Đồng Nhất

Downloader có retry/rate-limit config và checkpoint tốt, producer có retry-to-DLQ, nhưng batch API ingestion đơn giản hơn.

Khuyến nghị:

- Chuẩn hóa HTTP client/retry/backoff qua một module chung.
- Đưa DLQ hoặc failure event vào mọi ingestion path.
- Ghi request-level metrics cho REST batch ingestion.

### Testing Chưa Bao Phủ End-to-End

Tests hiện tập trung unit-level. Chưa thấy e2e test chạy Docker services, Spark, Trino, dbt compile/run hoặc Airflow DAG parsing đầy đủ.

Khuyến nghị:

- Thêm `pytest` DAG import tests.
- Thêm dbt compile test.
- Thêm smoke test API `/health` và `/datasets`.
- Thêm integration test local cho một dataset nhỏ: raw to bronze to silver to dbt gold.

## 13. Bottleneck Tiềm Năng

### Local Runtime Disk

Downloader có thể tải nhiều file lớn, đặc biệt STATS19 và historical APIs. `runtime/downloads` và `runtime/raw` có thể tăng nhanh.

Giảm thiểu:

- Dùng consolidation để giảm duplicate paths.
- Thêm retention cleanup cho old runs.
- Đẩy canonical data lên MinIO/S3.

### Great Expectations In-Memory

`gx_validation.py` tạo pandas DataFrame từ toàn bộ records. Với batch lớn, memory sẽ là bottleneck.

Giảm thiểu:

- Sample validation cho batch rất lớn.
- Chạy GX/Spark validation trên distributed data.
- Tách lightweight gate trước, deep validation sau.

### Spark `createOrReplace`

Bronze/Silver jobs đang dùng `createOrReplace`, phù hợp demo nhưng chưa tối ưu cho incremental pipelines.

Giảm thiểu:

- Dùng append/merge incremental.
- Partition theo ingestion date/event date.
- Thêm idempotency dựa trên batch/run id.

### Kafka Consumer Batch Landing

Consumer gom records vào list rồi ghi một JSONL file. Với `max_messages` lớn, memory và latency tăng.

Giảm thiểu:

- Flush theo chunk.
- Commit offset theo chunk.
- Ghi streaming sink trực tiếp tới object storage hoặc Iceberg.

### Single-Node Local Services

Docker Compose dùng single Kafka broker, single Trino, single Airflow scheduler, single governance DB. Đây là bottleneck và single point of failure cho production.

Giảm thiểu:

- Scale services hoặc chuyển sang managed services.
- Dùng distributed object storage thật.
- Tách control plane và data plane.

## 14. Khu Vực Nên Refactor Ưu Tiên

### 1. Dataset-Oriented Pipeline Contract

Tạo một object/model thống nhất cho dataset runtime context:

- dataset name
- source config
- quality rules
- schema
- target tables
- governance metadata
- raw/download paths

Hiện các module tự load từng phần config, dễ lệch logic.

### 2. Ingestion Adapter Layer

Chuẩn hóa output của batch, streaming và downloader thành cùng một format hoặc cùng một manifest contract.

Mục tiêu:

- Một Bronze loader có thể đọc mọi source.
- Một quality gate biết batch/run/source lineage.
- Giảm mapping rời rạc giữa `source_id`, `dataset`, `topic`, `target`.

### 3. Dataset-Specific Silver Transformations

Thay generic Silver bằng plugin/registry:

```text
processing/silver/transforms/
  openaq_measurements.py
  tfl_transport_status.py
  stats19_collisions.py
  ...
```

Mỗi transform xử lý timestamp, type, nested fields, geospatial columns và business semantics riêng.

### 4. Airflow Dynamic DAGs

Thay hard-code dataset bằng dynamic DAG/task factory đọc `datasets.yml`.

Mục tiêu:

- Chạy một dataset bất kỳ.
- Backfill theo source group.
- Reuse quality, agent, lineage và processing tasks.

### 5. Storage Boundary

Định nghĩa rõ:

- raw local path dùng khi nào
- MinIO/S3 path dùng khi nào
- Iceberg table lifecycle
- runtime generated artifacts retention

### 6. Observability Contract

Chuẩn hóa tất cả logs/events thành schema versioned:

- audit event schema
- quality metric schema
- DLQ event schema
- lineage facet extensions
- downloader profile schema

Sau đó API và dashboard có thể đọc ổn định hơn.

## 15. Định Hướng Phát Triển Tiếp Theo

Một roadmap hợp lý:

1. Sửa và kiểm chứng dbt compile/run cho các Gold models hiện có.
2. Chuẩn hóa pipeline cho một London dataset chính, ví dụ `tfl_transport_status` hoặc `openaq_measurements`, end-to-end từ download/API tới Gold.
3. Nối downloader output vào medallion processing bằng adapter chính thức.
4. Refactor Silver transformations theo dataset.
5. Parameterize Airflow DAGs theo catalog.
6. Thêm integration smoke tests cho Docker Compose stack.
7. Mở rộng FastAPI để expose data contracts, lineage graph và query Gold summaries.
8. Bổ sung monitoring/alerting tối thiểu cho freshness, failed requests, DLQ count và quarantine count.
9. Productionize storage, credentials và deployment nếu đưa lên cloud.

## 16. Tóm Tắt Vai Trò Pipeline

| Layer | Thành phần | Vai trò |
| --- | --- | --- |
| Source Catalog | `domains/*/datasets.yml` | Định nghĩa dataset, nguồn, governance, target |
| Quality Rules | `domains/*/quality_rules.yml`, `config/quality_defaults.yml` | Điều kiện để dữ liệu được tin cậy |
| Downloader | `ingestion/downloaders/` | Tải dữ liệu nhiều nguồn có checkpoint/profile |
| Batch Ingestion | `ingestion/batch/`, `cli/nexus.py` | Đưa CSV/API records vào raw landing |
| Streaming | `ingestion/streaming/` | Produce/consume Kafka events |
| Raw | `runtime/raw/`, `runtime/downloads/` | Landing zone và persisted source runs |
| Quality/Governance | `governance/` | Validation, audit, metrics, schema, DLQ, quarantine, agent |
| Bronze | `processing/bronze/` | Lưu raw envelope vào Iceberg |
| Silver | `processing/silver/` | Chuẩn hóa, trim, dedupe |
| Gold | `transform/dbt/`, `processing/gold/` | Analytical aggregates |
| Orchestration | `orchestration/airflow/dags/` | Schedule, dependency, branching, reprocess |
| Serving | `serving/api/`, `serving/query/trino/`, `serving/dashboards/superset/` | API metadata, SQL query, dashboards |
| Infra | `infra/docker/`, `infra/terraform/gcp/` | Local stack và cloud VM scaffold |

NEXUS hiện đã có nền móng tốt cho một Intelligent Data Platform: metadata-driven, có quality gate, governance agent, lineage, DLQ/quarantine và medallion architecture. Phần cần đầu tư tiếp theo là nối chặt downloader với medallion processing, làm Silver/Gold theo dataset thực tế, dynamic hóa Airflow và productionize storage/observability.
