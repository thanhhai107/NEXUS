# Multi-source Ingestion Pipeline

## 1. Tổng quan

Dự án này xây dựng một pipeline ingestion dữ liệu từ nhiều nguồn khác nhau như API, CSV, database, CDC và streaming source. Mục tiêu chính là đưa dữ liệu từ các nguồn không đồng nhất vào hệ thống lakehouse một cách an toàn, có kiểm soát và có khả năng mở rộng.

Pipeline tập trung xử lý các vấn đề thường gặp trong multi-source ingestion, đặc biệt là:

- Nguồn trả về dữ liệu bị thiếu field
- Nguồn trả về field mới mà pipeline chưa biết
- Nguồn bỏ field mà pipeline đang sử dụng
- Tên field thay đổi
- Kiểu dữ liệu của field thay đổi
- Dữ liệu thiếu giá trị
- Dữ liệu không hợp lệ
- Dữ liệu trùng lặp
- Dữ liệu bất thường
- Dữ liệu không đồng bộ giữa các nguồn

Kiến trúc tổng thể đi theo mô hình Medallion Architecture:

```text
Source Systems
   ↓
Airflow Orchestration
   ↓
Ingestion Jobs
   ↓
Bronze Layer
   ↓
Validation & Governance Layer
   ↓
Silver Layer
   ↓
Gold Layer
```

Nguyên tắc thiết kế chính:

```text
Preserve raw data in Bronze,
validate and normalize data in Silver,
publish only trusted data in Gold,
and isolate critical failures in Quarantine.
```

---

## 2. Technology Stack

| Mục đích | Công nghệ sử dụng |
|---|---|
| Workflow orchestration | Apache Airflow |
| Lakehouse table format | Apache Iceberg |
| Distributed processing | Apache Spark |
| Data validation | Great Expectations |
| Data transformation | dbt |
| Metadata catalog, glossary, lineage | OpenMetadata |
| Quarantine storage | Apache Iceberg tables |
| Object storage | GCS / S3 / MinIO / HDFS |
| Alerting | Airflow callbacks, Email, Slack webhook |

Dự án này **không sử dụng**:

- Dagster
- Delta Lake
- DataHub
- Soda

---

## 3. Kiến trúc hệ thống

```text
┌──────────────────────────┐
│      Source Systems      │
│ API / CSV / DB / CDC /   │
│ Streaming Sources        │
└─────────────┬────────────┘
              │
              ▼
┌──────────────────────────┐
│         Airflow          │
│ DAG Scheduling & Control │
└─────────────┬────────────┘
              │
              ▼
┌──────────────────────────┐
│      Ingestion Jobs      │
│ Spark / Python Extractor │
└─────────────┬────────────┘
              │
              ▼
┌──────────────────────────┐
│      Bronze Layer        │
│ Iceberg Raw Tables       │
│ Raw Payload + Metadata   │
└─────────────┬────────────┘
              │
              ▼
┌──────────────────────────┐
│   Great Expectations     │
│ Schema & Quality Checks  │
└─────────────┬────────────┘
              │
      ┌───────┴────────┐
      ▼                ▼
┌──────────────┐   ┌────────────────┐
│ Silver Layer │   │ Quarantine Zone│
│ Clean Tables │   │ Invalid Records│
└──────┬───────┘   └────────────────┘
       │
       ▼
┌──────────────────────────┐
│          dbt             │
│ Gold Models & Tests      │
└─────────────┬────────────┘
              │
              ▼
┌──────────────────────────┐
│       Gold Layer         │
│ BI / API / ML / Reports  │
└──────────────────────────┘
```

---

## 4. Repository Structure

```text
.
├── dags/
│   ├── ingestion/
│   │   ├── environment_ingestion_dag.py
│   │   ├── transport_ingestion_dag.py
│   │   └── education_ingestion_dag.py
│   └── common/
│       ├── callbacks.py
│       └── airflow_utils.py
│
├── ingestion/
│   ├── sources/
│   │   ├── api_ingestor.py
│   │   ├── csv_ingestor.py
│   │   ├── database_ingestor.py
│   │   └── streaming_ingestor.py
│   ├── schema/
│   │   ├── schema_detector.py
│   │   ├── schema_comparator.py
│   │   └── schema_contract_loader.py
│   └── writers/
│       └── iceberg_writer.py
│
├── processing/
│   ├── bronze/
│   │   └── write_bronze.py
│   ├── silver/
│   │   ├── resolve_schema_drift.py
│   │   ├── normalize_schema.py
│   │   └── write_silver.py
│   └── quarantine/
│       └── write_quarantine.py
│
├── validation/
│   ├── great_expectations/
│   │   ├── expectations/
│   │   ├── checkpoints/
│   │   └── great_expectations.yml
│   └── run_checkpoint.py
│
├── dbt/
│   ├── models/
│   │   ├── silver/
│   │   └── gold/
│   ├── tests/
│   ├── macros/
│   └── dbt_project.yml
│
├── metadata/
│   ├── openmetadata/
│   │   ├── ingestion_pipeline.yml
│   │   ├── glossary.yml
│   │   └── lineage.yml
│   └── contracts/
│       ├── environment_contract.yml
│       ├── transport_contract.yml
│       └── education_contract.yml
│
├── configs/
│   ├── sources.yml
│   ├── iceberg.yml
│   ├── schema_drift_policy.yml
│   └── validation_rules.yml
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 5. Data Layer Design

### 5.1 Bronze Layer

Bronze Layer lưu dữ liệu raw hoặc gần raw nhất có thể. Đây là nơi đảm bảo dữ liệu gốc không bị mất, kể cả khi schema có vấn đề.

Bronze nên lưu:

- Raw payload
- Source name
- Source type
- Ingestion timestamp
- Actual schema
- Expected schema version
- Missing fields
- Detected new fields
- Schema drift type
- Quality status

Ví dụ Iceberg table:

```sql
CREATE TABLE IF NOT EXISTS nexus.bronze.raw_events (
    source_name STRING,
    source_type STRING,
    raw_payload STRING,
    actual_schema STRING,
    expected_schema_version STRING,
    missing_fields ARRAY<STRING>,
    detected_new_fields ARRAY<STRING>,
    schema_drift_type STRING,
    ingestion_time TIMESTAMP,
    quality_status STRING
)
USING iceberg
PARTITIONED BY (days(ingestion_time));
```

Bronze không nên clean hoặc drop dữ liệu quá sớm. Vai trò chính của Bronze là lưu vết, audit, debug và phục vụ backfill.

---

### 5.2 Silver Layer

Silver Layer lưu dữ liệu đã được chuẩn hóa và kiểm tra chất lượng.

Silver chịu trách nhiệm:

- Chuẩn hóa tên field
- Chuẩn hóa kiểu dữ liệu
- Chuẩn hóa timestamp
- Chuẩn hóa đơn vị đo
- Xử lý schema drift
- Gắn quality flag
- Deduplicate record
- Validate dữ liệu theo rule

Ví dụ Iceberg table:

```sql
CREATE TABLE IF NOT EXISTS nexus.silver.environment_measurements (
    station_id STRING,
    temperature DOUBLE,
    humidity DOUBLE,
    event_timestamp TIMESTAMP,
    source_name STRING,
    schema_version STRING,
    is_missing_required_field BOOLEAN,
    has_cast_failure BOOLEAN,
    quality_status STRING,
    ingestion_time TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_timestamp));
```

---

### 5.3 Gold Layer

Gold Layer chứa dữ liệu đã được kiểm duyệt để phục vụ dashboard, report, API, machine learning hoặc phân tích nghiệp vụ.

Gold chỉ nên expose các field đã:

- Được validate
- Ổn định
- Có documentation
- Được approve trong contract
- An toàn cho downstream usage

Gold không nên phụ thuộc trực tiếp vào schema raw từ source.

---

### 5.4 Quarantine Zone

Quarantine Zone lưu các record hoặc batch có lỗi nghiêm trọng.

Dữ liệu nên được đưa vào Quarantine khi:

- Thiếu required field
- Thiếu primary key
- Thiếu timestamp
- Parser fail
- Cast failure rate vượt threshold
- Schema change làm hỏng downstream model
- Dữ liệu vi phạm business rule nghiêm trọng

Ví dụ Iceberg quarantine table:

```sql
CREATE TABLE IF NOT EXISTS nexus.quarantine.invalid_records (
    source_name STRING,
    source_type STRING,
    raw_payload STRING,
    error_type STRING,
    error_message STRING,
    failed_field STRING,
    expected_schema STRING,
    actual_schema STRING,
    ingestion_time TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_time));
```

---

## 6. Schema Drift Handling Policy

| Schema Drift Issue | Handling Strategy | Detect / Resolve | Tools |
|---|---|---|---|
| Source returns missing fields | Optional fields are set to `NULL` or default. Required fields are quarantined. Non-critical fields continue with warning. | Detect by comparing actual schema with expected schema. Resolve in Silver using `NULL`, default value, and quality flags. | Great Expectations, Spark, dbt, Iceberg, Airflow |
| Source returns new unknown fields | Preserve new fields in Bronze. Do not expose them directly to Silver/Gold before review. | Detect using schema diff. Resolve with controlled schema evolution and metadata review. | Spark, Great Expectations, Iceberg, OpenMetadata, Airflow |
| Source drops fields used by pipeline | Treat as breaking change if the field is used by Silver, Gold, API, BI, or ML. | Detect using schema diff, contract check, and lineage impact. Resolve by updating contract, versioning schema, or quarantining data. | Great Expectations, dbt, OpenMetadata, Airflow, Iceberg |
| Field name changes | Map source-specific field names to canonical field names. Store aliases in metadata. | Detect by schema diff and semantic mapping. Resolve in Silver using alias mapping. | OpenMetadata, Spark, dbt, Great Expectations |
| Field type changes | Safe cast if possible. Quarantine incompatible or high-failure casts. | Detect by schema validation and cast failure rate. Resolve by controlled casting, versioned schema, or quarantine. | Spark, Great Expectations, dbt, Iceberg, Airflow |

---

## 7. Schema Drift Rules

### 7.1 Missing Field

**Problem**

Source payload không chứa field mà pipeline mong đợi.

Ví dụ:

```json
{
  "station_id": "S001",
  "temperature": 31.5,
  "timestamp": "2026-05-28T10:00:00Z"
}
```

Expected field:

```text
humidity
```

**Handling**

- Nếu field là optional, set `NULL` hoặc default value có kiểm soát.
- Nếu field không được downstream sử dụng, tiếp tục pipeline nhưng gắn warning.
- Nếu field là required, đưa record hoặc batch vào Quarantine.
- Nếu field là primary key, timestamp, join key hoặc metric input, block Silver/Gold publication.
- Lưu `missing_fields` và `quality_status` trong metadata của Bronze.

**Detection**

- So sánh actual schema với expected schema.
- Dùng Great Expectations để check required columns.
- Dùng Airflow callback để alert khi missing fields vượt threshold.

**Resolution**

- Bronze lưu raw payload.
- Silver tạo field bị thiếu với `NULL/default` nếu an toàn.
- Silver thêm quality flag như `is_missing_<field>`.
- Gold loại bỏ record không đạt completeness rule.

---

### 7.2 New Unknown Field

**Problem**

Source thêm field mới mà pipeline chưa biết.

Ví dụ:

```json
{
  "station_id": "S001",
  "temperature": 31.5,
  "humidity": 78,
  "air_quality_index": 42,
  "timestamp": "2026-05-28T10:00:00Z"
}
```

New field:

```text
air_quality_index
```

**Handling**

- Không drop field mới ngay.
- Lưu full raw payload trong Bronze.
- Ghi nhận field mới trong `detected_new_fields`.
- Nếu field không ảnh hưởng downstream, tiếp tục pipeline với warning.
- Nếu field ảnh hưởng Silver/Gold/API/report, cần review trước khi promote.
- Không expose field mới lên Gold nếu chưa có meaning, type, owner và validation rule.

**Detection**

- Schema diff giữa actual schema và expected schema.
- Great Expectations check unexpected columns.
- Airflow alert khi unknown fields xuất hiện lặp lại.

**Resolution**

- Bronze lưu raw payload.
- Silver chỉ promote field sau review.
- OpenMetadata lưu field description, owner, glossary term và lineage.
- dbt models và tests được cập nhật trước khi expose lên Gold.

---

### 7.3 Dropped Field Used by Pipeline

**Problem**

Source không còn trả về field mà pipeline đang sử dụng.

**Handling**

- Xem là breaking change nếu field đang dùng ở Silver, Gold, API, dashboard hoặc ML feature.
- Nếu field là required, fail task hoặc quarantine batch.
- Nếu field không critical, set `NULL` tạm thời và tiếp tục với warning.
- Nếu thay đổi là chính thức, version schema và cập nhật transformation logic.
- Tạo incident và thông báo cho source owner.

**Detection**

- Schema diff phát hiện expected field biến mất.
- Great Expectations fail required column checks.
- dbt tests fail downstream models.
- OpenMetadata lineage xác định affected assets.

**Resolution**

- Kiểm tra downstream impact bằng OpenMetadata lineage.
- Cập nhật source contract.
- Version schema nếu cần.
- Cập nhật Spark và dbt transformations.
- Block Gold models nếu metric phụ thuộc field bị bỏ.

---

### 7.4 Field Name Change

**Problem**

Source đổi tên field nhưng business meaning không đổi.

Ví dụ:

```text
temp        -> temperature
createdAt   -> event_timestamp
vehicleId   -> vehicle_id
```

**Handling**

- Không rename thủ công rải rác ở nhiều job.
- Duy trì central alias mapping.
- Lưu aliases trong OpenMetadata hoặc config file.
- Dùng Business Glossary để xác nhận ý nghĩa.
- Resolve field name ở Silver bằng canonical naming.
- LLM hoặc semantic matching chỉ dùng để gợi ý mapping, không tự động quyết định.

**Detection**

- Schema diff cho thấy field cũ biến mất và field mới xuất hiện.
- OpenMetadata alias hoặc glossary match được tìm thấy.
- Semantic similarity gợi ý mapping tiềm năng.

**Resolution**

- Bronze lưu tên field gốc từ source.
- Silver map source-specific names sang canonical names.
- dbt models chỉ dùng canonical field names.
- OpenMetadata lưu alias, glossary term và field owner.

Example mapping:

```yaml
field_mappings:
  environment_api:
    temp: temperature
    humid: humidity
    createdAt: event_timestamp

  transport_api:
    vehicleId: vehicle_id
    speedKmh: speed_kmh
    observedAt: event_timestamp
```

---

### 7.5 Field Type Change

**Problem**

Source thay đổi kiểu dữ liệu của field một cách bất ngờ.

Examples:

```text
temperature: DOUBLE -> STRING
speed: INTEGER -> DOUBLE
location: STRING -> OBJECT
tags: STRING -> ARRAY
```

**Handling**

- Phân loại thay đổi thành safe cast, risky cast hoặc incompatible change.
- Nếu safe cast được, cast ở Silver.
- Theo dõi cast failure rate.
- Nếu cast failure rate thấp, tiếp tục pipeline với warning.
- Nếu cast failure rate cao, quarantine affected records.
- Nếu incompatible change, version schema hoặc cập nhật parser.
- Luôn giữ raw value ở Bronze.

**Detection**

- Actual type khác expected type.
- Cast failure rate tăng.
- Nested structure hoặc array/object shape thay đổi.
- CSV header hoặc column order mismatch được phát hiện.

**Resolution**

- Bronze lưu raw value.
- Silver tạo normalized column.
- Silver thêm flag như `has_cast_failure`.
- Quarantine lưu record có incompatible type changes.
- Gold chỉ dùng record đã normalize thành công.

Example casting logic:

```sql
SELECT
    station_id,
    TRY_CAST(temperature AS DOUBLE) AS temperature,
    CASE
        WHEN TRY_CAST(temperature AS DOUBLE) IS NULL
             AND temperature IS NOT NULL
        THEN TRUE
        ELSE FALSE
    END AS has_temperature_cast_failure
FROM nexus.bronze.raw_events;
```

---

## 8. Airflow Setup

Airflow dùng để orchestrate ingestion, validation, transformation, quarantine routing và metadata update.

### 8.1 Local Setup

Tạo folder cần thiết:

```bash
mkdir -p dags logs plugins config
```

Khởi tạo Airflow bằng Docker Compose:

```bash
docker compose up airflow-init
```

Chạy Airflow:

```bash
docker compose up -d
```

Truy cập Airflow UI:

```text
http://localhost:8080
```

---

### 8.2 DAG Flow

Recommended DAG flow:

```text
extract_source_data
        ↓
detect_schema
        ↓
compare_schema
        ↓
write_bronze
        ↓
run_great_expectations
        ↓
branch_by_validation_result
        ↓
 ┌───────────────┬────────────────┐
 ▼               ▼                ▼
write_silver   write_quarantine  send_alert
        ↓
run_dbt_models
        ↓
update_openmetadata
```

---

### 8.3 Example DAG Skeleton

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime


def compare_schema(**context):
    """
    Compare actual schema with expected schema.
    Push schema drift result to XCom.
    """
    return "write_bronze"


def choose_validation_path(**context):
    """
    Decide whether to continue to Silver or route data to Quarantine.
    This function should read validation result from XCom or validation output.
    """
    validation_status = context["ti"].xcom_pull(task_ids="run_great_expectations")

    if validation_status == "critical_failure":
        return "write_quarantine"

    return "write_silver"


with DAG(
    dag_id="multi_source_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ingestion", "schema-drift", "iceberg"],
) as dag:

    extract_source_data = BashOperator(
        task_id="extract_source_data",
        bash_command="python ingestion/sources/api_ingestor.py"
    )

    detect_schema = BashOperator(
        task_id="detect_schema",
        bash_command="python ingestion/schema/schema_detector.py"
    )

    compare_schema_task = PythonOperator(
        task_id="compare_schema",
        python_callable=compare_schema
    )

    write_bronze = BashOperator(
        task_id="write_bronze",
        bash_command="spark-submit processing/bronze/write_bronze.py"
    )

    run_great_expectations = BashOperator(
        task_id="run_great_expectations",
        bash_command="python validation/run_checkpoint.py --checkpoint bronze_checkpoint"
    )

    branch_by_validation_result = BranchPythonOperator(
        task_id="branch_by_validation_result",
        python_callable=choose_validation_path
    )

    write_silver = BashOperator(
        task_id="write_silver",
        bash_command="spark-submit processing/silver/write_silver.py"
    )

    write_quarantine = BashOperator(
        task_id="write_quarantine",
        bash_command="spark-submit processing/quarantine/write_quarantine.py"
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd dbt && dbt run && dbt test"
    )

    update_openmetadata = BashOperator(
        task_id="update_openmetadata",
        bash_command="metadata ingest -c metadata/openmetadata/ingestion_pipeline.yml"
    )

    extract_source_data >> detect_schema >> compare_schema_task >> write_bronze
    write_bronze >> run_great_expectations >> branch_by_validation_result
    branch_by_validation_result >> [write_silver, write_quarantine]
    write_silver >> run_dbt_models >> update_openmetadata
```

---

## 9. Apache Iceberg Setup

Apache Iceberg dùng làm table format chính cho Bronze, Silver, Gold và Quarantine.

### 9.1 Spark Configuration

Ví dụ Spark configuration:

```properties
spark.sql.catalog.nexus=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.nexus.type=hadoop
spark.sql.catalog.nexus.warehouse=s3a://nexus-lakehouse/warehouse
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

Local development với MinIO:

```properties
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.access.key=minio
spark.hadoop.fs.s3a.secret.key=minio123
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

---

### 9.2 Create Namespaces

```sql
CREATE NAMESPACE IF NOT EXISTS nexus.bronze;
CREATE NAMESPACE IF NOT EXISTS nexus.silver;
CREATE NAMESPACE IF NOT EXISTS nexus.gold;
CREATE NAMESPACE IF NOT EXISTS nexus.quarantine;
```

---

### 9.3 Create Bronze Table

```sql
CREATE TABLE IF NOT EXISTS nexus.bronze.raw_events (
    source_name STRING,
    source_type STRING,
    raw_payload STRING,
    actual_schema STRING,
    expected_schema_version STRING,
    missing_fields ARRAY<STRING>,
    detected_new_fields ARRAY<STRING>,
    schema_drift_type STRING,
    ingestion_time TIMESTAMP,
    quality_status STRING
)
USING iceberg
PARTITIONED BY (days(ingestion_time));
```

---

### 9.4 Create Quarantine Table

```sql
CREATE TABLE IF NOT EXISTS nexus.quarantine.invalid_records (
    source_name STRING,
    source_type STRING,
    raw_payload STRING,
    error_type STRING,
    error_message STRING,
    failed_field STRING,
    expected_schema STRING,
    actual_schema STRING,
    ingestion_time TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingestion_time));
```

---

## 10. Great Expectations Setup

Great Expectations dùng để validate:

- Required columns
- Missing fields
- Data types
- Null values
- Range rules
- Invalid values
- Uniqueness
- Completeness

### 10.1 Install Great Expectations

```bash
pip install great_expectations
```

---

### 10.2 Initialize Great Expectations

```bash
great_expectations init
```

Recommended folder:

```text
validation/great_expectations/
├── expectations/
├── checkpoints/
├── plugins/
└── great_expectations.yml
```

---

### 10.3 Example Expectation Suite

Ví dụ validation rules cho environment measurements:

```json
{
  "expectation_suite_name": "environment_measurement_suite",
  "expectations": [
    {
      "expectation_type": "expect_column_to_exist",
      "kwargs": {
        "column": "station_id"
      }
    },
    {
      "expectation_type": "expect_column_to_exist",
      "kwargs": {
        "column": "event_timestamp"
      }
    },
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {
        "column": "station_id"
      }
    },
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {
        "column": "event_timestamp"
      }
    },
    {
      "expectation_type": "expect_column_values_to_be_between",
      "kwargs": {
        "column": "temperature",
        "min_value": -80,
        "max_value": 80
      }
    }
  ]
}
```

---

### 10.4 Run Great Expectations from Airflow

Recommended command:

```bash
python validation/run_checkpoint.py --checkpoint bronze_checkpoint
```

Example Airflow task:

```python
run_great_expectations = BashOperator(
    task_id="run_great_expectations",
    bash_command="python validation/run_checkpoint.py --checkpoint bronze_checkpoint"
)
```

Validation result nên được dùng để quyết định:

- Continue to Silver
- Continue with warning
- Send records to Quarantine
- Fail the DAG

---

## 11. dbt Setup

Dbt dùng cho transformation từ Silver sang Gold và downstream tests.

### 11.1 Example dbt Source

```yaml
version: 2

sources:
  - name: silver
    schema: silver
    tables:
      - name: environment_measurements
      - name: transport_events
```

---

### 11.2 Example dbt Model

```sql
-- dbt/models/gold/gold_environment_daily_metrics.sql

SELECT
    station_id,
    DATE(event_timestamp) AS metric_date,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity,
    COUNT(*) AS total_records
FROM {{ source('silver', 'environment_measurements') }}
WHERE quality_status = 'VALID'
GROUP BY
    station_id,
    DATE(event_timestamp)
```

---

### 11.3 Example dbt Tests

```yaml
version: 2

models:
  - name: gold_environment_daily_metrics
    columns:
      - name: station_id
        tests:
          - not_null

      - name: metric_date
        tests:
          - not_null

      - name: avg_temperature
        tests:
          - not_null
```

---

## 12. OpenMetadata Setup

OpenMetadata dùng cho:

- Dataset cataloging
- Column documentation
- Business glossary
- Field alias mapping
- Ownership
- Lineage
- Governance tracking

### 12.1 Register Iceberg Tables

Cần register các bảng sau vào OpenMetadata:

```text
nexus.bronze.raw_events
nexus.silver.environment_measurements
nexus.silver.transport_events
nexus.gold.environment_daily_metrics
nexus.gold.transport_daily_metrics
nexus.quarantine.invalid_records
```

---

### 12.2 Example Glossary

```yaml
glossary:
  name: Nexus Business Glossary
  terms:
    - name: station_id
      description: Unique identifier of an environmental or transport station.

    - name: event_timestamp
      description: Timestamp when the event was observed at the source.

    - name: temperature
      description: Temperature value normalized to Celsius.

    - name: speed_kmh
      description: Vehicle speed normalized to kilometers per hour.
```

---

### 12.3 Example Alias Mapping

```yaml
aliases:
  temperature:
    - temp
    - temperature_c
    - air_temperature

  event_timestamp:
    - timestamp
    - created_at
    - observed_at
    - recorded_at

  station_id:
    - station
    - station_code
    - sensor_id
```

---

### 12.4 Use OpenMetadata for Lineage Impact

Khi schema drift xảy ra, OpenMetadata được dùng để xác định:

- Silver table nào đang dùng field bị thay đổi
- Gold model nào phụ thuộc vào Silver table bị ảnh hưởng
- Dashboard hoặc API nào bị ảnh hưởng
- Ai là owner của dataset bị ảnh hưởng
- Vấn đề này chỉ là warning hay breaking change

---

## 13. Configuration Files

### 13.1 Source Configuration

```yaml
sources:
  environment_api:
    type: api
    schedule: daily
    expected_schema: environment_contract_v1
    critical_fields:
      - station_id
      - event_timestamp
      - temperature

  transport_api:
    type: api
    schedule: hourly
    expected_schema: transport_contract_v1
    critical_fields:
      - vehicle_id
      - event_timestamp
      - speed_kmh
```

---

### 13.2 Schema Drift Policy

```yaml
schema_drift_policy:
  missing_field:
    optional:
      action: continue_with_null
    required:
      action: quarantine
    downstream_unused:
      action: continue_with_warning

  new_unknown_field:
    optional:
      action: store_in_bronze
    downstream_impact:
      action: review_before_promote
    parser_failure:
      action: quarantine

  dropped_used_field:
    critical:
      action: fail_or_quarantine
    non_critical:
      action: continue_with_warning
    permanent_change:
      action: version_schema

  renamed_field:
    known_alias:
      action: map_to_canonical_name
    unknown_alias:
      action: review_required

  type_changed:
    safe_cast:
      action: cast_in_silver
    high_cast_failure_rate:
      action: quarantine
    incompatible:
      action: version_schema_or_quarantine
```

---

## 14. Recommended Execution Flow

### Step 1: Configure Sources

Update:

```text
configs/sources.yml
```

Define:

- Source name
- Source type
- Expected schema
- Critical fields
- Ingestion frequency

---

### Step 2: Define Data Contracts

Update:

```text
metadata/contracts/
```

Each contract should define:

- Expected fields
- Data types
- Required fields
- Optional fields
- Field descriptions
- Units
- Allowed values
- Owner

---

### Step 3: Create Iceberg Tables

Create:

- Bronze tables
- Silver tables
- Gold tables
- Quarantine tables

---

### Step 4: Create Great Expectations Suites

Create validation suites for:

- Bronze schema validation
- Silver completeness validation
- Type validation
- Range validation
- Required field validation

---

### Step 5: Configure Airflow DAGs

Each DAG should include:

- Extract source data
- Detect schema
- Compare schema
- Write Bronze
- Validate with Great Expectations
- Branch to Silver or Quarantine
- Run dbt models and tests
- Update OpenMetadata

---

### Step 6: Register Metadata in OpenMetadata

Register:

- Iceberg tables
- Column descriptions
- Owners
- Business glossary terms
- Lineage between Bronze, Silver, and Gold
- Validation results if supported by the integration

---

### Step 7: Run Pipeline

Trigger the DAG:

```bash
airflow dags trigger multi_source_ingestion
```

Monitor:

- Airflow task status
- Great Expectations validation results
- Iceberg table snapshots
- Quarantine records
- dbt test results
- OpenMetadata lineage

---

## 15. Operational Rules

### 15.1 Continue Pipeline When

Continue pipeline khi:

- Missing field là optional
- New unknown field không gây hại
- Type change có thể safe cast
- Field rename có known alias
- Data quality issue dưới threshold

---

### 15.2 Quarantine Data When

Quarantine data khi:

- Required field bị thiếu
- Primary key bị thiếu
- Timestamp bị thiếu
- Type change incompatible
- Parser fail
- Cast failure rate vượt threshold
- Schema drift ảnh hưởng Gold model

---

### 15.3 Create Incident When

Tạo incident khi:

- Required field biến mất lặp lại nhiều lần
- Source bỏ field đang được downstream sử dụng
- Schema drift làm hỏng Gold metrics
- Quarantine rate vượt threshold
- Field meaning thay đổi mà không cập nhật contract
- OpenMetadata lineage cho thấy downstream impact nghiêm trọng

---

## 16. Summary

Framework ingestion này sử dụng:

```text
Airflow for orchestration
Iceberg for lakehouse storage
Spark for ingestion and processing
Great Expectations for validation
dbt for transformation and downstream tests
OpenMetadata for catalog, glossary, ownership, and lineage
```

Hệ thống được thiết kế để multi-source ingestion an toàn hơn bằng cách tách rõ raw preservation, validation, standardization và trusted publication qua Bronze, Silver, Gold và Quarantine layers.
