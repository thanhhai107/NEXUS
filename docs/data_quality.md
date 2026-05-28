# Data Quality Painpoints — README triển khai cho Multi-source Ingestion

Tài liệu này phân tích đầy đủ các painpoint trong nhóm **Data Quality Issues** và mô tả cách detect, resolve, lưu audit/quarantine, cũng như tech stack nên dùng để agent code có thể áp dụng vào hệ thống.

Stack mặc định của dự án:

- **Orchestration:** Apache Airflow
- **Storage/Table format:** Apache Iceberg
- **Validation:** Great Expectations
- **Metadata/Governance:** OpenMetadata
- **Processing:** Apache Spark / Spark SQL / SparkML
- **Transformation:** dbt cho các model SQL từ Silver/Gold, Spark cho xử lý nặng
- **Isolation:** Quarantine Zone cho bản ghi lỗi, nghi ngờ hoặc cần review

---

## 1. Nguyên tắc thiết kế chung

### 1.1. Không sửa trực tiếp dữ liệu gốc

Dữ liệu từ source phải được lưu vào Raw/Bronze ở dạng gần nguyên bản nhất có thể. Mọi thao tác clean, normalize, cast, deduplicate hoặc reconcile phải tạo ra dữ liệu mới ở Silver, kèm metadata giải thích cách xử lý.

Nguyên tắc:

```text
Source
  -> Raw Landing
  -> Bronze: lưu dữ liệu gần nguyên bản + validate cơ bản
  -> Quarantine: lưu bản ghi lỗi/nghi ngờ
  -> Silver: chuẩn hóa, sửa lỗi có kiểm soát, deduplicate, reconcile
  -> Gold: metric/business output
```

### 1.2. Detect sớm, resolve có kiểm soát

- **Bronze:** phát hiện lỗi càng sớm càng tốt: null, invalid range, duplicate key, schema mismatch, outlier cơ bản.
- **Quarantine:** cô lập record lỗi nghiêm trọng để không làm bẩn pipeline chính.
- **Silver:** resolve bằng rule rõ ràng: cast, normalize, impute, deduplicate, merge, reconcile.
- **Gold:** chỉ dùng dữ liệu đã qua chuẩn hóa, không xử lý lỗi raw ở Gold.

### 1.3. Mọi lỗi phải có audit trail

Mỗi record lỗi hoặc nghi ngờ cần có:

- `run_id`
- `source_name`
- `dataset_name`
- `layer`
- `record_key`
- `issue_category`
- `issue_code`
- `severity`
- `rule_id`
- `expected_value`
- `actual_value`
- `raw_payload`
- `detected_at`
- `action_taken`
- `status`

Không nên chỉ fail pipeline mà không lưu lại lý do.

### 1.4. Phân loại mức độ lỗi

| Severity | Ý nghĩa | Hành động mặc định |
|---|---|---|
| `critical` | Lỗi làm dữ liệu không thể dùng được hoặc phá vỡ downstream | Quarantine record hoặc fail dataset |
| `high` | Lỗi ảnh hưởng trực tiếp đến business logic | Quarantine record, tiếp tục pipeline nếu dưới threshold |
| `medium` | Lỗi có thể xử lý tự động ở Silver | Flag + resolve ở Silver |
| `low` | Lỗi nhẹ, cần theo dõi metric | Ghi audit metric, không chặn pipeline |

### 1.5. Bảng Iceberg khuyến nghị

```sql
-- Bảng lưu dữ liệu raw/bronze theo từng source
raw.<source_name>_<dataset_name>
bronze.<dataset_name>

-- Bảng quarantine dùng chung
quarantine.dq_issues

-- Bảng audit theo từng lần chạy
audit.dq_run_summary

-- Bảng dữ liệu đã chuẩn hóa
silver.<dataset_name>

-- Bảng phục vụ phân tích/reporting
gold.<domain_metric_or_mart>
```

Schema gợi ý cho `quarantine.dq_issues`:

```sql
CREATE TABLE IF NOT EXISTS quarantine.dq_issues (
    run_id STRING,
    source_name STRING,
    dataset_name STRING,
    layer STRING,
    record_key STRING,
    issue_category STRING,
    issue_code STRING,
    severity STRING,
    rule_id STRING,
    column_name STRING,
    expected_value STRING,
    actual_value STRING,
    raw_payload STRING,
    detected_at TIMESTAMP,
    action_taken STRING,
    status STRING,
    resolved_at TIMESTAMP,
    resolver_note STRING
)
USING iceberg;
```

Schema gợi ý cho `audit.dq_run_summary`:

```sql
CREATE TABLE IF NOT EXISTS audit.dq_run_summary (
    run_id STRING,
    source_name STRING,
    dataset_name STRING,
    layer STRING,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    status STRING,
    total_rows BIGINT,
    passed_rows BIGINT,
    failed_rows BIGINT,
    quarantined_rows BIGINT,
    warning_rows BIGINT,
    metrics_json STRING
)
USING iceberg;
```

---

## 2. Tổng quan painpoint

| Painpoint | Detect chủ yếu ở đâu | Resolve chủ yếu ở đâu | Công cụ chính |
|---|---|---|---|
| Invalid data | Bronze | Silver / Quarantine | Great Expectations, Data Contract, Spark, Iceberg |
| Outlier | Bronze + Silver | Silver / Quarantine | Great Expectations, Spark, SparkML |
| Missing values | Bronze | Silver / Quarantine | Great Expectations, Spark, dbt |
| Duplicate data | Bronze + Silver | Silver | Great Expectations, Spark, dbt |
| Inconsistency across sources | Silver | Silver / Quarantine | Spark, dbt, OpenMetadata, reference tables |

---

# 3. Painpoint 1 — Dữ liệu không hợp lệ (Invalid)

## 3.1. Định nghĩa

Dữ liệu không hợp lệ là dữ liệu không thỏa mãn các rule đã định nghĩa trước trong data contract hoặc expectation suite.

Ví dụ phổ biến:

- Giá trị nằm ngoài range cho phép.
- Field bắt buộc bị null.
- Primary key bị null hoặc trùng.
- Foreign key không tồn tại trong bảng tham chiếu.
- Giá trị không nằm trong enum hợp lệ.
- Sai format: email, phone number, timestamp, code, license plate.
- Giá trị âm ở field không được âm, ví dụ `speed < 0`, `distance < 0`.
- Timestamp tương lai bất hợp lý, ví dụ sensor event time lớn hơn ingestion time quá nhiều.
- Geometry không hợp lệ nếu có dữ liệu không gian.

## 3.2. Mục tiêu xử lý

- Không để dữ liệu phá vỡ logic downstream.
- Phân biệt lỗi có thể sửa tự động và lỗi phải quarantine.
- Ghi đầy đủ audit để biết rule nào fail.
- Cho phép pipeline tiếp tục nếu lỗi nằm dưới ngưỡng chấp nhận.

## 3.3. Detect

### Detect ở Bronze

Áp dụng Great Expectations và Spark validation ngay sau khi dữ liệu được load vào Bronze.

Rule nên có:

| Nhóm rule | Ví dụ expectation |
|---|---|
| Not null | `expect_column_values_to_not_be_null` |
| Primary key | `expect_column_values_to_be_unique` + not null |
| Range | `expect_column_values_to_be_between` |
| Enum | `expect_column_values_to_be_in_set` |
| Regex/format | `expect_column_values_to_match_regex` |
| Type | kiểm tra bằng schema contract hoặc Spark schema |
| Foreign key | Spark left anti join với reference/dimension table |
| Row count | kiểm tra số lượng bản ghi tối thiểu/tối đa |
| Freshness | kiểm tra dữ liệu có được cập nhật trong SLA không |

Ví dụ rule theo data contract:

```yaml
dataset: transport_traffic_events
owner: data-platform
layer: bronze

primary_key:
  - source_system
  - event_id

columns:
  event_id:
    type: string
    required: true
    unique_with:
      - source_system

  vehicle_speed_kmh:
    type: double
    required: false
    min: 0
    max: 200

  event_time:
    type: timestamp
    required: true
    max_delay_minutes_from_ingestion_time: 1440

  road_id:
    type: string
    required: true
    foreign_key:
      table: silver.dim_road
      column: road_id

quality_thresholds:
  critical_failure_ratio: 0.01
  warning_failure_ratio: 0.05
```

### Detect bằng foreign key check

Foreign key thường không nên chỉ dùng Great Expectations nếu cần join lớn. Với dữ liệu lớn, nên dùng Spark:

```sql
SELECT b.*
FROM bronze.transport_traffic_events b
LEFT ANTI JOIN silver.dim_road r
ON b.road_id = r.road_id;
```

Kết quả left anti join là các record có `road_id` không tồn tại trong bảng chuẩn.

## 3.4. Resolve

### Rule resolve mặc định

| Loại lỗi | Hành động |
|---|---|
| Required field null | Quarantine nếu field critical |
| Invalid range nhẹ | Flag hoặc cap nếu business cho phép |
| Invalid range nghiêm trọng | Quarantine |
| Invalid enum | Map bằng reference table nếu có alias, nếu không quarantine |
| Invalid timestamp | Chuẩn hóa timezone nếu parse được, nếu không quarantine |
| FK không tồn tại | Quarantine hoặc đưa vào pending nếu dimension đến muộn |
| Duplicate primary key | Chuyển sang duplicate handling, không tự ghi đè |

### Không nên làm

- Không tự sửa giá trị nếu không có rule rõ ràng.
- Không impute field định danh như `id`, `event_id`, `source_id`.
- Không drop record lỗi mà không ghi vào quarantine.
- Không sửa dữ liệu ở Gold.

## 3.5. Stage áp dụng

| Stage | Việc cần làm |
|---|---|
| Pre-ingestion | Đọc data contract, xác định schema/rule bắt buộc |
| Bronze | Validate schema, not null, range, enum, primary key |
| Quarantine | Lưu record fail rule nghiêm trọng |
| Silver | Normalize giá trị có thể sửa, join reference, tạo clean columns |
| Gold | Chỉ consume dữ liệu đã pass hoặc đã được resolve |

## 3.6. Tech stack

| Tool | Vai trò |
|---|---|
| Great Expectations | Validate not null, range, enum, regex, uniqueness |
| Spark/Spark SQL | Validate FK, xử lý volume lớn, tách pass/fail |
| Iceberg | Lưu Bronze/Silver/Quarantine/Audit tables |
| Airflow | Orchestrate validation task và branch theo kết quả |
| OpenMetadata | Lưu owner, schema, lineage, data contract, quality status |
| dbt | Rule SQL ở Silver/Gold nếu transformation dạng SQL |

---

# 4. Painpoint 2 — Dữ liệu bất thường (Outlier)

## 4.1. Định nghĩa

Outlier là dữ liệu không nhất thiết sai theo rule cứng, nhưng có giá trị bất thường so với phân phối dữ liệu, ngữ cảnh thời gian, không gian hoặc business domain.

Ví dụ:

- Nhiệt độ môi trường `80°C` ở thành phố bình thường.
- Tốc độ xe `250 km/h` trong dữ liệu đô thị.
- AQI tăng đột biến so với rolling average.
- Sensor gửi giá trị spike trong vài giây.
- Số lượt phương tiện trên một tuyến đường cao hơn trung bình 20 lần.
- Một trường học có số học sinh tăng quá mạnh so với kỳ trước.

## 4.2. Phân biệt Invalid và Outlier

| Tiêu chí | Invalid | Outlier |
|---|---|---|
| Bản chất | Sai theo rule cứng | Bất thường theo thống kê/ngữ cảnh |
| Ví dụ | `speed = -10` | `speed = 180` trong khu đô thị |
| Xử lý | Quarantine hoặc sửa bằng rule | Flag, review, cap, model-based detection |
| Detect | Bronze | Bronze + Silver |
| Cần lịch sử dữ liệu | Không nhất thiết | Thường cần |

## 4.3. Detect

### Nhóm phương pháp detect

| Phương pháp | Khi dùng |
|---|---|
| Domain threshold | Khi có ngưỡng business rõ ràng |
| IQR | Dữ liệu numeric ổn định, ít giả định phân phối |
| Z-score | Dữ liệu gần phân phối chuẩn |
| Rolling window | Time-series, sensor, traffic, AQI |
| Percentile | Muốn cắt theo phân vị P1/P99 |
| Isolation Forest | Nhiều feature, outlier phức tạp |
| Clustering-based | Khi muốn phát hiện điểm lệch khỏi cụm |
| Spatial outlier | Dữ liệu có tọa độ/địa lý |

### Detect bằng rule cứng ở Bronze

Ví dụ:

```yaml
columns:
  vehicle_speed_kmh:
    type: double
    min: 0
    max: 200
```

Nếu `speed < 0` thì đây là invalid, không chỉ là outlier.

### Detect bằng IQR ở Silver

Công thức:

```text
IQR = Q3 - Q1
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR
```

Áp dụng theo từng group nếu dữ liệu có ngữ cảnh:

```text
group by city_id, road_type, hour_of_day
```

Không nên tính outlier toàn cục nếu dữ liệu có nhiều vùng/loại khác nhau.

### Detect bằng rolling window

Dùng cho streaming hoặc dữ liệu time-series:

```text
rolling_mean = average(value) over last 7 days
rolling_std = stddev(value) over last 7 days
outlier if abs(value - rolling_mean) > 3 * rolling_std
```

### Detect bằng SparkML Isolation Forest hoặc model tương đương

Dùng khi:

- Có nhiều feature.
- Rule thống kê đơn giản không đủ.
- Dữ liệu lớn.
- Outlier phụ thuộc vào tổ hợp nhiều cột.

Ví dụ feature cho traffic:

```text
vehicle_speed_kmh
traffic_volume
hour_of_day
road_type_encoded
weather_condition_encoded
```

## 4.4. Resolve

| Trường hợp | Hành động |
|---|---|
| Chắc chắn sai | Quarantine |
| Nghi ngờ | Giữ record, thêm flag `is_outlier = true` |
| Outlier nhưng có thể hợp lệ | Không xóa, chỉ cảnh báo |
| Spike do sensor lỗi | Có thể smooth/interpolate nếu business cho phép |
| Outlier ảnh hưởng metric | Tạo clean value riêng, ví dụ `speed_kmh_clean` |
| Cần review | Đưa vào queue/manual review |

Các cột nên thêm ở Silver:

```text
is_outlier
outlier_method
outlier_score
outlier_reason
original_value
clean_value
```

## 4.5. Stage áp dụng

| Stage | Việc cần làm |
|---|---|
| Bronze | Rule cứng: min/max bất khả thi |
| Silver | Statistical/contextual outlier detection |
| Quarantine | Lưu record chắc chắn sai hoặc vượt ngưỡng nghiêm trọng |
| Gold | Có thể loại outlier khỏi metric hoặc tính metric riêng có/không outlier |

## 4.6. Tech stack

| Tool | Vai trò |
|---|---|
| Great Expectations | Kiểm tra range cứng và rule đơn giản |
| Spark SQL | Tính percentile, IQR, rolling window |
| SparkML | Isolation Forest/model-based detection |
| Iceberg | Lưu outlier flag, audit, quarantine |
| Airflow | Chạy batch outlier detection định kỳ |
| OpenMetadata | Ghi data quality metric và lineage |

---

# 5. Painpoint 3 — Dữ liệu bị thiếu (Missing)

## 5.1. Định nghĩa

Dữ liệu bị thiếu là trường/cột có tồn tại trong schema nhưng giá trị bị null, empty, unknown hoặc không có thông tin hợp lệ.

Không nhầm với schema drift kiểu “source bỏ hẳn field”. Painpoint này xử lý trường hợp field vẫn tồn tại nhưng giá trị thiếu.

Ví dụ:

- `station_id` null.
- `temperature` null.
- `event_time` null.
- Chuỗi rỗng `""` nhưng đáng lẽ phải là null.
- Giá trị placeholder như `N/A`, `unknown`, `-999`.
- Thiếu tọa độ `lat/lon`.
- Missing theo điều kiện, ví dụ `end_time` bắt buộc nếu `status = completed`.

## 5.2. Phân loại missing

| Loại missing | Ví dụ | Hành động |
|---|---|---|
| Critical missing | `event_id`, `event_time`, `source_id` null | Quarantine |
| Business missing | `road_id`, `station_id` null | Quarantine hoặc enrich |
| Optional missing | `description`, `note` null | Cho qua, ghi metric |
| Conditional missing | `closed_at` null khi `status = closed` | Validate theo điều kiện |
| Placeholder missing | `N/A`, `unknown`, `-999` | Normalize thành null rồi xử lý |

## 5.3. Detect

### Detect ở Bronze

Rule cần có:

```yaml
columns:
  event_id:
    required: true
    missing_tokens: ["", "null", "NULL", "N/A", "unknown"]

  event_time:
    required: true

  temperature_celsius:
    required: false
    missing_threshold_ratio: 0.1
```

Great Expectations rule:

- `expect_column_values_to_not_be_null`
- `expect_column_proportion_of_non_null_values_to_be_between`
- custom expectation cho missing token
- conditional expectation cho rule phụ thuộc giá trị cột khác

### Detect missing ratio

Theo dõi tỷ lệ null theo từng source và từng run:

```text
missing_ratio = null_count / total_rows
```

Nếu source A có `temperature` null 2% bình thường, nhưng hôm nay 70%, đó là vấn đề chất lượng hoặc source issue.

### Detect missing theo nhóm

Nên tính missing theo:

- `source_name`
- `dataset_name`
- `ingestion_date`
- `city_id`
- `station_id`
- `sensor_type`

Vì null toàn cục có thể che giấu lỗi cục bộ.

## 5.4. Resolve

### Decision matrix

| Trường hợp | Resolve |
|---|---|
| Critical field null | Quarantine |
| Optional field null | Giữ null, thêm metric |
| Missing do placeholder | Convert placeholder thành null |
| Có nguồn thay thế đáng tin | Enrich từ source ưu tiên |
| Có thể suy luận an toàn | Impute ở Silver và lưu flag |
| Không thể suy luận | Giữ null + `missing_reason` nếu biết |
| Missing ratio vượt threshold | Fail hoặc pause pipeline |

### Imputation policy

Chỉ impute khi có rule được phê duyệt trong contract.

Ví dụ:

| Field type | Cách impute có thể dùng |
|---|---|
| Numeric stable | median theo group |
| Time-series | forward fill, interpolation |
| Category | mode theo group |
| Location | join từ dimension table nếu có station_id |
| ID/key | Không impute |
| Timestamp critical | Không impute nếu không có nguồn tin cậy |

Cột nên thêm ở Silver:

```text
is_imputed
imputation_method
imputation_source
missing_reason
original_value
```

## 5.5. Stage áp dụng

| Stage | Việc cần làm |
|---|---|
| Bronze | Detect required null, missing token, missing ratio |
| Quarantine | Cô lập record thiếu field critical |
| Silver | Normalize placeholder, impute/enrich nếu có rule |
| Gold | Dùng clean value, có thể loại record imputed khỏi một số metric nhạy cảm |

## 5.6. Tech stack

| Tool | Vai trò |
|---|---|
| Great Expectations | Validate null, completeness ratio |
| Spark SQL | Tính missing ratio theo group/source |
| dbt | Normalize null/placeholder ở Silver nếu SQL đơn giản |
| Iceberg | Lưu audit, quarantine, silver flags |
| OpenMetadata | Theo dõi completeness metric |
| Airflow | Fail/branch theo threshold |

---

# 6. Painpoint 4 — Dữ liệu bị trùng (Duplicate)

## 6.1. Định nghĩa

Duplicate là trường hợp cùng một thực thể, sự kiện hoặc record xuất hiện nhiều lần trong một hoặc nhiều source.

Duplicate không chỉ là hai dòng giống hệt nhau. Trong multi-source ingestion, duplicate có nhiều dạng:

| Loại duplicate | Ví dụ |
|---|---|
| Exact duplicate | Hai dòng giống toàn bộ |
| Primary key duplicate | Trùng `event_id` |
| Composite key duplicate | Trùng `source_id + event_time + sensor_id` |
| Business duplicate | Cùng sự kiện nhưng khác id |
| Near duplicate | Tên/địa chỉ gần giống, cùng entity |
| Cross-source duplicate | Source A và B cùng nói về một entity |
| Event replay duplicate | Streaming gửi lại cùng event |
| Late duplicate | Record cũ được gửi lại ở batch sau |

## 6.2. Detect

### Exact duplicate

Tạo hash từ toàn bộ payload hoặc tập cột quan trọng:

```text
row_hash = sha2(concat_ws("||", col1, col2, col3, ...), 256)
```

Sau đó kiểm tra:

```sql
SELECT row_hash, COUNT(*)
FROM bronze.dataset
GROUP BY row_hash
HAVING COUNT(*) > 1;
```

### Primary key duplicate

Dùng Great Expectations:

```yaml
expectations:
  - type: expect_column_values_to_be_unique
    column: event_id
```

Hoặc Spark:

```sql
SELECT event_id, COUNT(*)
FROM bronze.dataset
GROUP BY event_id
HAVING COUNT(*) > 1;
```

### Composite key duplicate

Ví dụ traffic event không có event_id ổn định:

```text
natural_key = source_name + sensor_id + event_time + event_type
```

Check duplicate:

```sql
SELECT source_name, sensor_id, event_time, event_type, COUNT(*)
FROM bronze.traffic_events
GROUP BY source_name, sensor_id, event_time, event_type
HAVING COUNT(*) > 1;
```

### Cross-source duplicate / entity duplicate

Dùng rule matching hoặc similarity:

- Exact match theo canonical id.
- Rule-based matching theo tên, địa chỉ, tọa độ, timestamp.
- Fuzzy matching cho text.
- Probabilistic matching cho entity resolution.
- Similarity search nếu có embedding hoặc vector index.

## 6.3. Resolve

### Deduplicate bằng window function

Giữ bản ghi tốt nhất theo rule:

```sql
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY event_id
               ORDER BY ingestion_time DESC, source_priority ASC
           ) AS rn
    FROM bronze.dataset
)
WHERE rn = 1;
```

### Survivorship rule

Khi nhiều record nói về cùng một entity, cần rule chọn giá trị cuối cùng:

| Rule | Khi dùng |
|---|---|
| Latest timestamp wins | Dữ liệu cập nhật theo thời gian |
| Source priority wins | Có source tin cậy hơn |
| Non-null wins | Merge nhiều source để lấp field thiếu |
| Majority vote | Nhiều source cùng cung cấp một giá trị |
| Manual review | Conflict nghiêm trọng |

### Output nên có ở Silver

```text
canonical_entity_id
dedup_group_id
is_duplicate
survivorship_rule
selected_record_flag
duplicate_count
```

Không nên xóa hoàn toàn duplicate. Nên lưu lại duplicate group để audit.

## 6.4. Stage áp dụng

| Stage | Việc cần làm |
|---|---|
| Bronze | Detect exact duplicate, primary key duplicate, event replay |
| Silver | Deduplicate, merge, canonical entity id |
| Quarantine | Lưu duplicate nghiêm trọng không thể chọn winner |
| Gold | Chỉ dùng record canonical hoặc selected record |

## 6.5. Tech stack

| Tool | Vai trò |
|---|---|
| Great Expectations | Check uniqueness |
| Spark SQL | Deduplicate bằng hash/window/group by |
| dbt | Implement dedup model nếu logic SQL |
| Iceberg | MERGE INTO, time travel, audit duplicate group |
| OpenMetadata | Lưu lineage và owner của canonical table |
| Airflow | Chạy dedup task sau validation |
| Optional: Splink | Entity resolution/fuzzy/probabilistic matching |

---

# 7. Painpoint 5 — Dữ liệu không đồng bộ / không nhất quán giữa các nguồn (Inconsistency)

## 7.1. Định nghĩa

Inconsistency là khi nhiều nguồn cùng mô tả một entity, event hoặc metric nhưng trả về giá trị không thống nhất.

Ví dụ:

- Source A nói `station_name = "Ha Noi Center"`, source B nói `"Hanoi Centre"`.
- Source A trả về nhiệt độ theo Celsius, source B theo Fahrenheit nhưng không ghi rõ.
- Source A cập nhật traffic mỗi phút, source B cập nhật mỗi giờ.
- Source A dùng `active = true`, source B dùng `status = operating`.
- Một nguồn nói road segment đang mở, nguồn khác nói đang đóng.
- Một trường học có số học sinh khác nhau giữa hai nguồn.
- AQI cùng thời điểm nhưng lệch quá lớn giữa sensor và API tổng hợp.

## 7.2. Nguyên nhân thường gặp

| Nguyên nhân | Ví dụ |
|---|---|
| Khác định nghĩa | `traffic_volume` là per minute ở source A, per hour ở source B |
| Khác đơn vị | km/h vs m/s, Celsius vs Fahrenheit |
| Khác timezone | UTC vs local time |
| Khác granularity | daily vs hourly |
| Khác source freshness | source A mới hơn source B |
| Khác reference data | mã tỉnh/thành, mã trạm, mã tuyến đường |
| Khác logic tính toán | AQI theo chuẩn khác nhau |
| Lỗi source | source gửi dữ liệu cũ hoặc cache |

## 7.3. Detect

### Chuẩn hóa trước khi so sánh

Không so sánh trực tiếp dữ liệu nếu chưa chuẩn hóa:

```text
unit -> canonical unit
timezone -> UTC hoặc timezone chuẩn của domain
reference code -> canonical reference id
granularity -> cùng mức aggregation
entity id -> canonical_entity_id
```

### Cross-source reconciliation

Tạo bảng reconciliation ở Silver:

```text
silver.<dataset>_reconciliation
```

Cột gợi ý:

```text
canonical_entity_id
attribute_name
source_name
source_value
canonical_value
value_timestamp
source_priority
is_conflict
conflict_score
resolution_rule
```

### Rule phát hiện conflict

| Loại conflict | Rule detect |
|---|---|
| Numeric | difference > threshold |
| Category | values differ sau khi normalize |
| Timestamp | source lag vượt SLA |
| Unit | unit missing hoặc không map được |
| Reference code | code không map được sang canonical |
| Entity | nhiều source map vào cùng entity nhưng attribute mâu thuẫn |

Ví dụ numeric conflict:

```text
abs(source_a.temperature_c - source_b.temperature_c) > 3
```

Ví dụ timestamp freshness conflict:

```text
source_a.updated_at - source_b.updated_at > allowed_lag
```

## 7.4. Resolve

### Resolve strategy

| Strategy | Khi dùng |
|---|---|
| Canonical standard | Chuẩn hóa unit, timezone, code, naming |
| Source priority | Một source đáng tin hơn source khác |
| Latest timestamp wins | Dữ liệu có tính cập nhật cao |
| Majority consensus | Nhiều nguồn độc lập cùng cung cấp |
| Weighted confidence | Mỗi source có độ tin cậy khác nhau |
| Manual review/quarantine | Conflict nghiêm trọng |
| Keep multi-values | Khi không nên ép về một giá trị duy nhất |

### Ví dụ source priority

```yaml
source_priority:
  official_gov_api: 1
  city_open_data: 2
  third_party_api: 3
  scraped_data: 4
```

Nếu cùng một field có nhiều giá trị, chọn giá trị từ source có priority nhỏ nhất, trừ khi source đó đã quá stale.

### Ví dụ latest timestamp wins

```sql
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY canonical_entity_id, attribute_name
               ORDER BY value_timestamp DESC, source_priority ASC
           ) AS rn
    FROM silver.source_attribute_values
)
WHERE rn = 1;
```

### Khi nào đưa vào Quarantine

Nên quarantine hoặc đưa vào review queue nếu:

- Hai nguồn trusted mâu thuẫn lớn.
- Không xác định được unit/timezone.
- Không map được entity.
- Conflict ảnh hưởng trực tiếp đến metric quan trọng.
- Giá trị lệch vượt threshold nhiều lần liên tiếp.
- Không có rule chọn winner an toàn.

## 7.5. Stage áp dụng

| Stage | Việc cần làm |
|---|---|
| Bronze | Lưu từng source riêng, chưa ép canonical quá sớm |
| Silver | Chuẩn hóa unit/timezone/reference/entity, reconcile cross-source |
| Quarantine | Lưu conflict nghiêm trọng |
| Gold | Dùng canonical value, không dùng trực tiếp source-specific value |

## 7.6. Tech stack

| Tool | Vai trò |
|---|---|
| Spark SQL | Join nhiều source, reconcile, tính conflict |
| dbt | Semantic model, chuẩn hóa metric, rule SQL |
| OpenMetadata | Lưu owner, glossary, data contract, lineage, source description |
| Iceberg | Lưu source-specific table và canonical table |
| Great Expectations | Validate conformance sau chuẩn hóa |
| Airflow | Orchestrate reconciliation workflow |
| Reference tables | Unit mapping, source priority, entity mapping, code mapping |

---

# 8. Data contract chuẩn cho từng dataset

Agent code nên tạo một file contract cho mỗi dataset.

Ví dụ:

```yaml
dataset: environment_air_quality
domain: environment
owner: data-platform
description: Air quality observations from multiple sources

storage:
  raw_table: raw.environment_air_quality
  bronze_table: bronze.environment_air_quality
  silver_table: silver.environment_air_quality
  quarantine_table: quarantine.dq_issues

primary_key:
  - source_name
  - station_id
  - observed_at

source_priority:
  official_environment_api: 1
  city_sensor_network: 2
  third_party_provider: 3

columns:
  source_name:
    type: string
    required: true

  station_id:
    type: string
    required: true

  observed_at:
    type: timestamp
    required: true
    timezone: UTC

  aqi:
    type: integer
    required: false
    min: 0
    max: 500
    outlier:
      method: iqr
      group_by: ["station_id", "hour_of_day"]
      severity: medium

  pm25:
    type: double
    required: false
    min: 0
    max: 1000
    unit: ug/m3
    missing_threshold_ratio: 0.2

quality_rules:
  - id: dq_required_station_id
    category: missing
    severity: critical
    check: not_null
    column: station_id
    on_fail: quarantine

  - id: dq_aqi_valid_range
    category: invalid
    severity: high
    check: between
    column: aqi
    min: 0
    max: 500
    on_fail: quarantine

  - id: dq_pm25_outlier
    category: outlier
    severity: medium
    check: iqr
    column: pm25
    group_by: ["station_id"]
    on_fail: flag

thresholds:
  fail_pipeline_if_critical_ratio_gt: 0.01
  warn_if_warning_ratio_gt: 0.05
  fail_pipeline_if_total_rows_lt: 1
```

---

# 9. Airflow DAG flow khuyến nghị

DAG nên có các task chính:

```text
extract_source
  -> load_raw
  -> load_bronze
  -> validate_bronze_with_gx
  -> split_pass_fail_records
  -> write_quarantine
  -> write_dq_audit
  -> branch_by_quality_threshold
       -> stop_or_alert
       -> transform_silver
            -> detect_outliers
            -> deduplicate
            -> reconcile_sources
            -> validate_silver
            -> publish_metadata
            -> build_gold
```

Pseudo DAG:

```python
extract_source >> load_raw >> load_bronze
load_bronze >> validate_bronze
validate_bronze >> split_pass_fail
split_pass_fail >> write_quarantine
split_pass_fail >> write_audit
write_audit >> branch_by_threshold
branch_by_threshold >> stop_or_alert
branch_by_threshold >> transform_silver
transform_silver >> validate_silver >> publish_metadata >> build_gold
```

Branching rule:

```text
if critical_failure_ratio > contract.thresholds.fail_pipeline_if_critical_ratio_gt:
    fail pipeline and alert
else:
    continue to Silver with valid records
```

---

# 10. Great Expectations usage

## 10.1. Expectation suite nên được sinh từ data contract

Agent code nên đọc file YAML contract và generate expectation suite tương ứng.

Mapping gợi ý:

| Contract rule | Great Expectations |
|---|---|
| `required: true` | `expect_column_values_to_not_be_null` |
| `unique: true` | `expect_column_values_to_be_unique` |
| `min/max` | `expect_column_values_to_be_between` |
| `allowed_values` | `expect_column_values_to_be_in_set` |
| `regex` | `expect_column_values_to_match_regex` |
| `type` | `expect_column_values_to_be_of_type` |
| `row_count_min` | `expect_table_row_count_to_be_between` |

## 10.2. Rule không nên ép vào Great Expectations

Một số rule nên xử lý bằng Spark riêng:

- Foreign key check với bảng lớn.
- Cross-source reconciliation.
- Entity matching.
- Outlier model-based.
- Dedup phức tạp.
- Window-based anomaly detection.

Great Expectations nên dùng cho validation rõ ràng, deterministic, dễ audit.

---

# 11. Silver layer design

Silver không chỉ là “dữ liệu sạch”, mà nên có thêm metadata để giải thích dữ liệu đã được xử lý thế nào.

Các cột kỹ thuật nên có:

```text
source_name
ingestion_time
run_id
record_hash
row_hash
dq_status
dq_issue_codes
is_quarantined
is_outlier
is_duplicate
is_imputed
canonical_entity_id
valid_from
valid_to
```

Ví dụ `dq_status`:

| Giá trị | Ý nghĩa |
|---|---|
| `passed` | Không có lỗi |
| `resolved` | Có lỗi nhưng đã xử lý |
| `flagged` | Có nghi ngờ nhưng vẫn cho đi tiếp |
| `quarantined` | Không đưa vào Silver chính |
| `manual_review` | Cần kiểm tra thủ công |

---

# 12. Quarantine Zone design

## 12.1. Quarantine không phải nơi xóa dữ liệu

Quarantine là vùng lưu trữ có kiểm soát để giữ record lỗi/nghi ngờ. Mục tiêu là:

- Không làm bẩn Silver/Gold.
- Cho phép debug source.
- Cho phép replay sau khi rule được sửa.
- Cho phép audit chất lượng dữ liệu.

## 12.2. Loại dữ liệu nên vào quarantine

| Category | Ví dụ |
|---|---|
| Invalid critical | Primary key null, FK không tồn tại |
| Missing critical | `event_time` null |
| Severe outlier | Giá trị vượt ngưỡng bất khả thi |
| Duplicate conflict | Không chọn được winner |
| Cross-source conflict | Hai source trusted mâu thuẫn lớn |
| Schema issue | Field critical bị thiếu hoặc type thay đổi không cast được |

## 12.3. Trạng thái record trong quarantine

```text
new
under_review
resolved
replayed
ignored
```

## 12.4. Replay từ quarantine

Khi rule được sửa hoặc source được backfill:

```text
quarantine.dq_issues
  -> read records with status = resolved
  -> revalidate
  -> write to bronze/silver
  -> update status = replayed
```

---

# 13. OpenMetadata integration

OpenMetadata nên được dùng để quản lý:

- Dataset owner.
- Schema và column description.
- Data contract.
- Data quality test result.
- Lineage từ Raw -> Bronze -> Silver -> Gold.
- Glossary cho business term.
- Tags như `PII`, `Critical`, `DQ:HighRisk`, `Domain:Transport`.
- SLA/Freshness metadata.

Metadata nên publish sau mỗi DAG run:

```text
dataset_name
run_id
row_count
failed_count
quarantined_count
dq_score
expectation_results
lineage_edges
schema_version
```

Gợi ý tag:

```text
DQ_INVALID
DQ_MISSING
DQ_OUTLIER
DQ_DUPLICATE
DQ_INCONSISTENCY
HAS_QUARANTINE
CRITICAL_DATASET
```

---

# 14. Alerting policy

Airflow nên alert khi:

| Điều kiện | Alert |
|---|---|
| Critical failure ratio vượt threshold | Fail DAG + alert |
| Missing ratio tăng bất thường | Warning |
| Duplicate ratio vượt threshold | Warning hoặc fail |
| Quarantine tăng đột biến | Alert |
| Source freshness trễ SLA | Alert |
| Cross-source conflict nghiêm trọng | Alert |
| DQ score giảm so với baseline | Alert |

DQ score gợi ý:

```text
dq_score = 1 - weighted_failure_ratio
```

Trong đó:

```text
weighted_failure_ratio =
  critical_ratio * 1.0 +
  high_ratio * 0.7 +
  medium_ratio * 0.3 +
  low_ratio * 0.1
```

---

# 15. Checklist triển khai cho code agent

## 15.1. Repository structure gợi ý

```text
contracts/
  data_quality/
    environment_air_quality.yaml
    transport_traffic_events.yaml

expectations/
  environment_air_quality/
    suite.json
  transport_traffic_events/
    suite.json

orchestration/
  dags/
    dq_ingestion_dag.py

src/
  dq/
    contract_loader.py
    gx_suite_generator.py
    bronze_validator.py
    quarantine_writer.py
    audit_writer.py
    threshold_evaluator.py
    outlier_detector.py
    duplicate_detector.py
    reconciliation.py
    metadata_publisher.py

dbt/
  models/
    silver/
      stg_environment_air_quality.sql
      int_environment_air_quality_dedup.sql
      silver_environment_air_quality.sql
    gold/
      mart_air_quality_daily.sql
```

## 15.2. Module cần implement

| Module | Trách nhiệm |
|---|---|
| `contract_loader.py` | Đọc YAML contract |
| `gx_suite_generator.py` | Sinh Great Expectations suite từ contract |
| `bronze_validator.py` | Chạy validation ở Bronze |
| `quarantine_writer.py` | Ghi record lỗi vào Iceberg quarantine table |
| `audit_writer.py` | Ghi summary vào audit table |
| `threshold_evaluator.py` | Quyết định fail/continue theo threshold |
| `outlier_detector.py` | IQR/Z-score/rolling/model-based detection |
| `duplicate_detector.py` | Exact/key/business duplicate detection |
| `reconciliation.py` | Resolve conflict giữa nhiều source |
| `metadata_publisher.py` | Publish lineage/DQ metrics sang OpenMetadata |

## 15.3. Done criteria

Một dataset được coi là tích hợp DQ đầy đủ khi:

- Có data contract YAML.
- Có generated expectation suite.
- Có Airflow DAG task validate Bronze.
- Có quarantine table ghi record fail.
- Có audit summary theo mỗi run.
- Có rule threshold để fail hoặc continue.
- Có Silver model xử lý missing, duplicate, outlier, inconsistency.
- Có OpenMetadata lineage và DQ metrics.
- Có test dữ liệu mẫu cho pass/fail/quarantine.
- Có tài liệu mô tả rule và owner.

---

# 16. Prompt ngắn cho code agent

Dùng prompt sau để yêu cầu agent code triển khai:

```text
Bạn là code agent cho dự án multi-source ingestion dùng Airflow, Iceberg, OpenMetadata, Great Expectations, Spark và dbt.

Hãy triển khai data quality framework theo README này.

Yêu cầu:
1. Tạo contract YAML cho từng dataset.
2. Sinh Great Expectations suite từ contract.
3. Implement Bronze validation bằng Great Expectations và Spark.
4. Tách bản ghi pass/fail.
5. Ghi bản ghi lỗi vào Iceberg table quarantine.dq_issues.
6. Ghi run summary vào audit.dq_run_summary.
7. Implement threshold evaluator để quyết định fail DAG hoặc tiếp tục.
8. Implement Silver xử lý:
   - invalid data
   - missing values
   - outlier
   - duplicate
   - cross-source inconsistency
9. Publish DQ metrics, schema version và lineage sang OpenMetadata.
10. Không sửa trực tiếp dữ liệu Raw/Bronze. Mọi resolve phải tạo output ở Silver và có audit trail.

Output mong muốn:
- Python modules trong src/dq/
- Airflow DAG trong orchestration/dags/
- YAML contract trong contracts/data_quality/
- Great Expectations suite trong expectations/
- SQL/dbt model trong dbt/models/silver/
- README hướng dẫn chạy local và chạy bằng Airflow
```

---

# 17. Tóm tắt quyết định kỹ thuật

| Painpoint | Detect | Resolve | Storage/Audit | Tool chính |
|---|---|---|---|---|
| Invalid | Bronze validation | Quarantine hoặc normalize ở Silver | `quarantine.dq_issues`, `audit.dq_run_summary` | GX, Spark, Iceberg |
| Outlier | Bronze rule + Silver statistics/model | Flag, cap, smooth, quarantine | Outlier flags + audit metrics | Spark, SparkML, GX |
| Missing | Null/missing-token/completeness check | Quarantine, impute, enrich, keep null | Missing flags + audit metrics | GX, Spark, dbt |
| Duplicate | Unique/hash/natural key/entity matching | Dedup, survivorship, canonical id | Duplicate group audit | Spark, dbt, GX |
| Inconsistency | Cross-source reconciliation | Source priority, latest wins, consensus, quarantine | Reconciliation table | Spark, dbt, OpenMetadata |

---

## 18. Kết luận

Với các painpoint Data Quality, pipeline không nên chỉ dừng ở việc validate rồi fail. Thiết kế tốt hơn là:

```text
Detect rõ ràng
  -> Phân loại severity
  -> Cô lập record lỗi
  -> Resolve ở Silver bằng rule có kiểm soát
  -> Ghi audit đầy đủ
  -> Publish metadata/quality metric
  -> Cho phép replay khi đã sửa rule hoặc source
```

Cách này giúp hệ thống multi-source ingestion vừa an toàn cho downstream, vừa đủ minh bạch để debug, audit và mở rộng khi thêm source mới.
