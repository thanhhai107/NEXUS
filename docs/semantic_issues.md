# Semantic Issues Checklist cho NEXUS

> **Scope note:** This document was authored for the environment/transport
> domain model. On the current tree the active domain is **TPC-DI** (`domains/tpc/`).
> The semantic governance patterns described here apply to TPC-DI tables with
> the active config at `domains/tpc/semantic_rules.yml`.

Tài liệu này áp dụng lớp semantic governance cho các nguồn trong NEXUS. Các cấu hình triển khai nằm ở:

- `config/semantic_defaults.yml`: chuẩn mặc định cho OpenMetadata, glossary, unit, time, CRS, grain và entity resolution.
- `domains/<domain>/semantic_rules.yml`: semantic contract theo từng domain.
- `transform/dbt/seeds/unit_mapping.csv`: bảng mapping đơn vị đo dùng bởi dbt seed và Great Expectations.
- `transform/dbt/models/gold/schema.yml`: khai báo semantic grain, time standard và unit standardization cho Gold models.
- `python -m cli.nexus semantic export`: sinh output OpenMetadata hoặc Business Glossary từ semantic contracts.
- `python -m cli.nexus semantic match-entities`: tạo `canonical_entity_id` và entity crosswalk từ semantic rules.

## Checklist

| Semantic issue | Mô tả vấn đề | Ví dụ trong NEXUS | Rủi ro nếu không xử lý | Cách chuẩn hóa / giải pháp đề xuất | Công cụ phù hợp | Mức độ xử lý | Output cần tạo ra |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Cùng tên field nhưng ý nghĩa khác nhau | Một field có cùng tên nhưng business meaning thay đổi theo nguồn hoặc ngữ cảnh. | `state` trong `transport_events` là administrative area của incident; không được hiểu là lifecycle state như open/resolved. | Mapping sai vào dimension, filter sai, metric bị diễn giải sai. | Bắt field-level metadata bằng OpenMetadata; gắn `business_term`, `business_definition`, `source_system`; map field vào Business Glossary thay vì chỉ dựa tên cột. | OpenMetadata, Business Glossary, dbt `meta` | Full | Field context mapping; glossary term mapping; source field -> canonical concept |
| Khác tên field nhưng cùng ý nghĩa | Nhiều nguồn dùng tên khác nhau cho cùng một khái niệm. | `location_id`, `station_uid`, `atcocode` đều là mã entity địa điểm/điểm quan trắc/điểm dừng trong ngữ cảnh riêng. | Tạo duplicate dimensions, join thiếu, không gom entity được. | Khai báo `field_mappings` trong semantic rules; tạo canonical field như `monitoring_site_id`, `transit_stop_id`; lưu alias trong Business Glossary. | OpenMetadata, Business Glossary, dbt, semantic contract CLI | Full | Alias mapping; canonical data dictionary; semantic contract per dataset |
| Khác đơn vị đo | Cùng đại lượng nhưng đơn vị khác nhau hoặc đơn vị nguồn không rõ. | OpenAQ dùng `µg/m³` hoặc `ug/m3`; US Accidents dùng `distance_mi`; speed có thể là `km/h`, `m/s`, `mph`. | Metric sai nghiêm trọng, cảnh báo ngưỡng ô nhiễm/speed sai, model ML học sai scale. | Dùng `transform/dbt/seeds/unit_mapping.csv`; convert sang canonical unit trong dbt Gold; kiểm tra conversion bằng Great Expectations với derived canonical value. | dbt seed, Great Expectations, OpenMetadata | Full cho đơn vị có công thức tuyến tính; Partial cho `ppm/ppb` vì cần pollutant-specific context | Unit Mapping Table; canonical value field; GX validation result |
| Khác timestamp, timezone, time granularity | Timestamp khác format, timezone, hoặc grain theo giây/phút/giờ/ngày. | `event_time` trong streaming event; `datetime` trong OpenAQ; `period_beginning` trong London journeys là reporting period. | Sai thứ tự sự kiện, sai rollup theo giờ/ngày, join sai với dữ liệu aggregate. | Lưu UTC, ISO 8601; timezone dùng IANA; phân biệt `event_time`, `_nexus_ingested_at`, `_nexus_silver_loaded_at`; khai báo time grain trong semantic model. | dbt, OpenMetadata, Airflow/Dagster, Great Expectations | Full | Time role mapping; timezone policy; semantic time grain |
| Khác hệ tọa độ / hệ quy chiếu không gian | Dataset không gian có CRS khác nhau hoặc thiếu CRS. | `latitude`/`longitude` của OpenAQ, NaPTAN, transport events dùng EPSG:4326; bản đồ render cần EPSG:3857. | Điểm bị lệch, spatial join sai, tính khoảng cách/diện tích sai. | Chuẩn lưu trữ EPSG:4326; render EPSG:3857; xử lý bằng GeoPandas/PostGIS; bắt buộc metadata `source_crs`, `storage_crs`, `render_crs`. | GeoPandas, PostGIS, OpenMetadata | Full về policy; Partial cho runtime transform nếu nguồn mới chưa khai CRS | CRS policy; geometry metadata; spatial validation |
| Grain mismatch | Dataset có mức chi tiết khác nhau nhưng bị join/so sánh như cùng grain. | `transport_events` là raw event; `openaq_air_quality_hourly` là hourly aggregate; `london_journeys` là reporting-period aggregate. | Double counting, many-to-many join không kiểm soát, KPI sai. | Khai báo `entity_grain`, `time_grain`, `spatial_grain`, `aggregation_level`; chỉ join khi grain tương thích hoặc đã aggregate/allocate rõ ràng. | dbt `meta`, semantic layer, OpenMetadata | Full | Grain declaration; join compatibility rule; metric grain |
| Khác cách định nghĩa dữ liệu | Cùng thuật ngữ nhưng định nghĩa nghiệp vụ khác nhau. | `AQI` từ WAQI là vendor-provided AQI; OpenMeteo có `european_aqi` và `us_aqi`; không được mix thành một metric nếu thiếu standard. | Dashboard tranh cãi, báo cáo regulatory sai, không audit được KPI. | Đưa definition vào Business Glossary và semantic metric definition; metric phải có formula, filter, grain, unit, owner, effective date. | Business Glossary, OpenMetadata, dbt semantic metadata | Full | Metric dictionary; glossary definition; versioned business rule |
| Xác định hai record có cùng entity hay không | Hai record từ nhiều nguồn mô tả cùng entity nhưng khóa khác nhau hoặc không ổn định. | Cùng một monitoring site có `location_id` ở OpenAQ và `station_uid` ở WAQI; cùng stop có `atcocode` và tên/coordinate tương đương. | Duplicate entity, join thiếu/sai, lịch sử entity bị tách. | Pipeline entity matching: exact, rule-based, fuzzy, probabilistic, Splink; LLM chỉ hỗ trợ mapping phức tạp và cần review; sau matching tạo `canonical_entity_id`. | Splink, Python, PostGIS, LLM-assisted review, OpenMetadata | Partial hiện tại; Full khi có crosswalk production | `canonical_entity_id`; entity crosswalk; confidence score; manual review queue |

## Quy trình vận hành

1. Khi thêm dataset mới, cập nhật `domains/<domain>/semantic_rules.yml` cùng lúc với `datasets.yml`, `quality_rules.yml` và JSON Schema.
2. Mỗi field quan trọng phải có glossary term hoặc canonical field mapping.
3. Mọi measurement field phải khai báo `dimension_type`, `value_field`, `unit_field` hoặc `implicit_source_unit`.
4. Nếu unit convert được bằng scale/offset, thêm vào `transform/dbt/seeds/unit_mapping.csv` với `conversion_supported=true`.
5. Nếu unit cần context như `ppm/ppb`, để `conversion_supported=false` và tạo business rule riêng theo pollutant.
6. Timestamp phải có `event_time_field`, timezone IANA, UTC storage policy và ISO 8601 format.
7. Spatial dataset phải khai CRS nguồn, CRS lưu trữ và CRS render.
8. Gold model phải khai `semantic_grain` trong dbt `schema.yml`.
9. Entity matching phải sinh `canonical_entity_id` và lưu crosswalk trước khi dùng cho dimension hợp nhất.

## Kiểm tra nhanh

```powershell
$env:NEXUS_RUNTIME_DIR = "runtime"
python -m cli.nexus semantic show --dataset tpcdi_dim_customer
python -m cli.nexus semantic export --kind openmetadata --domain tpc
python -m cli.nexus semantic export --kind glossary --domain tpc
python -m cli.nexus contract show --dataset tpcdi_dim_customer
```

Các lệnh trên sử dụng dataset TPC-DI đang active (`domains/tpc/`).
