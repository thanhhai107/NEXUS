# Schema Drift & Data Quality Painpoints

> Document tổng hợp các vấn đề về schema drift và data quality cùng giải pháp tối ưu cho hệ thống NEXUS.

---

## Mục lục

1. [Schema Field Issues](#1-schema-field-issues)
2. [File Structure Issues](#2-file-structure-issues)
3. [Tổng kết Giai đoạn xử lý](#tổng-kết-giai-đoạn-xử-lý)
4. [SLM Integration](#slm-integration)

---

## 1. Schema Field Issues

### 1.1 Thiếu Field (Missing Fields)

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Mô tả** | Nguồn trả về dữ liệu bị thiếu field so với schema định nghĩa |
| **Giai đoạn** | Ingestion (phát hiện sớm) + Bronze (validate cuối) |
| **SLM cần thiết** | ❌ Không |

**Hành vi:**
- Required field bị thiếu → Fail contract / Quarantine record
- Optional field bị thiếu → Set null / default value, log warning

**Solution:**

```yaml
# Trong data contract/schema
fields:
  - name: record_id
    type: string
    required: true          # Bắt buộc
  - name: temperature
    type: float
    required: false         # Tùy chọn
    default: null
```

**Implementation:**
```
Ingestion Layer:
  ├── Schema registry định nghĩa required fields
  ├── Missing required → Quarantine/Fail
  └── Missing optional → Set null/default, log warning

Bronze Layer:
  └── MERGE với default values cho optional fields
```

---

### 1.2 Field Mới Không Biết (Unknown Fields)

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Mô tả** | Nguồn trả về field mới mà luồng không biết |
| **Giai đoạn** | Ingestion (phát hiện) + Silver (process) |
| **SLM cần thiết** | ⚠️ Optional - Gợi ý semantic type |

**Hành vi:**
- Strict mode: Chặn, reject record
- Permissive mode: Ghi nhận, cho qua nhưng log warning

**Solution:**

```yaml
# Config per source
sources:
  openaq:
    schema_policy:
      allow_unknown_fields: true  # hoặc false
      log_unknown_fields: true
```

**Implementation:**
```
Ingestion:
  ├── Strict mode: Reject record, alert
  ├── Permissive mode: 
  │   ├── Pass through với log
  │   └── Update schema registry với field mới
  └── SLM-assisted (optional):
      ├── Analyze field name → suggest semantic type
      └── Generate schema change request
```

---

### 1.3 Field Bị Bỏ (Dropped Fields)

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Mô tả** | Nguồn bỏ field mà luồng đang dùng |
| **Giai đoạn** | Bronze (validate schema compatibility) |
| **SLM cần thiết** | ❌ Không |

**Hành vi:**
- Required field bị bỏ → Fail contract, halt pipeline
- Optional field bị bỏ → Warning, allow với null

**Solution:**

```python
# Schema compatibility checker
def check_compatibility(old_schema, new_schema):
    old_required = {f.name for f in old_schema.fields if f.required}
    new_required = {f.name for f in new_schema.fields if f.required}
    
    # Breaking change: required field bị bỏ
    dropped_required = old_required - new_required
    if dropped_required:
        raise BreakingChangeError(f"Required fields dropped: {dropped_required}")
```

**Implementation:**
```
Bronze:
  ├── Schema versioning (semantic versioning)
  ├── Compatibility check: required_fields_old ⊆ required_fields_new
  ├── Required field dropped → FAIL contract, halt pipeline
  └── Optional field dropped → Warning, allow với null

Schema Registry:
  ├── Track schema versions per source
  └── Alert on breaking changes
```

---

### 1.4 Tên Field Thay Đổi (Field Renaming)

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Mô tả** | Tên field thay đổi (ví dụ: `temperature_f` → `temperature_c`) |
| **Giai đoạn** | Bronze/Silver (mapping) |
| **SLM cần thiết** | ⚠️ Optional - Detect và suggest alias |

**Hành vi:**
- Không tự đoán tên mới
- Dùng schema version + alias/mapping

**Solution:**

```yaml
# Schema alias table
sources:
  londonair:
    schema_aliases:
      v1:
        temperature_f: temperature_f
      v2:
        temperature_c: temperature_f  # field mới map sang field cũ
```

**Implementation:**
```
Bronze/Silver:
  ├── Schema alias table: {"old_name": "new_name", ...}
  ├── Version-based mapping
  └── SLM-assisted (optional): 
      ├── Detect semantic similarity
      ├── Suggest "potential rename of X to Y"
      └── Human approves → add to alias table
```

---

### 1.5 Kiểu Dữ Liệu Thay Đổi (Type Changes)

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Mô tả** | Kiểu dữ liệu của field thay đổi (string → int, float → string, etc.) |
| **Giai đoạn** | Ingestion (raw type check) + Bronze (safe cast) |
| **SLM cần thiết** | ❌ Không - Type coercion logic |

**Hành vi:**
- Validate type trước
- Chỉ cast khi an toàn
- Đổi type không tương thích → Reject/Quarantine

**Solution:**

```python
# Safe type coercion matrix
COERCION_RULES = {
    ("string", "number"): lambda x: float(x) if x.replace(".", "").replace("-", "").isdigit() else None,
    ("number", "string"): str,
    ("int", "float"): float,
    ("float", "int"): int,  # Truncate với warning
    ("string", "boolean"): lambda x: x.lower() in ("true", "1", "yes"),
    ("boolean", "string"): str,
}

# Incompatible types → Quarantine
INCOMPATIBLE_TYPES = [
    ("object", "string"),
    ("array", "number"),
    ("string", "object"),
]
```

**Implementation:**
```
Ingestion (Raw):
  └── Detect type mismatch, log với schema version

Bronze (Validated):
  ├── Safe coercion matrix (bảng trên)
  ├── Compatible types: Cast an toàn
  ├── Incompatible types: Quarantine record
  └── Log all type conversions
```

---

## 2. File Structure Issues

### 2.1 Nested Schema

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Mô tả** | Cấu trúc nested object thay đổi (thêm/bớt nested level) |
| **Giai đoạn** | Ingestion (parse) + Bronze (normalize) + Silver (flatten) |
| **SLM cần thiết** | ❌ Không |

**Hành vi:**
- Nested level thay đổi → Parse theo explicit schema
- Flatten nested object ở Silver layer

**Solution:**

```json
// Schema định nghĩa nested structure
{
  "name": "location",
  "type": "object",
  "properties": {
    "lat": {"type": "number"},
    "lon": {"type": "number"},
    "address": {
      "type": "object",
      "properties": {
        "street": {"type": "string"},
        "city": {"type": "string"}
      }
    }
  }
}
```

**Implementation:**
```
Ingestion:
  └── Parse JSON với explicit schema validation

Bronze:
  └── Preserve nested structure (raw storage)

Silver:
  └── Flatten nested objects:
      location.address.street → location_street
      location.address.city → location_city
```

---

### 2.2 CSV Header Mismatch

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Mô tả** | CSV file có header không match với schema |
| **Giai đoạn** | Ingestion (parse CSV) |
| **SLM cần thiết** | ❌ Không |

**Hành vi:**
- Header không match → Fail/Quarantine
- Extra columns → Configurable (ignore/allow)

**Solution:**

```python
def validate_csv_header(file_header: list[str], schema_columns: list[str]):
    schema_set = set(schema_columns)
    file_set = set(file_header)
    
    # Required columns missing
    missing = schema_set - file_set
    if missing:
        raise CSVHeaderError(f"Missing required columns: {missing}")
    
    # Extra columns
    extra = file_set - schema_set
    if extra and not config.ALLOW_EXTRA_COLUMNS:
        raise CSVHeaderError(f"Extra columns not in schema: {extra}")
    
    return True
```

**Implementation:**
```
Ingestion:
  ├── Validate header với schema
  ├── Missing required → Fail/Quarantine
  ├── Extra columns → Configurable (strict/permissive)
  └── Log all mismatches
```

---

### 2.3 Column Order Mismatch

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Mô tả** | Thứ tự columns trong CSV thay đổi |
| **Giai đoạn** | Ingestion (parse CSV) |
| **SLM cần thiết** | ❌ Không |

**Hành vi:**
- Nếu phụ thuộc thứ tự → Dùng ordered check
- Nếu không → Validate theo set columns

**Solution:**

```python
def validate_csv_structure(file_path: Path, schema):
    # Option 1: Validate by column name (recommended)
    df = pd.read_csv(file_path)
    schema_columns = {f.name for f in schema.fields}
    
    if set(df.columns) != schema_columns:
        missing = schema_columns - set(df.columns)
        extra = set(df.columns) - schema_columns
        raise CSVStructureError(f"Columns mismatch: missing={missing}, extra={extra}")
    
    # Option 2: Strict order validation (if required)
    if schema.strict_order:
        expected_order = [f.name for f in schema.fields if f.name in df.columns]
        actual_order = list(df.columns)
        if expected_order != actual_order:
            logger.warning(f"Column order mismatch: expected={expected_order}")
```

**Implementation:**
```
Ingestion:
  ├── Validate columns by SET (recommended)
  ├── If config requires order → Validate by ORDER
  └── Allow flexible column order by default
```

---

### 2.4 Array/Object Structure Change

| Thuộc tính | Chi tiết |
|-------------|----------|
| **Mô tả** | Cấu trúc array/object thay đổi (thêm/bớt elements, thay đổi keys) |
| **Giai đoạn** | Ingestion (parse) + Bronze (validate) |
| **SLM cần thiết** | ❌ Không |

**Hành vi:**
- Array length thay đổi → Version hoặc quarantine
- Object keys thay đổi → Validate shape, version

**Solution:**

```python
def validate_array_structure(value: list, schema):
    # Validate array items
    if schema.min_items and len(value) < schema.min_items:
        raise ArraySizeError(f"Array too small: {len(value)} < {schema.min_items}")
    
    if schema.max_items and len(value) > schema.max_items:
        raise ArraySizeError(f"Array too large: {len(value)} > {schema.max_items}")
    
    # Validate each item type
    for i, item in enumerate(value):
        validate_type(item, schema.items)
    
    return True

def validate_object_structure(value: dict, schema):
    # Required keys
    required_keys = {p.name for p in schema.properties if p.required}
    missing = required_keys - set(value.keys())
    if missing:
        raise ObjectKeysError(f"Missing required keys: {missing}")
    
    # Optional keys (allow extra by default)
    extra = set(value.keys()) - {p.name for p in schema.properties}
    if extra and not schema.allow_additional_properties:
        raise ObjectKeysError(f"Extra keys not in schema: {extra}")
    
    return True
```

**Implementation:**
```
Ingestion:
  ├── Parse array/object với explicit schema
  ├── Shape change → Log + version
  └── Configurable strictness per field

Bronze:
  ├── Version schema when shape changes
  └── Quarantine records với incompatible shapes
```

---

## Tổng kết Giai đoạn xử lý

| Painpoint | Ingestion | Bronze | Silver | SLM |
|-----------|:---------:|:------:|:------:|:---:|
| Missing field | ✅ | ✅ | - | ❌ |
| Unknown field | ✅ | - | ✅ | ⚠️ |
| Dropped field | - | ✅ | - | ❌ |
| Field rename | - | ✅ | ✅ | ⚠️ |
| Type change | ✅ | ✅ | - | ❌ |
| Nested schema | ✅ | ✅ | ✅ | ❌ |
| CSV header | ✅ | - | - | ❌ |
| Column order | ✅ | - | - | ❌ |
| Array/Object | ✅ | ✅ | - | ❌ |

**Legend:**
- ✅ = Xử lý chính
- - = Không cần/xử lý phụ
- ⚠️ = Optional với SLM

---

## SLM Integration

### Khi nào cần SLM?

| Painpoint | SLM Role | Mức độ |
|-----------|----------|---------|
| Unknown field | Gợi ý semantic type | Optional |
| Field rename | Detect similarity, suggest alias | Optional |
| Others | Không cần | ❌ |

### SLM Use Cases

```
1. Field Semantic Analysis
   Input: {"wthr_cd": 72}
   SLM: "wthr_cd likely means weather_code"
   Output: Suggest semantic type = "temperature"

2. Rename Detection
   Input: Old schema has "temperature_f", new has "temperature_c"
   SLM: "temperature_f and temperature_c are semantically similar"
   Output: Suggest add alias

3. Schema Change Description
   Input: Schema v2 has 3 new fields
   SLM: Generate natural language description
   Output: "Added fields: location_lat, location_lon, accuracy"
```

### SLM Recommendations

| Model | Size | Use Case | Deployment |
|-------|------|----------|------------|
| Llama 3.2 | 1B / 3B | Field analysis | Local (CPU) |
| Qwen2.5 | 1.5B / 3B | Semantic similarity | Local (CPU) |
| Phi-3.5 | 3.8B | Complex analysis | GPU |

**KHÔNG nên dùng SLM cho:**
- Type coercion (rule-based tốt hơn)
- Breaking change detection (versioning đủ)
- Required field validation (config đủ)

---

## Implementation Priority

### Phase 1: Core Rules (Khẩn cấp)
```
Priority 1:
├── Missing required field → Quarantine
├── CSV header mismatch → Fail
├── Type change validation → Reject incompatible
└── Schema versioning → Track changes

Priority 2:
├── Optional field missing → Default/null
├── Unknown field → Configurable (strict/permissive)
└── Column order → Set-based validation
```

### Phase 2: Enhancement (Cải thiện)
```
├── Nested schema flattening
├── Array/Object shape validation
├── Alias mapping
└── SLM integration (optional)
```

---

## References

- [JSON Schema Specification](https://json-schema.org/)
- [Great Expectations](https://greatexpectations.io/)
- [Data Contract Schema](../domains/)

---

*Document version: 1.0*
*Last updated: 2026-05-26*
