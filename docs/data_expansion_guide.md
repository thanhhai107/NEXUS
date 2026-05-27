# Hướng dẫn mở rộng Data cho NEXUS

## Mục lục
1. [Tổng quan chiến lược](#1-tổng-quan-chiến-lược)
2. [Domain Environment: Các nguồn dữ liệu](#2-domain-environment-các-nguồn-dữ-liệu)
3. [Domain Transport: Các nguồn dữ liệu](#3-domain-transport-các-nguồn-dữ-liệu)
4. [Phạm vi tải dữ liệu đề xuất](#4-phạm-vi-tải-dữ-liệu-đề-xuất)
5. [Config mẫu cho từng nguồn](#5-config-mẫu-cho-từng-nguồn)
6. [Volume ước tính](#6-volume-ước-tính)
7. [Thứ tự triển khai đề xuất](#7-thứ-tự-triển-khai-đề-xuất)
8. [Nguồn tích hợp thêm cho giai đoạn mở rộng](#8-nguồn-tích-hợp-thêm-cho-giai-đoạn-mở-rộng)

---

## 1. Tổng quan chiến lược

### Triết lý mở rộng
NEXUS là **Intelligent Data Platform** cho London - cần thể hiện 3 đặc tính:

| Đặc tính | Ý nghĩa | Cách đạt |
|----------|---------|----------|
| **Volume** | Xử lý được dữ liệu lớn | Nhiều rows, nhiều sources |
| **Velocity** | Batch + Streaming | Realtime events, time-series |
| **Variety** | Đa dạng data types | Sensor, vector, API, CSV, events |

### Tình trạng hiện tại
| Metric | Giá trị | Gap |
|--------|---------|-----|
| Total rows | ~2 triệu | Cần 10-50 triệu |
| Sources | 8 nguồn | Cần 15-20 nguồn |
| Velocity | Batch only | Cần streaming |
| Domains | Environment tốt, Transport yếu | Cần cân bằng |

### Phân bổ theo Domain & Layer

```
                    │  Environment    │  Transport     │
────────────────────┼─────────────────┼───────────────┤
Bronze (Raw)        │  Sensor data     │  GTFS, TfL API │
                    │  Historical CSV  │  CSV, API      │
────────────────────┼─────────────────┼───────────────┤
Silver (Canonical)  │  Air quality     │  Journeys      │
                    │  Weather         │  Traffic        │
────────────────────┼─────────────────┼───────────────┤
Gold (Aggregated)   │  Borough AQ     │  Mode share    │
                    │  Daily trends    │  Congestion    │
```

---

## 2. Domain Environment: Các nguồn dữ liệu

### 2.1 LondonAir Monitoring Network ⭐ CORE

**Giới thiệu:** Mạng lưới đo chất lượng không khí của London - nguồn tốt nhất hiện có.

**Tại sao nên mở rộng:**
- Đã có ~741K rows - foundation tốt
- 100+ monitoring stations across London
- Đo nhiều pollutants: NO2, PM10, PM2.5, O3, SO2, CO
- Data quality cao từERG (Eccles Respiratory Group)
- API miễn phí, no rate limit

**Đóng góp cho platform:**
- Time-series analysis
- Geospatial correlation (station → borough)
- Air quality index computation
- Health advisory integration

**Layer:** Bronze → Silver (đã tốt, cần mở rộng thêm)

**Scope đề xuất:**
```yaml
londonair:
  # Sites: Tất cả sites thay vì limit
  site_limit: null                    # ~100+ sites
  
  # Species: Đo tất cả pollutants
  species:
    - NO                              # Nitrogen monoxide
    - NO2                             # Nitrogen dioxide  
    - NOX                             # Nitrogen oxides
    - PM10                            # Coarse particulate
    - PM25                            # Fine particulate
    - O3                              # Ozone
    - SO2                             # Sulphur dioxide
    - CO                              # Carbon monoxide
    - C6H6                            # Benzene
  
  # Time range: 5 năm
  years:
    - 2020                            # COVID impact year
    - 2021                            # Recovery
    - 2022                            # Post-COVID
    - 2023                            # Current
    - 2024                            # Latest
    - 2025                            # Up to present
  
  # Granularity: Hourly
  data_period: hourly
  
  # Group code: Lấy tất cả group codes
  group_code_limit: null
```

**Volume ước tính:**
```
100 sites × 8,760 hours/năm × 5 năm × 5 species ≈ 21.9 triệu rows
```

**Tài nguyên ước tính:** ~150-200 MB silver data

---

### 2.2 Open-Meteo Historical Weather ⭐ HIGH

**Giới thiệu:** API weather history miễn phí, không giới hạn, global coverage.

**Tại sao nên thêm:**
- Dữ liệu từ 1940, hourly resolution
- 33 boroughs × 5 năm × 20 variables = dataset lớn
- Miễn phí, no API key required
- Metadata-rich: temperature, humidity, wind, precipitation, pressure

**Đóng góp cho platform:**
- Time-series JOIN với air quality
- Weather impact analysis
- Seasonal patterns
- Climate trend detection

**Layer:** Bronze → Silver

**Scope đề xuất:**
```yaml
openmeteo_historical:
  # 33 London borough centroids
  location_limit: null                # Lấy tất cả
  
  # Time range
  start_date: '2020-01-01'
  end_date: '2025-05-31'              # Latest available
  
  # Variables: Weather
  weather_variables:
    - temperature_2m                  # Temperature at 2m
    - relative_humidity_2m            # Humidity %
    - precipitation_mm                 # Precipitation
    - rain_mm                         # Rainfall
    - snowfall_mm                     # Snowfall
    - cloud_cover_%                   # Cloud cover
    - pressure_msl                    # Sea level pressure
    - wind_speed_10m_ms               # Wind speed
    - wind_direction_10m°              # Wind direction
    - wind_gusts_10m_ms               # Gusts
  
  # Variables: Air Quality
  air_quality_variables:
    - pm2_5                           # Fine particulate
    - pm10                            # Coarse particulate
    - nitrogen_dioxide                 # NO2
    - ozone                           # O3
    - sulphur_dioxide                  # SO2
    - carbon_monoxide                  # CO
    - ammonia                         # NH3
    - pollen_grass                    # Grass pollen
    - pollen_birch                    # Birch pollen
    - pollen_olive                    # Olive pollen
    - uv_index                        # UV index
    - aerosol_height                   # Aerosol layer height
  
  # Granularity
  hourly: true
```

**Volume ước tính:**
```
33 boroughs × 8,760 hours/năm × 5 năm × 20 variables ≈ 28.8 triệu rows
```

**Tài nguyên ước tính:** ~200-250 MB silver data

---

### 2.3 OpenAQ Global Air Quality ⭐ MEDIUM

**Giới thiệu:** Global aggregated air quality data từ nhiều sources.

**Tại sao nên thêm:**
- Bổ sung global context
- Multiple sensor types và methodologies
- Real-time và historical
- London bounding box: 51.3°N, -0.5°W to 51.7°N, 0.3°E

**Đóng góp cho platform:**
- Comparative analysis (London vs other cities)
- Sensor network diversity
- Data quality validation

**Layer:** Bronze → Silver

**Scope đề xuất:**
```yaml
openaq:
  # Geographic bounding box: Greater London
  use_bbox: true
  bbox:
    lat_min: 51.28                    # South
    lat_max: 51.69                    # North
    lon_min: -0.51                    # West  
    lon_max: 0.34                     # East
  
  # Parameters
  parameters:
    - pm25
    - pm10
    - no2
    - o3
    - so2
    - co
    - bc
  
  # Time range
  date_from: '2020-01-01'
  date_to: '2025-05-31'
  
  # Limit
  limit_per_page: 10000
  max_pages: 100                      # ~1 triệu rows
  
  # Granularity
  granularity: hourly                 # or 'raw' cho sensor readings
```

**Volume ước tính:**
```
~500K - 1 triệu rows (bounded by API limits và London area)
```

---

### 2.4 UK-AIR Air Quality Archive ⭐ LOW

**Giới thiệu:** UK government air quality data archive.

**Tại sao nên thêm:**
- Official government source
- Long-term historical data (2007+)
- Manual và automatic monitoring stations

**Đóng góp cho platform:**
- Data validation against official source
- Long-term trend analysis
- Policy compliance tracking

**Layer:** Bronze only (raw CSV ingest)

**Scope đề xuất:**
```yaml
ukair:
  # Chỉ lấy London stations
  region: london
  network_type:
    - automatic
    - manual
  
  # Time range
  year_from: 2020
  year_to: 2025
  
  # Sites limit
  site_limit: 30                     # Top 30 London sites
```

---

## 3. Domain Transport: Các nguồn dữ liệu

### 3.1 TfL Live Arrivals & Departures ⭐⭐⭐ CRITICAL

**Giới thiệu:** Real-time arrival predictions cho London transport.

**Tại sao nên thêm (CRITICAL):**
- **Streaming demo** - đây là nguồn streaming thực sự
- High-frequency data (every 30-60 seconds)
- Multiple transport modes: Bus, Tube, DLR, Overground, Elizabeth line
- Volume cao: 10,000+ buses × 4 stops × 60 snaps/day = 2.4M events/day

**Đóng góp cho platform:**
- **Kafka streaming showcase**
- Real-time analytics
- Delay prediction models
- Service reliability metrics
- Window functions (5-min, 15-min, 1-hour aggregates)

**Layer:** Bronze (raw events) → Silver (enriched) → Gold (aggregates)

**Ingestion mode:** **Streaming** (Kafka producer every 60s)

**Scope đề xuất:**
```yaml
tfl_arrivals:
  # NaPTAN Stop Types
  stop_types:
    - NaptanMetro                   # Tube stations
    - NaptanMetroETAS               # Tube with live
    - NaptanMetroGTIS               # Tube GTIS
    - NaptanBusCoach                # Bus stops
    - NaptainDLR                    # DLR stations
    - NaptanOverground              # Overground
    
  # Stop Count
  stop_limit: 500                    # Top 500 busiest stops
  
  # Snapshot frequency
  snapshot_interval_sec: 60          # Every 60 seconds
  daily_snapshots: 1440             # 24 hours
  
  # Modes
  modes:
    - tube
    - bus
    - dlr
    - overground
    - elizabeth-line
    - tram
    - river-bus
  
  # Expected Volume
  # 500 stops × 5 modes × 5 vehicles × 1440 snaps/day = ~18 triệu events/day
  # (quá lớn cho demo, recommend: 100 stops × 720 snaps = ~360K events/day)
  
  # Thực tế cho demo
  realistic_demo:
    stop_limit: 100                  # Top 100 busiest
    modes: [tube, bus, dlr]          # 3 main modes
    snapshot_interval_sec: 60
    runtime_hours_per_run: 8        # 8 tiếng = 480 snaps
    # Volume: 100 stops × 3 modes × 480 snaps × 5 vehicles = ~720K events/run
```

**Kafka Topic Design:**
```yaml
topics:
  - name: tfl-arrivals
    partitions: 6
    retention_hours: 168            # 7 days
    config:
      cleanup_policy: delete
      segment_bytes: 1073741824    # 1GB
      
  - name: tfl-line-status
    partitions: 3
    retention_hours: 720           # 30 days
    
  - name: tfl-disruptions
    partitions: 3
    retention_hours: 720
```

---

### 3.2 TfL Line Status & Disruptions ⭐⭐ HIGH

**Giới thiệu:** Real-time line status và service disruptions.

**Tại sao nên thêm:**
- Event-based (không phải polling)
- Smaller volume nhưng high value
- Disruption patterns analysis
- Service reliability metrics

**Đóng góp cho platform:**
- Complex event processing
- Alert generation
- Service level agreements
- Historical disruption analysis

**Layer:** Bronze → Silver (event enrichment)

**Ingestion mode:** **Streaming** (poll every 2-5 minutes)

**Scope đề xuất:**
```yaml
tfl_line_status:
  # Lines to monitor
  lines:
    - bakerloo
    - central
    - circle
    - district
    - hammersmith-city
    - jubilee
    - metropolitan
    - northern
    - piccadilly
    - victoria
    - waterloo-city
    - dlr
    - overground
    - elizabeth-line
    
  # Poll frequency
  poll_interval_sec: 300              # Every 5 minutes
  
  # Severity filter
  severity_include:
    - closed
    - severe-delays
    - reduced-service
    - planned-works
    - minor-delays
    
  # Expected Volume
  # 14 lines × 12 polls/day × 365 days ≈ 61K events/year
```

---

### 3.3 Transport for London (TfL) Journey Planning ⭐ MEDIUM

**Giới thiệu:** Historical journey data from TfL open data.

**Tại sao nên thêm:**
- Already in system (london_journeys)
- Modal split analysis
- Time-of-day patterns
- Transport demand forecasting

**Đóng góp cho platform:**
- Journey aggregation analytics
- Mode share calculations
- Peak/off-peak patterns
- Long-term trend analysis

**Layer:** Bronze → Silver

**Ingestion mode:** **Batch** (monthly/quarterly updates)

**Scope đề xuất:**
```yaml
london_journeys:
  # Time range: 2015-2025
  years:
    - 2015
    - 2016
    - 2017
    - 2018
    - 2019
    - 2020                            # COVID impact
    - 2021
    - 2022
    - 2023
    - 2024
    - 2025
    
  # Financial years
  periods:
    - full_year
    
  # Modes
  modes:
    - bus
    - underground
    - dlr
    - tram
    - overground
    - tfl-rail
    - river
    - cable-car
    
  # Already good: ~209 rows/year × 10 years = ~2K rows
  # Không cần scale lớn, đã đủ cho demo
```

---

### 3.4 DfT Road Traffic Counts ⭐⭐ HIGH

**Giới thiệu:** Manual và automatic traffic counts across UK.

**Tại sao nên thêm:**
- Already in system (dft_road_traffic)
- ~50,000 count points across UK
- Traffic flow, speed, HGV percentage
- Coverage cả London và GB-wide

**Đóng gúp cho platform:**
- Traffic volume analysis
- Road safety correlation với STATS19
- Air quality impact assessment (traffic → emissions)
- Infrastructure planning

**Layer:** Bronze → Silver

**Ingestion mode:** **Batch** (annual releases)

**Scope đề xuất:**
```yaml
dft_road_traffic:
  # Scope: GB-wide raw, London silver
  scope: GB_raw_then_london_silver
  
  # Data types
  data_types:
    - count_point                    # ~50k points
    - automatic_count                # ~8k points
    - manual_count                  # ~35k points
    
  # Time range
  years:
    - 2015
    - 2016
    - 2017
    - 2018
    - 2019
    - 2020                            # COVID impact
    - 2021
    - 2022
    - 2023
    - 2024
    
  # Filter for London
  regions:
    - inner_london
    - outer_london
    
  # Variables
  variables:
    - total_vehicles
    - cars_and_taxis
    - buses
    - lgvs                           # Light goods vehicles
    - hgvs_2_rigid                   # Heavy goods vehicles
    - hgvs_3_rigid
    - hgvs_4_rigid
    - hgvs_articulated
    - pedal_cycles
    - motorcycles
    
  # Expected Volume
  # 50k count points × 10 years × 12 months ≈ 6 triệu rows
```

---

### 3.5 STATS19 Road Safety ⭐⭐ HIGH

**Giới thiệu:** UK road accidents, casualties, và vehicles database.

**Tại sao nên thêm:**
- Already in system (stats19_collisions)
- Rich structured data: 70+ columns
- Geospatial (lat/lon) + temporal
- Multi-table: accidents, vehicles, casualties

**Đóng góp cho platform:**
- Complex JOIN analysis
- Geospatial analytics
- Machine learning features
- Safety hotspot identification

**Layer:** Bronze → Silver

**Ingestion mode:** **Batch** (annual releases)

**Scope đề xuất:**
```yaml
stats19:
  # Scope: GB-wide raw, London silver
  scope: GB_raw_then_london_silver
  
  # Tables
  tables:
    - accidents                      # Core accident data
    - vehicles                      # Vehicle involvement
    - casualties                    # Casualty details
    
  # Time range: 2015-2024 (10 năm)
  years:
    - 2015
    - 2016
    - 2017
    - 2018
    - 2019
    - 2020                          # COVID - lower volume
    - 2021
    - 2022
    - 2023
    - 2024
    
  # Volume estimation
  # GB: ~130k accidents/year × 10 = 1.3M accidents
  #     ~230k vehicles/year × 10 = 2.3M vehicles
  #     ~180k casualties/year × 10 = 1.8M casualties
  
  # London subset: ~25k accidents/year × 10 = 250K accidents
```

---

### 3.6 London Borough Data (Nomis/Census) ⭐ MEDIUM

**Giới thiệu:** UK Census và official statistics.

**Tại sao nên thêm:**
- Denominator data cho normalized metrics
- Demographics, households, employment
- Geographic boundaries (OA → LSOA → MSOA → Borough)
- Enrichment layer cho analytics

**Đóng góp cho platform:**
- Population-adjusted metrics (per capita emissions)
- Deprivation correlation
- Demographic segmentation
- Geospatial joins

**Layer:** Bronze → Silver (lookup table)

**Ingestion mode:** **Batch** (5-year Census + annual updates)

**Scope đề xuất:**
```yaml
nomis_census:
  # Census 2021 - Latest full Census
  source: census_2021
  geography_level: msoa               # Middle Super Output Area
  
  # Topics
  topics:
    - population                      # Age, sex, ethnicity
    - households                     # Household composition
    - housing                        # Tenure, type, rooms
    - employment                     # Industry, occupation
    - transport                     # Mode to work, cars
    - health                        # Long-term health
    - education                     # Qualifications
    
  # Output Areas for detailed analysis
  oa_download: false                  # Too granular for demo
  
  # London only
  region_filter:
    - E09000001                      # City of London
    - E09000007                      # Camden
    - E09000012                      # Hackney
    - E09000014                      # Haringey
    - E09000016                      # Havering
    - E09000020                      # Kensington and Chelsea
    - E09000022                      # Lambeth
    - E09000028                      # Southwark
    - E09000030                      # Tower Hamlets
    - E09000033                      # Westminster
    # ... (tất cả 33 boroughs)
    
  # Volume: ~1,500 MSOAs × 50 variables ≈ 75K rows
```

---

### 3.7 London Datastore (GLA) ⭐ LOW

**Giới thiệu:** Greater London Authority open data portal.

**Tại sao nên thêm:**
- London-specific datasets
- Transport, environment, social metrics
- Annual releases, good for time-series

**Đóng gúp cho platform:**
- City-specific context
- Policy-aligned metrics
- Cross-domain datasets

**Layer:** Bronze → Silver

**Scope đề xuất:**
```yaml
london_datastore:
  # Key datasets
  datasets:
    - london-borough-profiles         # Borough statistics
    - london-borough-atlas            # Comparative data
    - london-borough-annual-report    # Annual data
    - daily-air-quality-index         # AQI by borough
    - london-emissions               # NOx, CO2 by borough
    - transport-daily-figures        # Daily journey stats
    
  # Update frequency
  frequency: annual                   # Most datasets
    
  # Volume: ~50 datasets × 33 boroughs × 10 years ≈ 16K rows
```

---

### 3.8 OS Open Roads / OpenMapLocal ⭐ LOW

**Giới thiệu:** Ordnance Survey open road network data.

**Tại sao nên thêm:**
- Road network vector data
- Map matching cho STATS19
- Routing capabilities
- Road classification

**Đóng góp cho platform:**
- Geospatial enrichment
- Distance calculations
- Network analysis
- Speed limit context

**Layer:** Bronze only (shapefile/GeoJSON)

**Scope đề xuất:**
```yaml
os_open_roads:
  # Coverage: London area only
  extent: london
  
  # Format
  format: GeoJSON
  
  # Attributes
  include_attributes:
    - road_classification
    - road_name
    - speed_limit
    - road_type
    - length_metres
    
  # Volume: ~500K road segments × 10 attributes ≈ 5M rows (in simplified format)
```

---

## 4. Phạm vi tải dữ liệu đề xuất

### 4.1 Time Range Guidelines

| Nguồn | Recommended Range | Rationale |
|--------|------------------|-----------|
| LondonAir | 2020-2025 | COVID impact analysis |
| Open-Meteo | 2020-2025 | Match LondonAir |
| OpenAQ | 2020-2025 | Consistent window |
| TfL Arrivals | Last 7 days (streaming) | Real-time |
| TfL Line Status | Last 30 days (streaming) | Disruption analysis |
| TfL Journeys | 2015-2025 | Long-term trends |
| DfT Traffic | 2015-2024 | 10-year window |
| STATS19 | 2015-2024 | 10-year window |
| Census | 2021 | Static reference |

### 4.2 Geographic Scope Guidelines

| Level | Coverage | Use Case |
|-------|----------|----------|
| Inner London | 14 boroughs | Core analytics |
| Outer London | 19 boroughs | Extended analysis |
| Greater London | 33 boroughs | Full platform demo |
| Greater London + M25 | 35+ areas | Commuter zone |
| GB-wide | 400+ local auth | National context |

**Recommendation:** Raw data = GB-wide, Silver/Gold = London-focused

### 4.3 Frequency & Update Patterns

| Nguồn | Mode | Frequency | Rationale |
|--------|------|-----------|-----------|
| LondonAir | Batch | Daily/hourly | Near real-time |
| Open-Meteo | Batch | Monthly backfill | Historical only |
| OpenAQ | Batch | Weekly | API rate limit |
| TfL Arrivals | **Streaming** | 60 sec | Real-time demo |
| TfL Line Status | **Streaming** | 300 sec | Event-based |
| TfL Journeys | Batch | Quarterly | Official release |
| DfT Traffic | Batch | Annual | Official release |
| STATS19 | Batch | Annual | Official release |

### 4.4 Volume Guidelines by Demo Level

| Level | Target Rows | Sources | Strategy |
|-------|-------------|---------|----------|
| **Minimal** | 5-10 triệu | 10 | LondonAir + OpenMeteo + TfL streaming |
| **Good** | 20-50 triệu | 15 | + DfT Traffic + STATS19 + Census |
| **Impressive** | 100+ triệu | 20+ | + GB-wide STATS19 + OS Roads |

---

## 5. Config mẫu cho từng nguồn

### 5.1 Environment Domain Config

```yaml
# ============================================
# ENVIRONMENT DOMAIN - Bronze Layer Config
# ============================================

sources:
  # LondonAir - CORE
  londonair_monitoring:
    api_base: "https://api.erg.ic.ac.uk/AirQuality"
    site_limit: null                    # ~100 sites
    species: [NO, NO2, NOX, PM10, PM25, O3, SO2, CO, C6H6]
    group_code_limit: null
    years: [2020, 2021, 2022, 2023, 2024, 2025]
    download_batch_size: 10000
    output_format: jsonl
    layer: bronze
    domain: environment

  # Open-Meteo - HIGH
  openmeteo_weather:
    api_base: "https://archive-api.open-meteo.com/v1/archive"
    location_limit: null                # 33 boroughs
    start_date: '2020-01-01'
    end_date: '2025-05-31'
    variables:
      weather:
        - temperature_2m
        - relative_humidity_2m
        - precipitation
        - cloud_cover
        - wind_speed_10m
        - wind_direction_10m
      air_quality:
        - pm2_5
        - pm10
        - nitrogen_dioxide
        - ozone
    hourly: true
    layer: bronze
    domain: environment

  # OpenAQ - MEDIUM
  openaq_measurements:
    api_base: "https://api.openaq.org/v3"
    use_bbox: true
    bbox:
      lat_min: 51.28
      lat_max: 51.69
      lon_min: -0.51
      lon_max: 0.34
    parameters: [pm25, pm10, no2, o3, so2, co]
    date_from: '2020-01-01'
    date_to: '2025-05-31'
    limit_per_page: 10000
    max_pages: 100
    layer: bronze
    domain: environment
```

### 5.2 Transport Domain Config

```yaml
# ============================================
# TRANSPORT DOMAIN - Bronze Layer Config
# ============================================

sources:
  # TfL Arrivals - CRITICAL (Streaming)
  tfl_arrivals:
    api_base: "https://api.tfl.gov.uk/StopPoint"
    stop_limit: 100                     # Top 100 for demo
    modes: [tube, bus, dlr]
    snapshot_interval_sec: 60
    output_format: jsonl
    layer: bronze
    domain: transport
    velocity: streaming
    kafka_topic: tfl-arrivals
    
  # TfL Line Status - HIGH (Streaming)
  tfl_line_status:
    api_base: "https://api.tfl.gov.uk/Line"
    lines: [bakerloo, central, circle, district, 
            hammersmith-city, jubilee, metropolitan, 
            northern, piccadilly, victoria, 
            waterloo-city, dlr, overground, elizabeth-line]
    poll_interval_sec: 300
    layer: bronze
    domain: transport
    velocity: streaming
    kafka_topic: tfl-line-status

  # TfL Journeys - MEDIUM (Batch)
  london_journeys:
    source_url: "https://data.london.gov.uk/dataset/tfl-daily-metrics"
    years: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    layers: [bronze, silver]
    domain: transport
    velocity: batch

  # DfT Road Traffic - HIGH (Batch)
  dft_road_traffic:
    api_base: "https://api.dft.gov.uk/v1"
    scope: GB_raw_then_london_silver
    years: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    data_types: [count_point, automatic_count]
    layer: bronze
    domain: transport
    velocity: batch

  # STATS19 - HIGH (Batch)
  stats19_road_safety:
    base_url: "https://data.dft.gov.uk/road-safety"
    scope: GB_raw_then_london_silver
    years: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    tables: [accidents, vehicles, casualties]
    layer: bronze
    domain: transport
    velocity: batch
```

### 5.3 Enrichment Sources Config

```yaml
# ============================================
# ENRICHMENT SOURCES - Reference Data
# ============================================

sources:
  # Census - MEDIUM
  nomis_census_2021:
    base_url: "https://www.nomisweb.co.uk"
    source: census_2021
    geography: msoa
    london_only: true
    layer: silver                      # Lookup table
    domain: enrichment
    velocity: batch
    update_frequency: 5y               # Census every 5 years

  # London Boundaries - LOW
  london_geographies:
    source: "https://data.london.gov.uk"
    layers:
      - borough_boundaries
      - ward_boundaries
      - msoa_boundaries
      - lsoa_boundaries
    format: GeoJSON
    layer: silver
    domain: enrichment
    velocity: batch

  # OS Open Roads - LOW
  os_open_roads:
    source: "https://osdatahub.os.uk/downloads/open/OpenRoads"
    extent: london
    format: GeoJSON
    layer: bronze
    domain: enrichment
    velocity: batch
```

---

## 6. Volume ước tính

### 6.1 Per-Source Volume Estimates

| Source | Bronze Rows | Bronze Size | Silver Rows | Silver Size |
|--------|-------------|-------------|-------------|-------------|
| londonair_monitoring | 22 triệu | 180 MB | 15 triệu | 120 MB |
| openmeteo_weather | 29 triệu | 220 MB | 20 triệu | 150 MB |
| openaq_measurements | 1 triệu | 8 MB | 800K | 6 MB |
| tfl_arrivals (7 days) | 5 triệu | 40 MB | 4 triệu | 32 MB |
| tfl_line_status (30 days) | 12K | 1 MB | 10K | 0.5 MB |
| london_journeys | 2K | 0.5 MB | 2K | 0.5 MB |
| dft_road_traffic | 6 triệu | 80 MB | 1 triệu | 15 MB |
| stats19 (GB 10yr) | 5.4 triệu | 400 MB | 250K | 20 MB |
| nomis_census | 75K | 5 MB | 75K | 5 MB |
| **TOTAL** | **~68 triệu** | **~935 MB** | **~41 triệu** | **~350 MB** |

### 6.2 Volume by Demo Level

```
┌─────────────────────────────────────────────────────────────────┐
│                    VOLUME TARGET MATRIX                         │
├─────────────────┬───────────────┬───────────────┬───────────────┤
│    Level        │  Bronze Rows  │  Silver Rows  │    Time       │
├─────────────────┼───────────────┼───────────────┼───────────────┤
│ Minimal         │   5-10 triệu  │   3-5 triệu   │   1-2 ngày    │
│ Good            │  20-50 triệu  │  10-20 triệu  │   3-5 ngày    │
│ Impressive      │ 100+ triệu    │  50+ triệu    │   7-14 ngày   │
└─────────────────┴───────────────┴───────────────┴───────────────┘
```

### 6.3 Storage Requirements

| Layer | Min | Good | Impressive |
|-------|-----|------|------------|
| Bronze | 100 MB | 500 MB | 2 GB |
| Silver | 50 MB | 250 MB | 1 GB |
| Gold | 10 MB | 50 MB | 200 MB |
| DLQ | 5 MB | 20 MB | 50 MB |
| **Total** | **~165 MB** | **~820 MB** | **~3.25 GB** |

---

## 7. Thứ tự triển khai đề xuất

### Phase 1: Quick Wins (Ngày 1-2)

**Mục tiêu:** Đạt 10-15 triệu rows nhanh

```
1. LondonAir Expansion
   ├── Remove site_limit → null
   ├── Add all species
   └── Extend years to 2020-2025
   Expected: +20 triệu rows
   
2. Open-Meteo Historical
   ├── Setup 33 borough coordinates
   ├── Pull 2020-2025 data
   └── Add weather + air quality variables
   Expected: +25 triệu rows
```

**Commands:**
```bash
# LondonAir expansion
python scripts/download_data.py \
  --source londonair_monitoring \
  --run-id expand001 \
  --mode full_demo

# Open-Meteo
python scripts/download_data.py \
  --source openmeteo_weather \
  --run-id expand002 \
  --mode full_demo
```

---

### Phase 2: Transport Streaming (Ngày 3-5)

**Mục tiêu:** Add streaming capabilities

```
3. TfL Arrivals Streaming
   ├── Setup Kafka producer
   ├── Configure 100 stop monitoring
   └── Run 8-hour demo session
   Expected: +500K events/session
   
4. TfL Line Status Streaming
   ├── Setup Kafka producer
   ├── Poll every 5 minutes
   └── Run continuous
   Expected: +1K events/day
```

**Commands:**
```bash
# Start Kafka producer
$env:PYTHONPATH = "."; .\venv\Scripts\python.exe `
  ingestion/streaming/producer.py `
  --source tfl_arrivals `
  --stop-limit 100 `
  --interval 60

# Start consumer
$env:PYTHONPATH = "."; .\venv\Scripts\python.exe `
  ingestion/streaming/consumer.py `
  --topic tfl-arrivals `
  --dataset tfl_arrivals
```

---

### Phase 3: Enrichment (Ngày 6-10)

**Mục tiêu:** Add context và reference data

```
5. STATS19 Expansion
   ├── Add vehicles + casualties tables
   ├── Extend to 2015-2024
   └── GB-wide raw, London silver
   Expected: +4 triệu rows

6. DfT Traffic Expansion
   ├── Extend to 10 years
   ├── Add automatic counts
   └── London-focused silver
   Expected: +5 triệu rows

7. Census Integration
   ├── Download MSOA boundaries
   ├── Pull census variables
   └── Setup as lookup table
   Expected: +75K rows
```

---

### Phase 4: Polish (Ngày 11-14)

**Mục tiêu:** Final touches và validation

```
8. OpenAQ Integration
   ├── Setup London bounding box
   ├── Pull historical data
   └── Cross-validate with LondonAir
   Expected: +500K rows

9. Data Quality Validation
   ├── Run GX checks
   ├── Validate coverage
   └── Generate profile reports

10. Documentation Update
    ├── Update run manifests
    ├── Document volume metrics
    └── Prepare demo scripts
```

---

## 8. Nguồn tích hợp thêm cho giai đoạn mở rộng

Phần này là hướng mở rộng sau khi các source hiện có đã tải ổn định vào runtime local hoặc VM (`/data`). Các nguồn dưới đây đã được thêm cấu hình định hướng trong `config/download_defaults.yml`, nhưng chưa nên đưa vào source group chạy tự động cho tới khi có adapter ingestion tương ứng. Cách đi hợp lý là giữ Bronze raw đầy đủ trước, sau đó chuẩn hóa Silver theo từng output.

### 8.1 `tfl_live_traffic_disruptions`

**Mức ưu tiên:** Cao cho transport realtime. Nguồn này bổ sung road events mà TfL đang cập nhật từ traffic control centre, phù hợp để join với DfT road traffic, STATS19 và thời tiết/air quality.

**Cách tích hợp phù hợp nhất:** bắt đầu bằng TIMS legacy XML feed vì không cần auth và cấu trúc ổn định cho polling 5 phút. Unified API `/Road/all/Disruption` nên để optional path dùng `TFL_API_KEY` khi cần JSON hoặc enrich metadata.

**Bronze:** lưu nguyên XML theo partition `date/hour/poll_time`, manifest phải ghi `source_url`, `poll_time`, `etag/last_modified` nếu có, và hash để dedupe.

**Silver đề xuất:**
- `tfl_traffic_disruptions`: một record cho mỗi disruption, gồm id, category, status, severity, start/end time, location text, comments, current update, last modified.
- `tfl_traffic_disruption_geometries`: tách point/line/polygon từ `CauseArea`, giữ cả British National Grid và WGS84 nếu feed cung cấp.

```yaml
tfl_live_traffic_disruptions:
  velocity: streaming
  ingestion_mode: polling_xml
  poll_interval_sec: 300
  source_url: "https://tfl.gov.uk/tfl/syndication/feeds/tims_feed.xml"
  api_url: "https://api.tfl.gov.uk/Road/all/Disruption"
  expected_format: xml
  silver_outputs:
    - tfl_traffic_disruptions
    - tfl_traffic_disruption_geometries
```

### 8.2 `tfl_bikepoint_occupancy`

**Mức ưu tiên:** Cao cho active mobility realtime. Nguồn này tạo time-series occupancy cho Santander Cycles, giúp demo velocity rõ hơn TfL line status vì mỗi poll sinh nhiều station snapshots.

**Cách tích hợp phù hợp nhất:** dùng XML feed `livecyclehireupdates.xml` trước vì public/no auth. Unified API `/BikePoint` giữ làm fallback hoặc enrich station metadata.

**Bronze:** lưu raw XML mỗi 5 phút. Cần giữ snapshot time từ feed nếu có; nếu không có thì dùng `poll_time`/`ingested_at`.

**Silver đề xuất:**
- `tfl_bikepoint_occupancy`: station snapshot với `nb_bikes`, `nb_empty_docks`, `nb_docks`, `nb_spaces`, `capacity`, trạng thái `installed/locked/temporary`.
- `tfl_bikepoint_station_reference`: thông tin station tương đối ổn định như id, name, lat, lon, capacity.

```yaml
tfl_bikepoint_occupancy:
  velocity: streaming
  ingestion_mode: polling_xml
  poll_interval_sec: 300
  source_url: "https://www.tfl.gov.uk/tfl/syndication/feeds/cycle-hire/livecyclehireupdates.xml"
  api_url: "https://api.tfl.gov.uk/BikePoint"
  silver_outputs:
    - tfl_bikepoint_occupancy
    - tfl_bikepoint_station_reference
```

### 8.3 `ea_hydrology_rainfall_river`

**Mức ưu tiên:** Trung bình-cao cho environment. Nguồn này bổ sung hydrology, rainfall và flood context, rất hợp để phân tích tác động mưa lớn lên traffic disruption, accident risk và air quality.

**Cách tích hợp phù hợp nhất:** tách thành hai nhánh. Flood Monitoring API dùng micro-batch `readings?latest` mỗi 15 phút cho near realtime. Hydrology API dùng batch/micro-batch để discover stations quanh London và tải readings theo khoảng ngày.

**Bronze:** lưu JSON response nguyên bản, giữ `meta` vì có licence, publisher, version, limit và documentation.

**Silver đề xuất:**
- `ea_hydrology_stations`
- `ea_rainfall_readings`
- `ea_river_level_readings`
- `ea_river_flow_readings`
- `ea_flood_alerts`

```yaml
ea_hydrology_rainfall_river:
  velocity: micro_batch
  apis:
    flood_monitoring:
      base_url: "https://environment.data.gov.uk/flood-monitoring"
      latest_readings: "/data/readings?latest"
      poll_interval_sec: 900
    hydrology:
      base_url: "https://environment.data.gov.uk/hydrology"
      station_endpoint: "/id/stations"
      readings_endpoint: "/data/readings"
  observed_properties:
    - rainfall
    - waterLevel
    - waterFlow
```

### 8.4 `defra_noise_mapping`

**Mức ưu tiên:** Trung bình cho demo, cao cho variety. Đây là nguồn geospatial/modelled, không phải sensor realtime, nên nên đưa vào batch enrichment thay vì realtime polling.

**Cách tích hợp phù hợp nhất:** bước đầu tải qua UI “Download data by area of interest” để inspect layer/format cho London. Sau khi chắc format, mới tự động hóa WCS theo bbox London. Adapter cần hỗ trợ geospatial/raster, CRS `EPSG:27700`, clipping/reprojection và chuyển sang grid hoặc borough aggregate.

**Bronze:** lưu file geospatial/raster nguyên bản kèm metadata WCS/GetCapabilities.

**Silver đề xuất:**
- `defra_road_noise_grid`
- `defra_rail_noise_grid`
- `noise_by_borough`
- `noise_exposure_joined_with_transport`

```yaml
defra_noise_mapping:
  velocity: batch
  source_type: geospatial_modelled_noise
  datasets:
    road_noise_all_metrics:
      wcs: "https://environment.data.gov.uk/spatialdata/road-noise-all-metrics-england-round-4/wcs"
    rail_noise_all_metrics:
      wcs: "https://environment.data.gov.uk/spatialdata/rail-noise-all-metrics-england-round-4/wcs"
  geography:
    target_area: london
    crs: "EPSG:27700"
```

### 8.5 Đánh giá tích hợp

| Source | Hợp lý | Chưa hợp lý / cần làm trước |
|--------|--------|------------------------------|
| `tfl_live_traffic_disruptions` | Rất hợp với realtime transport, poll 5 phút, raw XML dễ lưu Bronze | Cần parser XML và schema Silver riêng cho geometry |
| `tfl_bikepoint_occupancy` | Rất hợp để tạo time-series dày, public feed không auth | Cần dedupe snapshot và tách station reference khỏi occupancy |
| `ea_hydrology_rainfall_river` | Bổ sung environment micro-batch, metadata tốt, không cần API key | Cần strategy station discovery quanh London trước khi tải historical lớn |
| `defra_noise_mapping` | Tăng variety geospatial và liên kết transport-environment | Chưa nên chạy tự động nếu pipeline chưa có raster/WCS handling |

Thứ tự implement khuyến nghị: `tfl_bikepoint_occupancy` trước, sau đó `tfl_live_traffic_disruptions`, tiếp theo `ea_hydrology_rainfall_river`, cuối cùng mới đến `defra_noise_mapping`.

---

## Checklist Triển khai

### Pre-flight Check
- [ ] Docker services running (Kafka, Zookeeper, Spark)
- [ ] venv activated với dependencies
- [ ] Runtime directories clean
- [ ] .env configured

### Phase 1 Checklist
- [ ] LondonAir: site_limit = null
- [ ] LondonAir: all species added
- [ ] LondonAir: years = [2020-2025]
- [ ] Open-Meteo: 33 boroughs configured
- [ ] Open-Meteo: 2020-2025 data pulled
- [ ] Open-Meteo: weather + AQ variables

### Phase 2 Checklist
- [ ] Kafka topics created
- [ ] TfL API credentials (optional)
- [ ] Producer running continuously
- [ ] Consumer landing to bronze
- [ ] Bronze→Silver conversion working
- [ ] Silver data queryable

### Phase 3 Checklist
- [ ] STATS19: 3 tables (accidents, vehicles, casualties)
- [ ] STATS19: 2015-2024 years
- [ ] DfT: 10-year traffic data
- [ ] Census: MSOA boundaries loaded
- [ ] Census: 2021 variables available

### Phase 4 Checklist
- [ ] OpenAQ: London bbox configured
- [ ] OpenAQ: 500K+ rows loaded
- [ ] GX: All sources validated
- [ ] Profile reports generated
- [ ] Demo scripts ready

### Post-deployment Check
- [ ] Bronze layer accessible
- [ ] Silver layer accessible
- [ ] Streaming pipeline operational
- [ ] DLQ monitoring active
- [ ] Documentation updated
