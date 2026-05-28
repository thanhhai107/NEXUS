# Semantic Annotation Module

LLM-assisted semantic metadata enrichment for NEXUS data fields using Ollama.

## Overview

This module automatically enriches data field schemas with semantic metadata (descriptions, roles, units) by:
1. Matching field names against known patterns (template matching)
2. Using LLM with API documentation to infer semantic meaning
3. Caching results to avoid redundant LLM calls

**Key Feature**: LLM only returns annotations when documentation provides clear information. Generic/hallucinated descriptions are filtered out.

## Setup

### Local Machine (Windows/Mac/Linux)

#### 1. Install Ollama

```bash
# Windows: Download from https://ollama.com/download
# Mac/Linux:
curl -fsSL https://ollama.com/install.sh | sh
```

#### 2. Download Model

```bash
ollama pull qwen2.5:0.5b
```

#### 3. Start Ollama

```bash
ollama serve

# Verify it's running
curl http://localhost:11434
```

### VM Server

```bash
# 1. SSH to VM
ssh user@vm-server

# 2. Install Ollama
curl -fsSL https://ollama.com/install.sh | sh

# 3. Start Ollama service
ollama serve &

# 4. Pull model
ollama pull qwen2.5:0.5b

# 5. Verify
ollama list
```

## Configuration

### Config File: `config/download_defaults.yml`

```yaml
semantic_annotation:
  enabled: true
  llm_model: "qwen2.5:0.5b"
  ollama_url: "http://localhost:11434"
  llm_timeout_seconds: 180
  
  trigger:
    min_new_fields: 3        # Call LLM if >= 3 new fields
    reannotate_threshold: 10 # Re-annotate if >= 10 new fields

  docs_urls:
    # Transport
    tfl: ""
    tfl_arrivals: ""
    tfl_line_status: ""
    london_journeys: "https://data.london.gov.uk/dataset/tfl-journeys-type"
    naptan: "https://naptan.api.dft.gov.uk/swagger/index.html"

    # Air Quality
    openaq: "https://docs.openaq.org/resources/measurements"
    waqi: "https://aqicn.org/json-api/doc/"
    londonair: "https://api.erg.ic.ac.uk/AirQuality/help"
    ukair_air_quality_archive: "https://uk-air.defra.gov.uk/data/flat_files"

    # Weather
    openmeteo: "https://open-meteo.com/en/docs"
    openmeteo_historical_weather: "https://open-meteo.com/en/docs"
    openweather: "https://openweathermap.org/api/one-call-4"
    ncei: "https://www.ncei.noaa.gov/cdo-web/webservices/v2"

    # Road Safety
    stats19: "https://www.gov.uk/government/statistical-data-sets/road-safety-open-data"
    dft: "https://storage.googleapis.com/dft-statistics/road-traffic/all-traffic-data-metadata.pdf"
```

## Data Sources

| Source | Domain | Docs URL |
|--------|--------|----------|
| `tfl` | Transport | *(empty - no docs)* |
| `tfl_arrivals` | Transport | *(empty - no docs)* |
| `tfl_line_status` | Transport | *(empty - no docs)* |
| `london_journeys` | Transport | London Datastore |
| `naptan` | Transport | NaPTAN API Swagger |
| `openaq` | Air Quality | OpenAQ Measurements API |
| `waqi` | Air Quality | WAQI JSON API |
| `londonair` | Air Quality | LondonAir API |
| `ukair_air_quality_archive` | Air Quality | UK-AIR Archive |
| `openmeteo` | Weather | Open-Meteo Docs |
| `openmeteo_historical_weather` | Weather | Open-Meteo Docs |
| `openweather` | Weather | OpenWeather API |
| `ncei` | Climate | NCEI CDO Web Services |
| `stats19` | Road Safety | UK Gov Stats |
| `dft` | Road Traffic | DFT PDF Metadata |

## Usage

### Automatic (via Downloader)

Semantic annotation runs automatically after schema inference:

```bash
python -m ingestion.downloaders.london_downloader --source tfl_arrivals
```

Output:
```
[tfl_arrivals] schema inferred: fields=27 records=61 path=...
[DEBUG] LLM returned empty for 25/25 fields
[tfl_arrivals] semantic metadata: path=runtime/semantic_cache/tfl_arrivals/v3cb2/annotations.json
[tfl_arrivals] success: rows=108 files=3 size_mb=0.103
```

### Programmatic

```python
from ingestion.semantic import SemanticAnnotationPipeline

pipeline = SemanticAnnotationPipeline(
    cache_dir="runtime/semantic_cache",
    llm_model="qwen2.5:0.5b"
)

result = pipeline.process(
    source_id="tfl_arrivals",
    source_key="tfl_arrivals",
    inferred_schema=schema,
    docs_url="https://api.tfl.gov.uk/",
    domain="transport"
)
```

## Cache Location

| Environment | Path |
|-------------|------|
| **Local** | `D:/.Kỳ II năm Ba/Big Data/NEXUS/runtime/semantic_cache/` |
| **VM** | `/data/semantic_cache/` |

```
runtime/semantic_cache/
├── tfl_arrivals/
│   └── v3cb2/
│       └── annotations.json
├── openaq/
│   └── v1a2b/
│       └── annotations.json
└── ...
```

## Annotation Format

```json
{
  "field_name": {
    "description": "Business meaning from documentation",
    "role": "dimension|measure|event_time|status|...",
    "unit": "seconds|meters|...",
    "confidence": 0.85,
    "source": "llm"
  },
  "_schema_hash": "3cb2",
  "_annotated_at": "2026-05-28T09:27:25",
  "_annotated_by": "llm"
}
```

Fields without documentation get empty annotations:
```json
{
  "unknown_field": {
    "description": "",
    "role": "",
    "confidence": 0.0,
    "source": "none",
    "needs_review": true
  }
}
```

## Anti-Hallucination

The system filters out LLM hallucinations:

### Rejected Patterns

These descriptions are filtered out:
- "X is the unique identifier for the record"
- "Unique identifier for each Y in the data"
- "Name of each station in the data"
- "in the data", "in the dataset"
- Generic field name repetitions

### Validation Rules

1. **Minimum length**: Description must be >= 15 characters
2. **No generic patterns**: Matches against 15+ hallucination patterns
3. **Confidence threshold**: Must be >= 0.4
4. **Valid role**: Must be one of the predefined roles
5. **No docs = empty**: Fields without documentation get empty annotation

## Troubleshooting

### Ollama Not Running

```bash
# Start Ollama
ollama serve

# Check status
curl http://localhost:11434
```

### Model Not Found

```bash
# Pull model
ollama pull qwen2.5:0.5b

# List models
ollama list
```

### All Fields Empty

This is expected when:
1. No documentation URL is configured
2. Documentation URL is invalid/empty
3. LLM returns generic descriptions (filtered out)

To verify:
```bash
# Check docs URLs in config
grep -A 30 "docs_urls:" config/download_defaults.yml
```

### Connection Refused

```python
import requests
response = requests.get("http://localhost:11434")
print(response.json())
```

## Model Options

| Model | Size | RAM | Use Case |
|-------|------|-----|----------|
| `qwen2.5:0.5b` | ~400MB | ~1GB | **Default** - Fast |
| `qwen2.5:1.5b` | ~1GB | ~2GB | Better quality |
| `phi3.5-mini` | ~2.4GB | ~3GB | Best quality |

To change model, update `config/download_defaults.yml`:
```yaml
semantic_annotation:
  llm_model: "qwen2.5:1.5b"
```

## Ollama Model Storage

| OS | Path |
|----|------|
| Linux | `~/.ollama/models/` |
| Mac | `~/.ollama/models/` |
| Windows | `C:\Users\<username>\.ollama\models\` |
| VM | `~/.ollama/models/` |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    INGESTION PIPELINE                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Download → Schema Inference → Semantic Annotation           │
│                                    │                         │
│                                    ▼                         │
│                        ┌───────────────────────┐            │
│                        │  SchemaDiffDetector   │            │
│                        │  (What changed?)      │            │
│                        └───────────┬───────────┘            │
│                                    │                        │
│                                    ▼                        │
│                        ┌───────────────────────┐            │
│                        │  TemplateAnnotator    │            │
│                        │  (Fast, pattern-based) │            │
│                        └───────────┬───────────┘            │
│                                    │                        │
│                         ┌──────────┴──────────┐            │
│                         │                     │             │
│                    matched              unmatched             │
│                    (cached)            (call LLM)           │
│                                    │                        │
│                                    ▼                        │
│                        ┌───────────────────────┐            │
│                        │  OllamaAnnotator      │            │
│                        │  + Validation         │            │
│                        │  (Anti-hallucination) │            │
│                        └───────────┬───────────┘            │
│                                    │                        │
│                                    ▼                        │
│                        ┌───────────────────────┐            │
│                        │  SemanticCache        │            │
│                        │  runtime/semantic_... │            │
│                        └───────────────────────┘            │
└─────────────────────────────────────────────────────────────┘
```
