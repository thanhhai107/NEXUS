# Semantic Annotation Module

LLM-assisted semantic metadata enrichment for NEXUS data fields using Amazon Bedrock.

## Overview

This module enriches inferred schemas with semantic metadata (description, role, unit) by:
1. Matching field names with rule-based templates
2. Calling Amazon Bedrock for fields that still need semantic context
3. Caching annotations to avoid repeated LLM calls

The annotator is strict by design: weak or generic outputs are filtered to reduce hallucinations.

## Runtime Requirements

- AWS credentials are available to boto3 (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`)
- Region is configured (`AWS_DEFAULT_REGION`, default `us-east-1`)
- The account/role has access to `amazon.nova-pro-v1:0` in Bedrock

## Configuration

Semantic annotation settings are read from `config/download_defaults.yml` under `semantic_annotation`.

Example:

```yaml
semantic_annotation:
  enabled: true
  llm_model: "amazon.nova-pro-v1:0"
  bedrock_region: "us-east-1"
  llm_timeout_seconds: 180
  trigger:
    min_new_fields: 3
    reannotate_threshold: 10
  docs_urls:
    openaq: "https://docs.openaq.org/resources/measurements"
    naptan: "https://naptan.api.dft.gov.uk/swagger/index.html"
```

## Usage

### Automatic (Downloader flow)

Semantic annotation runs after schema inference in ingestion flows:

```bash
python -m ingestion.downloaders.london_downloader --source tfl_arrivals
```

### Programmatic

```python
from ingestion.semantic import SemanticAnnotationPipeline

pipeline = SemanticAnnotationPipeline(
    cache_dir="runtime/semantic_cache",
    llm_model="amazon.nova-pro-v1:0",
    llm_region="us-east-1",
)

result = pipeline.process(
    source_id="tfl_arrivals",
    source_key="tfl_arrivals",
    inferred_schema=schema,
    docs_url="https://api.tfl.gov.uk/",
    domain="transport",
)
```

## Output Format

```json
{
  "field_name": {
    "description": "Business meaning from documentation",
    "role": "dimension",
    "unit": "seconds",
    "confidence": 0.85,
    "source": "llm"
  }
}
```

Fields with insufficient evidence are left empty and flagged for review.

## Troubleshooting

### Access Denied

Ensure the IAM principal has Bedrock invoke permissions and model access in the selected region.

### ThrottlingException

Daily or request-level quotas are exceeded. Retry later or request higher Bedrock quotas.

### Empty Annotations

This is expected when:
- Source docs are missing or low quality
- Fields are ambiguous
- Validation filters reject low-confidence outputs

## Architecture

```
Download -> Schema Inference -> SemanticAnnotationPipeline
                                     |
                                     +-> SchemaDiffDetector
                                     +-> TemplateAnnotator
                                     +-> BedrockAnnotator
                                     +-> SemanticCache
```
