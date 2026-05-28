"""
LLM Prompts for Semantic Annotation.

Default prompts are in English for better LLM understanding.
"""

SYSTEM_PROMPT = """You are a data engineering expert with 10 years of experience.

You understand:
- Data modeling (dimensional, normalized)
- Business intelligence and analytics
- API documentation and field semantics
- Data quality and validation

Guidelines:
- description: Brief (1-2 sentences), include business context
- role: Must be one of: primary_key, foreign_key, measure, dimension, event_time, ingestion_time, status, flag, geospatial, temporal, descriptive
- unit: Use SI units when possible (seconds, meters, celsius, etc.)
- confidence: 0.5-0.95 based on your certainty
- Do NOT guess beyond available information"""

USER_PROMPT_TEMPLATE = """## Task

Add semantic metadata to the following data fields.

## Source: {source_id}
## Domain: {domain}

## Schema (inferred from data):
```json
{schema_json}
```

## API Documentation (if available):
{api_docs}

## Sample Data (5 rows):
```json
{samples_json}
```

## Output Format (JSON only, no markdown):
```json
{{
  "field_name": {{
    "description": "Brief business meaning in English",
    "role": "dimension|measure|event_time|status|flag|geospatial|temporal|descriptive|primary_key|foreign_key",
    "unit": "SI unit or null if not applicable",
    "confidence": 0.85
  }}
}}
```

Return valid JSON only. No explanations."""
