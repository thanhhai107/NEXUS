"""
LLM Annotator using Ollama API.

Local inference via Ollama HTTP API.
Default model: qwen2.5:0.5b (lightweight, fast)
"""

from __future__ import annotations

import json
import requests
from dataclasses import dataclass
from typing import Any

# Default settings
DEFAULT_MODEL = "qwen2.5:0.5b"
DEFAULT_BASE_URL = "http://localhost:11434"
DEFAULT_TIMEOUT = 180  # 3 minutes
DEFAULT_MAX_TOKENS = 2048
DEFAULT_TEMPERATURE = 0.3


@dataclass
class LLMAnnotation:
    """Single field annotation from LLM."""
    
    description: str
    role: str
    unit: str | None = None
    confidence: float = 0.85
    glossary_term: str | None = None
    notes: str | None = None


class OllamaAnnotator:
    """
    LLM Annotator using Ollama API.
    
    Default model: qwen2.5:0.5b (lightweight, fast)
    Alternative: phi3.5-mini (better quality, more resources)
    
    Usage:
        annotator = OllamaAnnotator()
        
        annotations = annotator.annotate(
            source_id="tfl_arrivals",
            fields={"expected_arrival": field_schema},
            api_docs="API documentation text...",
            samples=[{"expected_arrival": "2024-01-01T12:00:00Z"}],
        )
    """
    
    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = DEFAULT_TIMEOUT,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        temperature: float = DEFAULT_TEMPERATURE,
    ):
        """
        Initialize Ollama annotator.
        
        Args:
            model: Ollama model name (default: qwen2.5:0.5b)
            base_url: Ollama API base URL
            timeout: Request timeout in seconds
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature (0.0-1.0)
        """
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_tokens = max_tokens
        self.temperature = temperature
    
    def annotate(
        self,
        source_id: str,
        fields: dict[str, Any],
        api_docs: str | None = None,
        samples: list[dict] | None = None,
        domain: str = "unknown",
    ) -> dict[str, dict]:
        """
        Annotate fields using LLM.
        
        Args:
            source_id: Source identifier
            fields: Dict of field_name -> FieldSchema or dict
            api_docs: API documentation text (optional)
            samples: Sample data records (optional)
            domain: Domain name (optional, for context)
        
        Returns:
            Dict of field_name -> annotation dict
        """
        # Build schema JSON for prompt
        schema_data = self._build_schema_json(fields)
        
        # Build samples JSON
        samples_json = json.dumps(samples[:5] if samples else [], indent=2, ensure_ascii=False)
        
        # Build prompt
        prompt = self._build_prompt(
            source_id=source_id,
            schema_json=schema_data,
            api_docs=api_docs or "Not available",
            samples_json=samples_json,
            domain=domain,
        )
        
        # Call LLM
        response = self._call_llm(prompt)
        
        # Parse response
        annotations = self._parse_response(response)
        
        return annotations
    
    def _build_schema_json(self, fields: dict[str, Any]) -> str:
        """
        Build schema JSON for prompt.
        
        Args:
            fields: Dict of field_name -> FieldSchema
        
        Returns:
            JSON string
        """
        schema_data = {}
        
        for name, field in fields.items():
            # Support both FieldSchema objects and dicts
            if hasattr(field, "to_dict"):
                field_dict = field.to_dict()
            else:
                field_dict = field
            
            schema_data[name] = {
                "type": field_dict.get("type", "unknown"),
                "nullable": field_dict.get("nullable", False),
                "sample": field_dict.get("sample_values", field_dict.get("sample", []))[:3],
                "pattern": field_dict.get("pattern"),
            }
        
        return json.dumps(schema_data, indent=2, ensure_ascii=False)
    
    def _build_prompt(
        self,
        source_id: str,
        schema_json: str,
        api_docs: str,
        samples_json: str,
        domain: str,
    ) -> str:
        """
        Build full prompt for LLM.
        
        Args:
            source_id: Source identifier
            schema_json: Schema as JSON string
            api_docs: API documentation text
            samples_json: Samples as JSON string
            domain: Domain name
        
        Returns:
            Full prompt string
        """
        return f"""You are a data engineering expert with 10 years of experience.

You understand:
- Data modeling (dimensional, normalized)
- Business intelligence and analytics
- API documentation and field semantics
- Data quality and validation

CRITICAL RULES:
1. ONLY annotate based on EXPLICIT information from the documentation or samples
2. If the documentation does NOT explain a field, leave description and role EMPTY (not guessed)
3. Never invent business meaning for undocumented fields
4. confidence MUST be below 0.6 if documentation is unclear or insufficient
5. When in doubt, leave blank rather than hallucinate

## Task

Add semantic metadata ONLY if documentation provides clear information.

## Source: {source_id}
## Domain: {domain}

## Schema (inferred from data):
```json
{schema_json}
```

## API Documentation:
{api_docs}

## Sample Data (5 rows):
```json
{samples_json}
```

## Output Format (JSON only, no markdown):
```json
{{
  "field_name": {{
    "description": "ONLY if documentation clearly states the meaning",
    "role": "ONLY if clearly inferable from docs (dimension|measure|etc.)",
    "unit": "SI unit or null",
    "confidence": 0.0-0.95 based on how certain you are
  }}
}}
```

Leave fields EMPTY if documentation does not provide clear information. Better to leave blank than to guess incorrectly.

Return valid JSON only. No explanations."""
    
    def _call_llm(self, prompt: str) -> str:
        """
        Call Ollama API.
        
        Args:
            prompt: Full prompt string
        
        Returns:
            LLM response text
        
        Raises:
            RuntimeError: If Ollama is not available
        """
        url = f"{self.base_url}/api/generate"
        
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": self.temperature,
                "num_predict": self.max_tokens,
            }
        }
        
        try:
            response = requests.post(
                url,
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            
            result = response.json()
            return result.get("response", "")
            
        except requests.exceptions.ConnectionError:
            raise RuntimeError(
                f"Cannot connect to Ollama at {self.base_url}. "
                f"Make sure Ollama is running: `ollama serve`"
            )
        except requests.exceptions.Timeout:
            raise RuntimeError(
                f"Ollama request timed out after {self.timeout}s. "
                f"Try a smaller model or increase timeout."
            )
        except requests.exceptions.HTTPError as e:
            raise RuntimeError(f"Ollama API error: {e}")
    
    def _parse_response(self, response: str) -> dict[str, dict]:
        """
        Parse LLM response into annotations dict.
        
        Args:
            response: Raw LLM response text
        
        Returns:
            Dict of field_name -> annotation dict
        """
        # Try to extract JSON from response
        text = response.strip()
        
        # Remove markdown code blocks if present
        if text.startswith("```"):
            lines = text.split("\n")
            json_lines = []
            in_json = False
            for line in lines:
                if line.strip().startswith("```"):
                    in_json = not in_json
                    continue
                if in_json:
                    json_lines.append(line)
            text = "\n".join(json_lines)
        
        # Try to parse as JSON
        try:
            annotations = json.loads(text)
            if isinstance(annotations, dict):
                return self._validate_annotations(annotations)
        except json.JSONDecodeError:
            pass
        
        # Try to find JSON in text
        import re
        
        # Look for JSON object pattern
        json_pattern = r'\{[^{}]*"[a-zA-Z_][a-zA-Z0-9_]*"[^{}]*\}'
        matches = re.findall(json_pattern, text, re.DOTALL)
        
        if matches:
            for match in matches:
                try:
                    parsed = json.loads(match)
                    if isinstance(parsed, dict):
                        return self._validate_annotations(parsed)
                except json.JSONDecodeError:
                    continue
        
        print(f"Warning: Could not parse LLM response as JSON. Response preview: {text[:200]}")
        return {}
    
    def _validate_annotations(self, annotations: dict[str, dict]) -> dict[str, dict]:
        """
        Validate and clean annotations.
        
        Stricter validation to prevent hallucination:
        - Low confidence (< 0.4) or empty description = no annotation
        - Invalid role = no annotation
        - Generic descriptions that indicate hallucination = no annotation
        
        Args:
            annotations: Raw annotations from LLM
        
        Returns:
            Validated annotations dict
        """
        import re
        
        valid_roles = {
            "primary_key", "foreign_key", "measure", "dimension",
            "event_time", "ingestion_time", "status", "flag",
            "geospatial", "temporal", "descriptive", "metadata"
        }
        
        # Patterns that indicate hallucination or generic descriptions
        # These are phrases that don't provide real semantic information
        generic_patterns = [
            # Repeating field name as "X is the Y of Z"
            r"^[\w]+ is the \w+",
            r"^[\w]+ is an? \w+",
            # "Unique identifier for each X"
            r"unique identifier for each",
            r"unique id for each",
            r"identifier for each",
            # "Name of each X"
            r"^name of each",
            r"^the name of the",
            r"^the \w+ of the \w+",
            # Generic descriptions
            r"in the data$",
            r"in the dataset$",
            r"in the record$",
            r"^this field",
            r"^the field",
            r"^field ",
            # Just repeating field name
            r"^(id|name|value|type|code|status|flag|date|time)\s",
            # "for each" patterns
            r"for each \w+ in",
        ]
        generic_regex = [re.compile(p, re.IGNORECASE) for p in generic_patterns]
        
        validated = {}
        
        for field_name, annotation in annotations.items():
            if not isinstance(annotation, dict):
                continue
            
            description = str(annotation.get("description", "")).strip()
            role = str(annotation.get("role", "")).strip().lower()
            confidence = float(annotation.get("confidence", 0.5))
            
            # Skip if description is empty
            if not description:
                continue
            
            # Skip if description is too short (< 15 chars)
            if len(description) < 15:
                continue
            
            # Skip if role is empty or invalid
            if not role or role not in valid_roles:
                continue
            
            # Skip if confidence is too low
            if confidence < 0.4:
                continue
            
            # Skip if description matches generic patterns (hallucination)
            is_generic = False
            for pattern in generic_regex:
                if pattern.search(description):
                    is_generic = True
                    break
            
            if is_generic:
                continue
            
            # Build validated annotation
            cleaned = {
                "description": description[:200],
                "role": role,
                "unit": annotation.get("unit"),
                "confidence": min(max(confidence, 0.4), 0.95),
                "source": "llm",
            }
            
            # Remove None values
            cleaned = {k: v for k, v in cleaned.items() if v is not None}
            
            validated[field_name] = cleaned
        
        return validated
    
    def check_health(self) -> dict[str, Any]:
        """
        Check if Ollama is running and model is available.
        
        Returns:
            Dict with health status
        """
        try:
            # Check server
            response = requests.get(
                f"{self.base_url}/",
                timeout=5
            )
            server_ok = response.status_code == 200
        except Exception:
            server_ok = False
        
        try:
            # Check model
            response = requests.get(
                f"{self.base_url}/api/tags",
                timeout=5
            )
            models = []
            if response.status_code == 200:
                data = response.json()
                models = [m.get("name", "") for m in data.get("models", [])]
            
            model_available = self.model in models
        except Exception:
            models = []
            model_available = False
        
        return {
            "server_ok": server_ok,
            "model_available": model_available,
            "model": self.model,
            "available_models": models,
            "base_url": self.base_url,
        }
