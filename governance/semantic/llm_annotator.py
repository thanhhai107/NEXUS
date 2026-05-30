"""LLM-based Semantic Annotator.

Uses Ollama LLM to generate semantic annotations for fields.
"""

from __future__ import annotations

import json
import time
from typing import Any


class OllamaAnnotator:
    """Annotates fields using LLM."""
    
    def __init__(
        self,
        model: str = "qwen2.5:0.5b",
        base_url: str = "http://localhost:11434",
        timeout: int = 180,
    ):
        self.model = model
        self.base_url = base_url
        self.timeout = timeout
    
    def annotate(
        self,
        source_id: str,
        fields: dict[str, Any],
        api_docs: str | None = None,
        samples: list[dict] | None = None,
        domain: str = "unknown",
    ) -> dict[str, dict[str, Any]]:
        """Annotate fields using LLM."""
        try:
            import requests
        except ImportError:
            return {}
        
        # Build prompt
        field_list = ", ".join(fields.keys())
        
        prompt = f"""You are a data engineer annotating fields from a {domain} dataset.
Dataset: {source_id}

Fields to annotate: {field_list}

"""
        if api_docs:
            prompt += f"API Documentation:\n{api_docs[:2000]}\n\n"
        
        if samples:
            prompt += f"Sample data:\n{json.dumps(samples[:3], indent=2)}\n\n"
        
        prompt += """For each field, provide:
- description: What this field represents
- role: One of: identifier, timestamp, location, measurement, attribute, index, event, name, description
- confidence: 0.0 to 1.0

Return JSON with field names as keys.
"""

        try:
            response = requests.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                },
                timeout=self.timeout,
            )
            
            if response.status_code == 200:
                result = response.json()
                text = result.get("response", "")
                
                # Try to parse JSON from response
                try:
                    # Find JSON in response
                    start = text.find("{")
                    end = text.rfind("}") + 1
                    if start >= 0 and end > start:
                        return json.loads(text[start:end])
                except json.JSONDecodeError:
                    pass
        except Exception as e:
            print(f"LLM annotation failed: {e}")
        
        return {}
    
    def check_health(self) -> dict[str, Any]:
        """Check if LLM is available."""
        try:
            import requests
            response = requests.get(
                f"{self.base_url}/api/tags",
                timeout=5,
            )
            
            if response.status_code == 200:
                models = response.json().get("models", [])
                return {
                    "available": True,
                    "models": [m.get("name") for m in models],
                }
        except Exception:
            pass
        
        return {
            "available": False,
            "error": "Cannot connect to Ollama",
        }
