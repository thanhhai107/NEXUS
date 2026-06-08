"""LLM-based Semantic Annotator.

Uses Amazon Bedrock (default) or OpenAI-compatible API
to generate semantic annotations for fields.
"""

from __future__ import annotations

import json
import os
from typing import Any

from governance.llm.client import chat, check_health as llm_check_health


class BedrockAnnotator:
    """Annotates fields using an LLM."""

    def __init__(
        self,
        model: str = "amazon.nova-pro-v1:0",
        region: str = "us-east-1",
        timeout: int = 180,
    ):
        self.model = os.getenv("NEXUS_AGENT_MODEL", model)
        self.region = os.getenv("AWS_DEFAULT_REGION", region)
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
            text = chat(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                system="Return only valid JSON. No markdown.",
                temperature=0.3,
                max_tokens=2048,
            )

            start = text.find("{")
            end = text.rfind("}") + 1
            if start >= 0 and end > start:
                return json.loads(text[start:end])
        except Exception as e:
            print(f"LLM annotation failed: {e}")

        return {}

    def check_health(self) -> dict[str, Any]:
        """Check if the configured LLM is accessible."""
        return llm_check_health()
