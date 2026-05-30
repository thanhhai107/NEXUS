"""LLM-based Semantic Annotator.

Uses Amazon Bedrock to generate semantic annotations for fields.
"""

from __future__ import annotations

import json
import os
from typing import Any

import boto3


class BedrockAnnotator:
    """Annotates fields using Amazon Bedrock."""

    def __init__(
        self,
        model: str = "amazon.nova-pro-v1:0",
        region: str = "us-east-1",
        timeout: int = 180,
    ):
        self.model = os.getenv("NEXUS_AGENT_MODEL", model)
        self.region = os.getenv("AWS_DEFAULT_REGION", region)
        self.timeout = timeout
        self.client = None

    def annotate(
        self,
        source_id: str,
        fields: dict[str, Any],
        api_docs: str | None = None,
        samples: list[dict] | None = None,
        domain: str = "unknown",
    ) -> dict[str, dict[str, Any]]:
        """Annotate fields using LLM via Bedrock."""
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
            if self.client is None:
                self.client = boto3.client(
                    "bedrock-runtime",
                    region_name=self.region,
                )

            response = self.client.converse(
                modelId=self.model,
                system=[{"text": "Return only valid JSON. No markdown."}],
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={
                    "temperature": 0.3,
                    "maxTokens": 2048,
                },
            )

            text = response["output"]["message"]["content"][0]["text"]

            # Try to parse JSON from response
            try:
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
        """Check if Bedrock is accessible."""
        try:
            bedrock = boto3.client("bedrock", region_name=self.region)
            response = bedrock.list_foundation_models(
                byProvider="Amazon",
                byOutputModality="TEXT",
            )
            model_summaries = response.get("modelSummaries", [])
            model_ids = [m["modelId"] for m in model_summaries]
            model_available = any(self.model in mid for mid in model_ids)
            return {
                "available": True,
                "models": model_ids[:20],
                "model_available": model_available,
            }
        except Exception as e:
            return {
                "available": False,
                "error": str(e),
            }
