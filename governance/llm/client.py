from __future__ import annotations

import os
from typing import Any

import boto3


def chat(
    model: str,
    messages: list[dict[str, Any]],
    system: str | None = None,
    temperature: float = 0,
    max_tokens: int = 2048,
) -> str:
    provider = os.getenv("NEXUS_LLM_PROVIDER", "bedrock").lower()
    if provider == "openai":
        return _openai_chat(model, messages, system, temperature, max_tokens)
    return _bedrock_chat(model, messages, system, temperature, max_tokens)


def _bedrock_chat(
    model: str,
    messages: list[dict[str, Any]],
    system: str | None = None,
    temperature: float = 0,
    max_tokens: int = 2048,
) -> str:
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    client = boto3.client("bedrock-runtime", region_name=region)
    kwargs: dict[str, Any] = dict(
        modelId=model,
        messages=messages,
        inferenceConfig={"temperature": temperature, "maxTokens": max_tokens},
    )
    if system:
        kwargs["system"] = [{"text": system}]
    response = client.converse(**kwargs)
    return response["output"]["message"]["content"][0]["text"]


def _openai_chat(
    model: str,
    messages: list[dict[str, Any]],
    system: str | None = None,
    temperature: float = 0,
    max_tokens: int = 2048,
) -> str:
    from openai import OpenAI

    client = OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
    )
    stream = client.responses.create(
        model=model,
        input=messages,
        instructions=system,
        temperature=temperature,
        max_output_tokens=max_tokens,
        stream=True,
    )
    parts: list[str] = []
    for event in stream:
        if type(event).__name__ == "ResponseTextDeltaEvent":
            parts.append(event.delta)
    return "".join(parts)


def check_health() -> dict[str, Any]:
    provider = os.getenv("NEXUS_LLM_PROVIDER", "bedrock").lower()
    if provider == "openai":
        return _openai_health()
    return _bedrock_health()


def _bedrock_health() -> dict[str, Any]:
    try:
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        model = os.getenv("NEXUS_AGENT_MODEL", "amazon.nova-pro-v1:0")
        bedrock = boto3.client("bedrock", region_name=region)
        response = bedrock.list_foundation_models(
            byProvider="Amazon", byOutputModality="TEXT"
        )
        model_ids = [m["modelId"] for m in response.get("modelSummaries", [])]
        return {
            "available": True,
            "models": model_ids[:20],
            "model_available": any(model in mid for mid in model_ids),
        }
    except Exception as e:
        return {"available": False, "error": str(e)}


def _openai_health() -> dict[str, Any]:
    try:
        from openai import OpenAI

        client = OpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        )
        resp = client.models.list()
        model_ids = [m.id for m in resp]
        return {
            "available": True,
            "models": model_ids[:20],
            "model_available": os.getenv("NEXUS_AGENT_MODEL", "") in model_ids,
        }
    except Exception as e:
        return {"available": False, "error": str(e)}
