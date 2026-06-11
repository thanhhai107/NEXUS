from __future__ import annotations

import os
from typing import Any


def chat(
    model: str,
    messages: list[dict[str, Any]],
    system: str | None = None,
    temperature: float = 0,
    max_tokens: int = 2048,
) -> str:
    from openai import OpenAI

    client = OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        base_url=os.getenv("OPENAI_BASE_URL"),
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
    try:
        from openai import OpenAI

        client = OpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            base_url=os.getenv("OPENAI_BASE_URL"),
        )
        resp = client.models.list()
        model_ids = [m.id for m in resp]
        configured_model = os.getenv("NEXUS_AGENT_MODEL") or ""
        return {
            "available": True,
            "models": model_ids[:20],
            "model_available": configured_model in model_ids,
        }
    except Exception as e:
        return {"available": False, "error": str(e)}
