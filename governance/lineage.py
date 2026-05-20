from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable, Mapping

from common.config import LOGS_DIR
from governance.context import GovernanceContext, utc_now_iso
from governance.storage import append_governance_event

DEFAULT_LINEAGE_LOG = LOGS_DIR / "lineage.jsonl"
OPENLINEAGE_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
OPENLINEAGE_PRODUCER = "https://github.com/nexus-lakehouse/nexus"


def record_lineage(
    job_name: str,
    inputs: Iterable[str],
    outputs: Iterable[str],
    batch_id: str | None = None,
    run_id: str | None = None,
    source_path: str | Path | None = None,
    actor: str | None = None,
    event_type: str = "COMPLETE",
    namespace: str | None = None,
    lineage_log: Path = DEFAULT_LINEAGE_LOG,
) -> Path:
    """Append lineage locally and optionally emit it to an OpenLineage endpoint."""
    namespace = namespace or os.getenv("OPENLINEAGE_NAMESPACE", "nexus")
    context = GovernanceContext.from_values(batch_id, run_id, source_path, actor)
    input_names = list(inputs)
    output_names = list(outputs)
    event_time = utc_now_iso()
    openlineage_event = {
        "eventType": event_type,
        "eventTime": event_time,
        "run": {
            "runId": context.run_id,
            "facets": {
                "nexus_batch": {
                    "_producer": OPENLINEAGE_PRODUCER,
                    "_schemaURL": OPENLINEAGE_SCHEMA_URL,
                    "batch_id": context.batch_id,
                    "actor": context.actor,
                    "source_path": context.source_path,
                }
            },
        },
        "job": {
            "namespace": namespace,
            "name": job_name,
        },
        "inputs": [_openlineage_dataset(name, namespace) for name in input_names],
        "outputs": [_openlineage_dataset(name, namespace) for name in output_names],
        "producer": OPENLINEAGE_PRODUCER,
        "schemaURL": OPENLINEAGE_SCHEMA_URL,
    }
    event = {
        **openlineage_event,
        "job_name": job_name,
        "input_names": input_names,
        "output_names": output_names,
        **context.to_event_fields(),
        "timestamp": event_time,
    }
    append_governance_event("lineage", event, lineage_log)
    _emit_openlineage_event(openlineage_event)
    return lineage_log


def _openlineage_dataset(name: str, namespace: str) -> dict[str, object]:
    return {
        "namespace": namespace,
        "name": name,
        "facets": {
            "dataSource": {
                "_producer": OPENLINEAGE_PRODUCER,
                "_schemaURL": OPENLINEAGE_SCHEMA_URL,
                "name": name,
                "uri": name,
            }
        },
    }


def _emit_openlineage_event(payload: Mapping[str, Any]) -> bool:
    endpoint = _openlineage_http_endpoint()
    if endpoint is None:
        return False

    try:
        import requests

        response = requests.post(
            endpoint,
            json=dict(payload),
            timeout=_openlineage_timeout_seconds(),
        )
        response.raise_for_status()
    except Exception as exc:
        if _openlineage_strict():
            raise RuntimeError(f"Failed to emit OpenLineage event to {endpoint}") from exc
        return False

    return True


def _openlineage_http_endpoint() -> str | None:
    base_url = os.getenv("OPENLINEAGE_URL", "").strip()
    if not base_url:
        return None

    base_url = base_url.rstrip("/")
    endpoint = os.getenv("OPENLINEAGE_ENDPOINT", "/api/v1/lineage").strip() or "/api/v1/lineage"
    if endpoint.startswith(("http://", "https://")):
        return endpoint

    if base_url.endswith("/api/v1/lineage"):
        return base_url

    if not endpoint.startswith("/"):
        endpoint = f"/{endpoint}"
    return f"{base_url}{endpoint}"


def _openlineage_timeout_seconds() -> float:
    raw_value = os.getenv("OPENLINEAGE_TIMEOUT_SECONDS", "5")
    try:
        return float(raw_value)
    except ValueError:
        return 5.0


def _openlineage_strict() -> bool:
    return os.getenv("OPENLINEAGE_STRICT", "false").strip().lower() in {"1", "true", "yes", "on"}
