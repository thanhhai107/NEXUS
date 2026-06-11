"""Lineage Module.

Provides OpenLineage-compatible lineage tracking for NEXUS data pipelines.
Records transformations and data flow between datasets.

Usage:
    # Basic lineage
    record_lineage("bronze_to_silver", inputs=["bronze.tpcdi_trade"], outputs=["silver.tpcdi_trade"])

    # With transform metadata
    record_lineage(
        "silver_validate",
        inputs=["bronze.tpcdi_trade"],
        outputs=["silver.tpcdi_trade_clean"],
        input_version="v1.0.0",
        output_version="v1.0.1",
        operator="silver_validate",
        sql_version="v1.2.0",
        code_version="abc123",
        column_mapping={"old_col": "new_col"},
        transform_rules=["dedup", "normalize"],
    )
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable, Mapping

from common.config import LOGS_DIR
from governance.context import GovernanceContext, utc_now_iso
from governance.storage import append_governance_event


DEFAULT_LINEAGE_LOG = LOGS_DIR / "lineage.jsonl"
OPENLINEAGE_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
OPENLINEAGE_PRODUCER = os.getenv("OPENLINEAGE_PRODUCER_URL", "https://github.com/nexus-lakehouse/nexus")


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
    # Transform metadata facets
    input_version: str | None = None,
    output_version: str | None = None,
    operator: str | None = None,
    sql_version: str | None = None,
    code_version: str | None = None,
    column_mapping: dict[str, str] | None = None,
    transform_rules: list[str] | None = None,
) -> Path:
    """Append lineage locally and optionally emit it to an OpenLineage endpoint.
    
    Args:
        job_name: Name of the transformation job
        inputs: List of input dataset names
        outputs: List of output dataset names
        batch_id: Batch identifier
        run_id: Run identifier
        source_path: Source file path
        actor: Actor who executed the job
        event_type: OpenLineage event type (START, COMPLETE, FAIL)
        namespace: OpenLineage namespace
        lineage_log: Path to lineage log file
        input_version: Version of input dataset
        output_version: Version of output dataset
        operator: Name of the operator/transformation applied
        sql_version: Version of SQL query used
        code_version: Git SHA or version of code used
        column_mapping: Mapping of input columns to output columns
        transform_rules: List of transformation rules applied
        
    Returns:
        Path to lineage log file
    """
    namespace = namespace or os.getenv("OPENLINEAGE_NAMESPACE", "nexus")
    context = GovernanceContext.from_values(batch_id, run_id, source_path, actor)
    input_names = list(inputs)
    output_names = list(outputs)
    event_time = utc_now_iso()

    # Build transform facets if any metadata provided
    transform_facets = _build_transform_facets(
        input_version=input_version,
        output_version=output_version,
        operator=operator,
        sql_version=sql_version,
        code_version=code_version,
        column_mapping=column_mapping,
        transform_rules=transform_rules,
    )

    # Build run facets
    run_facets = {
        "nexus_batch": {
            "_producer": OPENLINEAGE_PRODUCER,
            "_schemaURL": OPENLINEAGE_SCHEMA_URL,
            "batch_id": context.batch_id,
            "actor": context.actor,
            "source_path": context.source_path,
        }
    }

    # Add transform facets if available
    if transform_facets:
        run_facets["transform"] = transform_facets

    openlineage_event = {
        "eventType": event_type,
        "eventTime": event_time,
        "run": {
            "runId": context.run_id,
            "facets": run_facets,
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
        "input_version": input_version,
        "output_version": output_version,
        "operator": operator,
        "sql_version": sql_version,
        "code_version": code_version,
        "column_mapping": column_mapping,
        "transform_rules": transform_rules,
        **context.to_event_fields(),
        "timestamp": event_time,
    }
    append_governance_event("lineage", event, lineage_log)
    _emit_openlineage_event(openlineage_event)
    _emit_openmetadata_event(openlineage_event)
    return lineage_log


def _build_transform_facets(
    input_version: str | None,
    output_version: str | None,
    operator: str | None,
    sql_version: str | None,
    code_version: str | None,
    column_mapping: dict[str, str] | None,
    transform_rules: list[str] | None,
) -> dict[str, Any] | None:
    """Build transform facets for OpenLineage event.
    
    Returns None if no transform metadata is provided.
    """
    facets = {}

    if input_version is not None:
        facets["input_version"] = input_version
    if output_version is not None:
        facets["output_version"] = output_version
    if operator is not None:
        facets["operator"] = operator
    if sql_version is not None:
        facets["sql_version"] = sql_version
    if code_version is not None:
        facets["code_version"] = code_version
    if column_mapping is not None:
        facets["column_mapping"] = column_mapping
    if transform_rules is not None:
        facets["transform_rules"] = transform_rules

    return facets if facets else None


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


def _emit_openmetadata_event(payload: Mapping[str, Any]) -> bool:
    """Emit lineage event to OpenMetadata collector.

    Uses OpenLineage-compatible format. OpenMetadata accepts
    OpenLineage events via its Lineage API endpoint.
    """
    endpoint = _openmetadata_http_endpoint()
    if endpoint is None:
        return False

    try:
        import requests

        response = requests.post(
            endpoint,
            json=dict(payload),
            headers={"Content-Type": "application/json"},
            timeout=_openmetadata_timeout_seconds(),
        )
        response.raise_for_status()
    except Exception as exc:
        if os.getenv("OPENMETADATA_STRICT", "").lower() in {"1", "true", "yes", "on"}:
            raise RuntimeError(f"Failed to emit lineage event to OpenMetadata: {endpoint}") from exc
        return False

    return True


def _openmetadata_http_endpoint() -> str | None:
    """Get OpenMetadata lineage endpoint URL."""
    base_url = os.getenv("OPENMETADATA_URL", "").strip()
    if not base_url:
        return None

    base_url = base_url.rstrip("/")
    return f"{base_url}/api/v1/lineage"


def _openmetadata_timeout_seconds() -> float:
    raw_value = os.getenv("OPENMETADATA_TIMEOUT_SECONDS", "10")
    try:
        return float(raw_value)
    except ValueError:
        return 10.0
