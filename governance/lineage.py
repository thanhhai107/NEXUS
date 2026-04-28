from __future__ import annotations

from pathlib import Path
from typing import Iterable

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
    namespace: str = "nexus",
    lineage_log: Path = DEFAULT_LINEAGE_LOG,
) -> Path:
    """Append an OpenLineage-compatible source-to-target lineage event."""
    context = GovernanceContext.from_values(batch_id, run_id, source_path, actor)
    input_names = list(inputs)
    output_names = list(outputs)
    event = {
        "eventType": event_type,
        "eventTime": utc_now_iso(),
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
        "job_name": job_name,
        "input_names": input_names,
        "output_names": output_names,
        **context.to_event_fields(),
        "timestamp": utc_now_iso(),
    }
    append_governance_event("lineage", event, lineage_log)
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
