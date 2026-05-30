from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from ingestion.base.utils import iter_timestamp_strings

RUNTIME_VERSION = "raw-envelope-v1"


@dataclass(frozen=True)
class EnvelopeContext:
    dataset_id: str
    source_id: str
    ingestion_type: str
    source_key: str | None = None
    source_type: str | None = None
    run_id: str | None = None
    chunk_id: str | None = None
    source_path: str | Path | None = None
    schema_version: str = "0.0.0"  # Required - defaults to unknown for backward compat
    published_at: str | None = None
    trace_id: str | None = None


def clean_field_name(name: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower())
    return normalized.strip("_")


def normalize_record(record: Mapping[str, object]) -> dict[str, object]:
    return {clean_field_name(str(key)): value for key, value in record.items()}


def build_raw_envelope(
    record: Mapping[str, object],
    context: EnvelopeContext,
    *,
    record_index: int = 0,
    normalize_payload: bool = False,
    entity_key: str | None = None,
) -> dict[str, Any]:
    payload = normalize_record(record) if normalize_payload else dict(record)
    trace_id = context.trace_id or str(uuid.uuid4())
    event_time = next(iter_timestamp_strings(payload), None)
    source_path = str(context.source_path) if context.source_path is not None else None
    record_id = _record_id(context, record_index, payload)
    ingested_at = datetime.now(timezone.utc).isoformat()

    return {
        "_nexus_ingestion_type": context.ingestion_type,
        "_nexus_source_id": context.source_id,
        "_nexus_source_key": context.source_key,
        "_nexus_source_type": context.source_type,
        "_nexus_dataset_id": context.dataset_id,
        "_nexus_run_id": context.run_id,
        "_nexus_chunk_id": context.chunk_id,
        "_nexus_record_id": record_id,
        "_nexus_entity_key": entity_key,
        "_nexus_event_time": event_time,
        "_nexus_ingested_at": ingested_at,
        "_nexus_published_at": context.published_at,
        "_nexus_schema_version": context.schema_version,
        "_nexus_trace_id": trace_id,
        "_nexus_runtime_version": RUNTIME_VERSION,
        "_nexus_source_path": source_path,
        # Backward-compatible aliases used by existing Bronze and API flows.
        "_nexus_source": context.source_id,
        "_nexus_dataset": context.dataset_id,
        "payload": payload,
    }


def _record_id(context: EnvelopeContext, record_index: int, payload: Mapping[str, object]) -> str:
    digest = hashlib.sha256()
    parts = [
        context.ingestion_type,
        context.source_id,
        context.dataset_id,
        context.run_id or "",
        context.chunk_id or "",
        str(context.source_path or ""),
        str(record_index),
        json.dumps(dict(payload), sort_keys=True, default=str),
    ]
    for part in parts:
        digest.update(part.encode("utf-8"))
    return digest.hexdigest()
