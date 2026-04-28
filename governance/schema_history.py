from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from common.config import RUNTIME_DIR
from governance.context import GovernanceContext
from governance.storage import append_governance_event

DEFAULT_SCHEMA_HISTORY_DIR = RUNTIME_DIR / "schemas" / "history"


def fingerprint_schema(schema: Mapping[str, object]) -> str:
    encoded = json.dumps(schema, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def save_schema_snapshot(
    dataset: str,
    schema: Mapping[str, object],
    batch_id: str | None = None,
    run_id: str | None = None,
    source_path: str | Path | None = None,
    actor: str | None = None,
    history_dir: Path = DEFAULT_SCHEMA_HISTORY_DIR,
) -> Path:
    """Persist schema versions so breaking changes can be reviewed."""
    context = GovernanceContext.from_values(batch_id, run_id, source_path, actor)
    history_dir.mkdir(parents=True, exist_ok=True)
    fingerprint = fingerprint_schema(schema)
    output_path = history_dir / f"{dataset}_{fingerprint[:12]}.json"
    payload = {
        "dataset": dataset,
        "fingerprint": fingerprint,
        **context.to_event_fields(),
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "schema": schema,
    }
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)
    append_governance_event("schema_history", payload)
    return output_path
