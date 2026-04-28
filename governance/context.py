from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_actor() -> str:
    return (
        os.getenv("NEXUS_ACTOR")
        or os.getenv("AIRFLOW_CTX_DAG_OWNER")
        or os.getenv("USERNAME")
        or os.getenv("USER")
        or "system"
    )


def default_run_id(batch_id: str | None = None) -> str:
    return (
        os.getenv("NEXUS_RUN_ID")
        or os.getenv("AIRFLOW_CTX_DAG_RUN_ID")
        or os.getenv("AIRFLOW_CTX_RUN_ID")
        or batch_id
        or "manual"
    )


def normalize_source_path(source_path: str | Path | None) -> str | None:
    if source_path is None:
        return None
    return str(source_path)


@dataclass(frozen=True)
class GovernanceContext:
    batch_id: str
    run_id: str
    source_path: str | None
    actor: str

    @classmethod
    def from_values(
        cls,
        batch_id: str | None = None,
        run_id: str | None = None,
        source_path: str | Path | None = None,
        actor: str | None = None,
    ) -> "GovernanceContext":
        resolved_batch_id = batch_id or "manual"
        return cls(
            batch_id=resolved_batch_id,
            run_id=run_id or default_run_id(resolved_batch_id),
            source_path=normalize_source_path(source_path),
            actor=actor or default_actor(),
        )

    def to_event_fields(self) -> dict[str, str | None]:
        return {
            "batch_id": self.batch_id,
            "run_id": self.run_id,
            "source_path": self.source_path,
            "actor": self.actor,
        }
