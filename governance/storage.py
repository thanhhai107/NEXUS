from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Mapping

DEFAULT_EVENTS_TABLE = "nexus_governance_events"
VALID_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def storage_mode() -> str:
    return os.getenv("NEXUS_GOVERNANCE_STORAGE", "local").strip().lower() or "local"


def using_postgres_storage() -> bool:
    return storage_mode() == "postgres"


def _is_s3_storage() -> bool:
    mode = storage_mode()
    if mode == "s3":
        return True
    if mode == "local":
        from common.config import is_vm_mode
        return is_vm_mode()
    return False


def _s3_key(stream: str, local_path: Path | None = None) -> str:
    if local_path is not None:
        from common.config import RUNTIME_DIR
        try:
            relative = local_path.relative_to(RUNTIME_DIR)
            return f"governance/{relative.as_posix()}"
        except ValueError:
            pass
    return f"governance/{stream}.jsonl"


def _read_jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []

    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


def append_governance_event(
    stream: str,
    payload: Mapping[str, Any],
    local_path: Path | None = None,
) -> Path | None:
    if using_postgres_storage():
        _append_postgres_event(stream, dict(payload))
        return None

    if _is_s3_storage():
        from common.storage import get_storage
        storage = get_storage()
        s3_key = _s3_key(stream, local_path)
        storage.append_jsonl(s3_key, dict(payload))
        return local_path or Path(s3_key)

    if local_path is None:
        return None

    local_path.parent.mkdir(parents=True, exist_ok=True)
    with local_path.open("a", encoding="utf-8", newline="\n") as file:
        file.write(json.dumps(dict(payload), ensure_ascii=False) + "\n")
    return local_path


def read_governance_events(stream: str, local_path: Path | None = None) -> list[dict[str, Any]]:
    if using_postgres_storage():
        return _read_postgres_events(stream)

    if _is_s3_storage():
        from common.storage import get_storage
        storage = get_storage()
        s3_key = _s3_key(stream, local_path)
        if not storage.exists(s3_key):
            return []
        return list(storage.read_jsonl(s3_key))

    return _read_jsonl(local_path)


def _postgres_url() -> str:
    url = os.getenv("NEXUS_GOVERNANCE_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "NEXUS_GOVERNANCE_STORAGE=postgres requires NEXUS_GOVERNANCE_DATABASE_URL."
        )
    return url


def _events_table() -> str:
    table = os.getenv("NEXUS_GOVERNANCE_EVENTS_TABLE", DEFAULT_EVENTS_TABLE)
    if not VALID_TABLE_NAME.match(table):
        raise RuntimeError(f"Invalid governance events table name: {table}")
    return table


def _connect():
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("Postgres governance storage requires psycopg.") from exc
    return psycopg.connect(_postgres_url())


def _event_time(payload: Mapping[str, Any]) -> str | None:
    for key in ("timestamp", "created_at", "captured_at", "eventTime", "quarantined_at"):
        value = payload.get(key)
        if value:
            return str(value)
    return None


def _append_postgres_event(stream: str, payload: dict[str, Any]) -> None:
    from psycopg.types.json import Jsonb

    table = _events_table()
    dataset = payload.get("dataset") or payload.get("dataset_name")
    batch_id = payload.get("batch_id")
    event_type = payload.get("event_type") or payload.get("eventType")
    event_time = _event_time(payload)

    with _connect() as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id BIGSERIAL PRIMARY KEY,
                stream TEXT NOT NULL,
                dataset TEXT,
                batch_id TEXT,
                event_type TEXT,
                event_time TIMESTAMPTZ,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            f"""
            INSERT INTO {table} (stream, dataset, batch_id, event_type, event_time, payload)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (stream, dataset, batch_id, event_type, event_time, Jsonb(payload)),
        )


def _read_postgres_events(stream: str) -> list[dict[str, Any]]:
    table = _events_table()
    with _connect() as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id BIGSERIAL PRIMARY KEY,
                stream TEXT NOT NULL,
                dataset TEXT,
                batch_id TEXT,
                event_type TEXT,
                event_time TIMESTAMPTZ,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        rows = conn.execute(
            f"SELECT payload FROM {table} WHERE stream = %s ORDER BY id",
            (stream,),
        ).fetchall()

    output: list[dict[str, Any]] = []
    for (payload,) in rows:
        if isinstance(payload, str):
            output.append(json.loads(payload))
        else:
            output.append(dict(payload))
    return output
