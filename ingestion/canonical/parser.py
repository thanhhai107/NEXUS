from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable

from common.config import PROJECT_ROOT
from ingestion.base.utils import extract_records


def resolve_artifact_path(path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate


def iter_artifact_records(path: str | Path) -> Iterable[dict[str, Any]]:
    artifact_path = resolve_artifact_path(path)
    suffix = artifact_path.suffix.lower()
    if suffix == ".jsonl":
        yield from _iter_jsonl(artifact_path)
        return
    if suffix in {".json", ".geojson"}:
        payload = _read_json_payload(artifact_path)
        yield from extract_records(payload)
        return
    if suffix == ".csv":
        with artifact_path.open("r", encoding="utf-8-sig", newline="") as file:
            yield from csv.DictReader(file)
        return
    raise ValueError(f"Unsupported ingestion artifact type: {artifact_path.suffix}")


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                yield payload


def _read_json_payload(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)
