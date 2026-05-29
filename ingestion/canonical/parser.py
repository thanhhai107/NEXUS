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
        yield from _iter_csv(artifact_path)
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


def _iter_csv(path: Path) -> Iterable[dict[str, Any]]:
    """Parse CSV files, handling UK Air format with metadata header lines."""
    import re
    
    last_error: UnicodeDecodeError | None = None
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            with path.open("r", encoding=encoding, newline="") as file:
                lines = file.readlines()
            
            # Find the header line (starts with "Date" ignoring case)
            header_idx = 0
            for i, line in enumerate(lines):
                if line.strip().lower().startswith("date"):
                    header_idx = i
                    break
            
            # Parse CSV starting from header
            import io
            csv_content = "".join(lines[header_idx:])
            reader = csv.DictReader(io.StringIO(csv_content))
            
            for row in reader:
                # Normalize field names
                normalized = {
                    _normalize_field_name(k): v 
                    for k, v in row.items() 
                    if k is not None
                }
                if normalized:
                    yield normalized
            return
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error:
        raise last_error


def _normalize_field_name(name: str | None) -> str:
    """Normalize CSV field name to safe identifier."""
    import re
    
    if name is None:
        return "unnamed_column"
    
    # Strip whitespace
    name = name.strip()
    if not name:
        return "unnamed_column"
    
    # UK Air uses hour columns like " 01:00", " 02:00" - convert to "hour_01_00"
    if re.match(r"^\s*\d{2}:\d{2}\s*$", name):
        hour = name.strip().replace(":", "_")
        return f"hour_{hour}"
    
    # Replace spaces and special chars with underscores
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", name)
    return normalized.strip("_").lower() or "unnamed_column"
