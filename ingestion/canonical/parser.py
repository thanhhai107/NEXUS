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
    if suffix in {".xml", ".xsd"}:
        yield from _iter_xml(artifact_path)
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


def _iter_xml(path: Path) -> Iterable[dict[str, Any]]:
    """Parse XML files into dictionaries.

    Handles common XML structures:
    - Single root with repeated child elements
    - Nested records with attributes and text
    - Converts XML attributes to prefixed keys (e.g., @id)
    - Converts text content to #text key
    """
    import xml.etree.ElementTree as ET

    tree = ET.parse(path)
    root = tree.getroot()

    def _element_to_dict(element: ET.Element) -> dict[str, Any]:
        result: dict[str, Any] = {}

        # Handle attributes
        if element.attrib:
            for key, value in element.attrib.items():
                result[f"@{key}"] = value

        # Handle child elements
        for child in element:
            child_dict = _element_to_dict(child)
            child_key = child.tag

            # Handle repeated child elements (create list)
            if child_key in result:
                if not isinstance(result[child_key], list):
                    result[child_key] = [result[child_key]]
                result[child_key].append(child_dict)
            else:
                result[child_key] = child_dict

        # Handle text content
        if element.text and element.text.strip():
            text_key = "#text"
            if result:  # Has attributes or children
                result[text_key] = element.text.strip()
            else:
                return element.text.strip()

        return result

    # Try to find repeated record elements
    # Common patterns: <records><record>...</record></records> or <items><item>...</item></items>
    record_tags = {"record", "item", "entry", "row", "data", "event", "object"}
    children = list(root)

    # If root has many children, treat each as a record
    if len(children) > 1:
        for child in children:
            yield _element_to_dict(child)
    # If root has a single child with many grandchildren, treat grandchildren as records
    elif len(children) == 1:
        grandchildren = list(children[0])
        if len(grandchildren) > 1 and children[0].tag.lower() in record_tags:
            for grandchild in grandchildren:
                yield _element_to_dict(grandchild)
        else:
            # Single record
            yield _element_to_dict(root)
    else:
        # Single element with no children
        yield _element_to_dict(root)
