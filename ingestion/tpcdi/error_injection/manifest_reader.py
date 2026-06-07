"""
Manifest reader — load injection_manifest.json and iterate mutations.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_manifest(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    if path.is_dir():
        path = path / "injection_manifest.json"
    if not path.exists():
        raise FileNotFoundError(f"injection_manifest.json not found at {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def iter_mutations(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """Return list of all mutations from manifest."""
    return manifest.get("mutations", [])


def get_mutation_by_id(manifest: dict[str, Any], mutation_id: str) -> dict[str, Any] | None:
    for m in manifest.get("mutations", []):
        if m.get("mutation_id") == mutation_id:
            return m
    return None


def get_mutations_by_type(manifest: dict[str, Any], mutation_type: str) -> list[dict[str, Any]]:
    return [m for m in manifest.get("mutations", []) if m.get("mutation_type") == mutation_type]
