from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from common.config import PROJECT_ROOT, load_dataset_catalog, load_semantic_config

_UNIT_TRANSLATION = str.maketrans({
    "\u00b0": "",
    "\u00b2": "2",
    "\u00b3": "3",
    "\u00b5": "u",
    "\u03bc": "u",
})


@dataclass(frozen=True)
class SemanticContract:
    dataset: str
    domain: str
    standards: Mapping[str, Any]
    issue_catalog: Mapping[str, Any]
    dataset_rules: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "domain": self.domain,
            "standards": dict(self.standards),
            "issue_catalog": dict(self.issue_catalog),
            "dataset_rules": dict(self.dataset_rules),
        }


def normalize_unit_key(value: object) -> str:
    """Normalize common source-unit spellings before unit mapping lookup."""
    text = str(value or "").strip().lower().translate(_UNIT_TRANSLATION)
    text = text.replace("micrograms", "ug").replace("microgram", "ug")
    text = text.replace(" per ", "/").replace("per", "/")
    text = text.replace("cubicmetre", "m3").replace("cubicmeter", "m3")
    text = text.replace("^3", "3").replace(" ", "")
    replacements = {
        "ugm3": "ug/m3",
        "ug/m3": "ug/m3",
        "ug/m^3": "ug/m3",
        "kmh": "km/h",
        "kph": "km/h",
        "mps": "m/s",
        "metre": "m",
        "meter": "m",
        "meters": "m",
        "metres": "m",
        "kilometre": "km",
        "kilometer": "km",
        "kilometers": "km",
        "kilometres": "km",
        "miles": "mi",
        "mile": "mi",
    }
    return replacements.get(text, text)


def load_unit_mapping_table(path: str | Path | None = None) -> list[dict[str, Any]]:
    semantic_config = load_semantic_config()
    default_path = (
        semantic_config.get("default_semantic", {})
        .get("standards", {})
        .get("unit", {})
        .get("mapping_seed", "transform/dbt/seeds/unit_mapping.csv")
    )
    candidate = Path(path or default_path)
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    if not candidate.exists():
        return []

    rows: list[dict[str, Any]] = []
    with candidate.open("r", encoding="utf-8-sig", newline="") as file:
        for row in csv.DictReader(file):
            normalized = row.get("source_unit_normalized") or row.get("source_unit")
            rows.append({
                **row,
                "dimension_type": str(row.get("dimension_type") or "").strip().lower(),
                "source_unit_normalized": normalize_unit_key(normalized),
            })
    return rows


def load_semantic_contract(dataset_name: str) -> SemanticContract:
    catalog = load_dataset_catalog().get("datasets", {})
    if dataset_name not in catalog:
        raise KeyError(f"Unknown dataset: {dataset_name}")

    semantic_config = load_semantic_config()
    defaults = semantic_config.get("default_semantic", {})
    dataset_rules = semantic_config.get("datasets", {}).get(dataset_name, {}) or {}
    return SemanticContract(
        dataset=dataset_name,
        domain=str(catalog[dataset_name].get("domain", "unknown")),
        standards=dict(defaults.get("standards") or {}),
        issue_catalog=dict(defaults.get("issue_catalog") or {}),
        dataset_rules=dataset_rules,
    )


def list_semantic_contracts() -> list[SemanticContract]:
    catalog = load_dataset_catalog().get("datasets", {})
    return [load_semantic_contract(name) for name in sorted(catalog)]


__all__ = [
    "SemanticContract",
    "list_semantic_contracts",
    "load_semantic_contract",
    "load_unit_mapping_table",
    "normalize_unit_key",
]
