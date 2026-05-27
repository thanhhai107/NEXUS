from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

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


def build_openmetadata_export(
    dataset_names: Iterable[str] | None = None,
    *,
    domain: str | None = None,
) -> dict[str, Any]:
    """Build field-level metadata payloads suitable for OpenMetadata ingestion."""
    selected = _selected_dataset_names(dataset_names, domain)
    return {
        "tool": "OpenMetadata",
        "entity_type": "table_field_metadata",
        "datasets": [_openmetadata_dataset_payload(name) for name in selected],
    }

def build_business_glossary_export(
    dataset_names: Iterable[str] | None = None,
    *,
    domain: str | None = None,
) -> dict[str, Any]:
    """Build a compact Business Glossary export from semantic contracts."""
    terms: list[dict[str, Any]] = []
    for dataset_name in _selected_dataset_names(dataset_names, domain):
        contract = load_semantic_contract(dataset_name)
        rules = dict(contract.dataset_rules)
        glossary = dict(rules.get("glossary") or {})
        dataset_term = glossary.get("dataset_term") or dataset_name
        terms.append({
            "term": dataset_term,
            "type": "dataset",
            "dataset": dataset_name,
            "domain": contract.domain,
            "definition": dict(rules.get("definitions") or {}).get(dataset_term, ""),
            "grain": dict(rules.get("grain") or {}),
            "owner_system": "NEXUS",
        })
        for field_name, field_term in dict(glossary.get("field_terms") or {}).items():
            terms.append({
                "term": field_term,
                "type": "field",
                "dataset": dataset_name,
                "source_field": field_name,
                "domain": contract.domain,
                "definition": _field_definition(rules, field_name, field_term),
                "canonical_field": _canonical_field(rules, field_name),
                "owner_system": "NEXUS",
            })
    return {
        "tool": "Business Glossary",
        "entity_type": "glossary_terms",
        "terms": terms,
    }

def write_semantic_export(payload: Mapping[str, Any], output_path: str | Path) -> Path:
    path = Path(output_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path

def _selected_dataset_names(dataset_names: Iterable[str] | None, domain: str | None) -> list[str]:
    catalog = load_dataset_catalog().get("datasets", {})
    selected = list(dataset_names or sorted(catalog))
    if domain:
        selected = [name for name in selected if str(catalog.get(name, {}).get("domain")) == domain]
    unknown = sorted(set(selected) - set(catalog))
    if unknown:
        raise KeyError(f"Unknown dataset(s): {', '.join(unknown)}")
    return selected

def _openmetadata_dataset_payload(dataset_name: str) -> dict[str, Any]:
    contract = load_semantic_contract(dataset_name)
    catalog = load_dataset_catalog().get("datasets", {})
    dataset = catalog[dataset_name]
    rules = dict(contract.dataset_rules)
    schema = _read_schema(dataset.get("schema_path"))
    properties = dict(schema.get("properties") or {})
    glossary = dict(rules.get("glossary") or {})
    field_terms = dict(glossary.get("field_terms") or {})
    columns = [
        _openmetadata_column_payload(rules, field_name, field_schema, field_terms.get(field_name))
        for field_name, field_schema in sorted(properties.items())
    ]
    return {
        "name": dataset_name,
        "domain": contract.domain,
        "description": dataset.get("description", ""),
        "glossaryTerm": glossary.get("dataset_term"),
        "schemaPath": dataset.get("schema_path"),
        "dataGrain": dict(rules.get("grain") or {}),
        "entityMatching": dict(rules.get("entity_matching") or {}),
        "columns": columns,
    }

def _openmetadata_column_payload(
    rules: Mapping[str, Any],
    field_name: str,
    field_schema: Mapping[str, Any],
    glossary_term: str | None,
) -> dict[str, Any]:
    standards = dict(rules.get("standards") or {})
    unit_rules = dict(standards.get("units") or {})
    time_rules = dict(standards.get("time") or {})
    spatial_rules = dict(standards.get("spatial") or {})
    custom_properties: dict[str, Any] = {
        "canonicalField": _canonical_field(rules, field_name),
        "semanticIssue": _semantic_issue(rules, field_name),
        "businessDefinition": _field_definition(rules, field_name, glossary_term),
    }
    if field_name in {unit_rules.get("value_field"), unit_rules.get("unit_field")}:
        custom_properties["unit"] = unit_rules
    if field_name == time_rules.get("event_time_field"):
        custom_properties["timeRole"] = "event_time"
        custom_properties["timezone"] = time_rules.get("source_timezone")
        custom_properties["storageTimezone"] = time_rules.get("storage_timezone")
    if field_name == time_rules.get("ingestion_time_field"):
        custom_properties["timeRole"] = "ingestion_time"
    if field_name == time_rules.get("processing_time_field"):
        custom_properties["timeRole"] = "processing_time"
    if field_name in {spatial_rules.get("latitude_field"), spatial_rules.get("longitude_field")}:
        custom_properties["crs"] = {
            "source": spatial_rules.get("source_crs"),
            "storage": spatial_rules.get("storage_crs"),
            "render": spatial_rules.get("render_crs"),
        }
    return {
        "name": field_name,
        "dataType": field_schema.get("type"),
        "description": glossary_term or field_schema.get("description", ""),
        "glossaryTerm": glossary_term,
        "customProperties": {key: value for key, value in custom_properties.items() if value not in (None, "", {})},
    }

def _read_schema(schema_path: str | None) -> dict[str, Any]:
    if not schema_path:
        return {}
    path = Path(schema_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

def _canonical_field(rules: Mapping[str, Any], field_name: str) -> str | None:
    for mapping in list(rules.get("field_mappings") or []):
        if mapping.get("source_field") == field_name:
            return str(mapping.get("canonical_field"))
    return None

def _semantic_issue(rules: Mapping[str, Any], field_name: str) -> str | None:
    for mapping in list(rules.get("field_mappings") or []):
        if mapping.get("source_field") == field_name:
            return str(mapping.get("issue"))
    return None

def _field_definition(
    rules: Mapping[str, Any],
    field_name: str,
    glossary_term: str | None,
) -> str:
    definitions = dict(rules.get("definitions") or {})
    return str(
        definitions.get(field_name)
        or definitions.get(glossary_term or "")
        or glossary_term
        or ""
    )

__all__ = [
    "SemanticContract",
    "build_business_glossary_export",
    "build_openmetadata_export",
    "list_semantic_contracts",
    "load_semantic_contract",
    "load_unit_mapping_table",
    "normalize_unit_key",
    "write_semantic_export",
]
