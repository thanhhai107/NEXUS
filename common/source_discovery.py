from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_REPOSITORY_URL = "https://github.com/Akapi895/data-bigdata"
DEFAULT_SOURCE_DIR = PROJECT_ROOT / "assets" / "source_discovery"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "runtime" / "source_discovery"
ALL_SCHEMAS_FILE = "all_schemas.json"
ENDPOINT_REPORT_FILE = "endpoint_verification_report.json"
JSON_SCHEMA_DRAFT = "https://json-schema.org/draft/2020-12/schema"
PRIMITIVE_TYPES = {"string", "integer", "number", "boolean", "object", "array", "null"}


def load_discovery_catalog(source_dir: Path = DEFAULT_SOURCE_DIR) -> dict[str, Any]:
    """Load the consolidated source discovery catalog."""
    catalog_path = source_dir / ALL_SCHEMAS_FILE
    if not catalog_path.exists():
        raise FileNotFoundError(
            f"Source discovery catalog not found at {catalog_path}. "
            "Expected assets/source_discovery/all_schemas.json or pass --source-dir."
        )
    return _read_json(catalog_path)


def source_summary(source_dir: Path = DEFAULT_SOURCE_DIR) -> dict[str, Any]:
    """Return a compact summary of discovered data sources."""
    catalog = load_discovery_catalog(source_dir)
    sources = catalog.get("sources", [])
    schemas = catalog.get("schemas", {})
    return {
        "repository": SOURCE_REPOSITORY_URL,
        "source_dir": str(source_dir),
        "generated_at": catalog.get("generated_at"),
        "source_count": len(sources),
        "schema_count": len(schemas),
        "sources": sources,
    }


def schema_names(source_dir: Path = DEFAULT_SOURCE_DIR) -> list[str]:
    """List schema names available in the source discovery catalog."""
    schemas = load_discovery_catalog(source_dir).get("schemas", {})
    return sorted(str(name) for name in schemas)


def load_schema_definition(schema_name: str, source_dir: Path = DEFAULT_SOURCE_DIR) -> dict[str, Any]:
    """Load one discovered schema definition by name."""
    schemas = load_discovery_catalog(source_dir).get("schemas", {})
    try:
        definition = schemas[schema_name]
    except KeyError as exc:
        available = ", ".join(sorted(schemas)[:10])
        raise KeyError(f"Unknown source schema `{schema_name}`. Available examples: {available}") from exc
    if not isinstance(definition, dict):
        raise ValueError(f"Schema `{schema_name}` is not an object definition.")
    return definition


def to_nexus_json_schema(schema_name: str, definition: dict[str, Any]) -> dict[str, Any]:
    """Convert a discovered schema definition to a NEXUS JSON Schema."""
    schema_type = definition.get("type") if definition.get("type") in PRIMITIVE_TYPES else "object"
    output: dict[str, Any] = {
        "$schema": JSON_SCHEMA_DRAFT,
        "title": _title(schema_name),
        "type": schema_type,
        "x-source-discovery-schema": schema_name,
    }

    description = definition.get("description")
    if description:
        output["description"] = description

    required = definition.get("required_fields") or definition.get("required") or []
    if isinstance(required, list):
        output["required"] = [str(field) for field in required]

    properties = definition.get("properties") or {}
    if isinstance(properties, dict):
        output["properties"] = {
            str(field_name): _field_to_json_schema(field_definition)
            for field_name, field_definition in properties.items()
            if isinstance(field_definition, dict)
        }

    return output


def sync_discovery(
    source_dir: Path = DEFAULT_SOURCE_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    selected_schemas: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Write source discovery metadata into the NEXUS runtime area."""
    output_dir.mkdir(parents=True, exist_ok=True)
    schema_output_dir = output_dir / "schemas"
    schema_output_dir.mkdir(parents=True, exist_ok=True)

    summary = source_summary(source_dir)
    sources_path = output_dir / "sources.json"
    _write_json(sources_path, summary)

    endpoint_report = source_dir / ENDPOINT_REPORT_FILE
    endpoint_report_path = None
    if endpoint_report.exists():
        endpoint_report_path = output_dir / ENDPOINT_REPORT_FILE
        _write_json(endpoint_report_path, _read_json(endpoint_report))

    requested = list(selected_schemas) if selected_schemas else schema_names(source_dir)
    written_schema_paths: list[str] = []
    for name in requested:
        definition = load_schema_definition(name, source_dir)
        nexus_schema = to_nexus_json_schema(name, definition)
        schema_path = schema_output_dir / f"{schema_filename(name)}.schema.json"
        _write_json(schema_path, nexus_schema)
        written_schema_paths.append(str(schema_path))

    return {
        "repository": SOURCE_REPOSITORY_URL,
        "source_dir": str(source_dir),
        "output_dir": str(output_dir),
        "sources_path": str(sources_path),
        "endpoint_report_path": str(endpoint_report_path) if endpoint_report_path else None,
        "schema_dir": str(schema_output_dir),
        "schemas_written": len(written_schema_paths),
        "schema_paths": written_schema_paths,
    }


def integrate_schema_into_domain(
    schema_name: str,
    domain: str,
    dataset: str,
    source_dir: Path = DEFAULT_SOURCE_DIR,
    domains_dir: Path = PROJECT_ROOT / "domains",
) -> dict[str, Any]:
    """Materialize a discovered schema into domains/<domain> and register a dataset stub."""
    schema_definition = load_schema_definition(schema_name, source_dir)
    nexus_schema = to_nexus_json_schema(schema_name, schema_definition)

    domain_dir = domains_dir / domain
    schemas_dir = domain_dir / "schemas"
    schemas_dir.mkdir(parents=True, exist_ok=True)

    schema_rel_path = Path("domains") / domain / "schemas" / f"{dataset}.schema.json"
    schema_rel_path_text = schema_rel_path.as_posix()
    schema_path = domains_dir / domain / "schemas" / f"{dataset}.schema.json"
    _write_json(schema_path, nexus_schema)

    datasets_path = domain_dir / "datasets.yml"
    catalog = _read_yaml(datasets_path)
    datasets = catalog.setdefault("datasets", {})
    dataset_entry = datasets.get(dataset, {})
    if not isinstance(dataset_entry, dict):
        dataset_entry = {}

    dataset_entry.update(
        {
            "domain": domain,
            "description": dataset_entry.get("description") or f"Imported from source discovery schema {schema_name}.",
            "source_type": dataset_entry.get("source_type") or "api_stream",
            "source_uri": dataset_entry.get("source_uri") or "https://example.com/replace-me",
            "schema_path": schema_rel_path_text,
            "source_discovery": {
                "repository": SOURCE_REPOSITORY_URL,
                "source_file": f"assets/source_discovery/{ALL_SCHEMAS_FILE}",
                "schema_names": [schema_name],
            },
            "freshness_hours": dataset_entry.get("freshness_hours") or 24,
            "primary_keys": dataset_entry.get("primary_keys") or [],
        }
    )
    datasets[dataset] = dataset_entry
    _write_yaml(datasets_path, catalog)

    return {
        "domain": domain,
        "dataset": dataset,
        "schema_name": schema_name,
        "schema_path": str(schema_path),
        "datasets_path": str(datasets_path),
    }


def schema_filename(schema_name: str) -> str:
    """Return a filesystem-safe schema filename stem."""
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", schema_name).strip("._")
    return safe or "schema"


def _field_to_json_schema(field: dict[str, Any]) -> dict[str, Any]:
    field_type = field.get("type")
    ref = field.get("ref")
    output: dict[str, Any] = {}

    if field_type in PRIMITIVE_TYPES:
        output["type"] = field_type
        if field_type == "array":
            output["items"] = {}
    elif field_type and field_type != "any":
        output["type"] = "object"
        output["x-source-discovery-ref"] = str(field_type)
    elif ref:
        output["type"] = "object"
        output["x-source-discovery-ref"] = str(ref)

    if field.get("nullable") and "type" in output:
        current_type = output["type"]
        output["type"] = [current_type, "null"] if isinstance(current_type, str) else [*current_type, "null"]
    elif field.get("nullable"):
        output["x-nullable"] = True

    for key in ("format", "description"):
        value = field.get(key)
        if value:
            output[key] = value

    enum = field.get("enum")
    if isinstance(enum, list) and enum:
        output["enum"] = enum

    return output


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _read_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PyYAML is required for source-discovery integration. Install dependencies from requirements.txt."
        ) from exc

    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PyYAML is required for source-discovery integration. Install dependencies from requirements.txt."
        ) from exc

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(payload, file, sort_keys=False, allow_unicode=True)


def _title(schema_name: str) -> str:
    suffix = schema_name.split("_", 1)[-1]
    return suffix.replace("_", " ").replace(".", " ")
