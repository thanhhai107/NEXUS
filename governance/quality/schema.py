from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from governance.quality.auto_fix import clean_column_name

Record = Mapping[str, object]


@dataclass
class SchemaCoercionResult:
    records: list[dict[str, object]]
    summary: dict[str, Any] = field(default_factory=dict)


def normalize_json_schema(
    schema: Mapping[str, Any] | None,
    auto_fix: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not schema:
        return None

    normalized = copy.deepcopy(dict(schema))
    if not auto_fix or not auto_fix.get("normalize_column_names"):
        return normalized

    required = normalized.get("required")
    if isinstance(required, list):
        normalized["required"] = [clean_column_name(str(field)) for field in required]

    properties = normalized.get("properties")
    if isinstance(properties, dict):
        normalized["properties"] = {
            clean_column_name(str(field)): definition
            for field, definition in properties.items()
        }

    return normalized


def coerce_records_to_schema(
    records: Sequence[Record],
    schema: Mapping[str, Any] | None,
) -> SchemaCoercionResult:
    properties = (schema or {}).get("properties") or {}
    summary: dict[str, Any] = {
        "enabled": bool(schema),
        "record_count": len(records),
        "coerced_value_count": 0,
        "failed_coercion_count": 0,
    }
    if not schema:
        return SchemaCoercionResult(records=[dict(record) for record in records], summary=summary)

    output: list[dict[str, object]] = []
    for record in records:
        fixed = dict(record)
        for field, definition in properties.items():
            if field not in fixed:
                continue
            coerced, changed, failed = _coerce_value(fixed[field], definition)
            if changed:
                summary["coerced_value_count"] += 1
                fixed[field] = coerced
            if failed:
                summary["failed_coercion_count"] += 1
        output.append(fixed)
    return SchemaCoercionResult(records=output, summary=summary)


def validate_json_schema(
    records: Sequence[Record],
    schema: Mapping[str, Any] | None,
    max_errors: int = 20,
) -> tuple[bool, list[str]]:
    if not schema:
        return True, []

    try:
        from jsonschema import Draft202012Validator, FormatChecker
    except ImportError as exc:
        return False, [f"jsonschema dependency is required for schema enforcement: {exc}"]

    validator = Draft202012Validator(dict(schema), format_checker=FormatChecker())
    issues: list[str] = []

    for index, record in enumerate(records):
        errors = sorted(validator.iter_errors(dict(record)), key=lambda error: list(error.path))
        for error in errors:
            path = ".".join(str(item) for item in error.path) or "$"
            issues.append(f"JSON Schema violation at record {index}, field {path}: {error.message}")
            if len(issues) >= max_errors:
                issues.append("JSON Schema validation stopped after 20 errors.")
                return False, issues

    return not issues, issues


def records_failing_json_schema(
    records: Sequence[Record],
    schema: Mapping[str, Any] | None,
) -> list[dict[str, object]]:
    if not schema:
        return []

    try:
        from jsonschema import Draft202012Validator, FormatChecker
    except ImportError:
        return [dict(record) for record in records]

    validator = Draft202012Validator(dict(schema), format_checker=FormatChecker())
    return [
        dict(record)
        for record in records
        if list(validator.iter_errors(dict(record)))
    ]


def _coerce_value(value: object, definition: Mapping[str, Any]) -> tuple[object, bool, bool]:
    if value is None or value == "":
        return value, False, False

    expected_types = definition.get("type")
    if isinstance(expected_types, str):
        expected = {expected_types}
    elif isinstance(expected_types, list):
        expected = {str(item) for item in expected_types}
    else:
        return value, False, False

    if "integer" in expected:
        return _coerce_integer(value)
    if "number" in expected:
        return _coerce_number(value)
    if "boolean" in expected:
        return _coerce_boolean(value)
    if "string" in expected and not isinstance(value, str):
        return str(value), True, False
    return value, False, False


def _coerce_integer(value: object) -> tuple[object, bool, bool]:
    if isinstance(value, bool):
        return value, False, True
    if isinstance(value, int):
        return value, False, False
    try:
        text = str(value).strip()
        number = int(float(text))
        if float(text) != number:
            return value, False, True
        return number, True, False
    except (TypeError, ValueError):
        return value, False, True


def _coerce_number(value: object) -> tuple[object, bool, bool]:
    if isinstance(value, bool):
        return value, False, True
    if isinstance(value, (int, float)):
        return value, False, False
    try:
        return float(str(value).strip()), True, False
    except (TypeError, ValueError):
        return value, False, True


def _coerce_boolean(value: object) -> tuple[object, bool, bool]:
    if isinstance(value, bool):
        return value, False, False
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True, True, False
    if text in {"false", "0", "no", "n"}:
        return False, True, False
    return value, False, True
