from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence


Record = Mapping[str, object]


@dataclass
class AutoFixResult:
    records: list[dict[str, object]]
    summary: dict[str, Any] = field(default_factory=dict)


def clean_column_name(name: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower())
    return normalized.strip("_")


def normalize_field_names(fields: Sequence[str], auto_fix: Mapping[str, Any] | None) -> list[str]:
    if not auto_fix or not auto_fix.get("normalize_column_names"):
        return list(fields)
    return [clean_column_name(field) for field in fields]


def normalize_field_name(field: str, auto_fix: Mapping[str, Any] | None) -> str:
    if not auto_fix or not auto_fix.get("normalize_column_names"):
        return field
    return clean_column_name(field)


def apply_auto_fix(records: Sequence[Record], auto_fix: Mapping[str, Any] | None) -> AutoFixResult:
    config = dict(auto_fix or {})
    summary: dict[str, Any] = {
        "enabled": bool(config),
        "record_count": len(records),
        "changed_record_count": 0,
        "trimmed_value_count": 0,
        "normalized_column_count": 0,
        "column_collision_count": 0,
        "filled_missing_count": 0,
        "rules": {
            "trim_strings": bool(config.get("trim_strings")),
            "normalize_column_names": bool(config.get("normalize_column_names")),
            "fill_missing": sorted((config.get("fill_missing") or {}).keys()),
        },
    }

    fixed_records: list[dict[str, object]] = []
    fill_missing = dict(config.get("fill_missing") or {})

    for record in records:
        original = dict(record)
        fixed: dict[str, object] = {}

        for raw_key, raw_value in original.items():
            value = raw_value
            if config.get("trim_strings") and isinstance(value, str):
                trimmed = value.strip()
                if trimmed != value:
                    summary["trimmed_value_count"] += 1
                value = trimmed

            key = clean_column_name(str(raw_key)) if config.get("normalize_column_names") else str(raw_key)
            if key != raw_key:
                summary["normalized_column_count"] += 1

            if key in fixed:
                summary["column_collision_count"] += 1
                if _is_missing(fixed[key]) and not _is_missing(value):
                    fixed[key] = value
                continue
            fixed[key] = value

        for raw_key, fill_value in fill_missing.items():
            key = clean_column_name(str(raw_key)) if config.get("normalize_column_names") else str(raw_key)
            if key not in fixed or _is_missing(fixed[key]):
                fixed[key] = fill_value
                summary["filled_missing_count"] += 1

        if fixed != original:
            summary["changed_record_count"] += 1
        fixed_records.append(fixed)

    return AutoFixResult(records=fixed_records, summary=summary)


def _is_missing(value: object) -> bool:
    return value is None or value == ""
