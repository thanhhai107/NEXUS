from __future__ import annotations

from dataclasses import dataclass, field
from statistics import quantiles
from typing import Any, Mapping, Sequence

Record = Mapping[str, object]


@dataclass
class SilverPreparationResult:
    records: list[dict[str, object]]
    summary: dict[str, Any] = field(default_factory=dict)


def prepare_silver_records(
    records: Sequence[Record],
    *,
    required_columns: Sequence[str],
    dedup_keys: Sequence[str],
    numeric_columns: Sequence[str] | None = None,
) -> SilverPreparationResult:
    """Apply generic Silver data-quality handling for local record batches."""
    numeric = list(numeric_columns or _numeric_columns(records))
    outlier_fields = _iqr_outliers(records, numeric)
    seen: set[tuple[object, ...]] = set()
    output: list[dict[str, object]] = []
    missing_count = 0
    duplicate_count = 0
    outlier_count = 0
    conflict_count = 0
    first_by_key: dict[tuple[object, ...], dict[str, object]] = {}

    for record in records:
        row = dict(record)
        for column in required_columns:
            missing = row.get(column) in (None, "")
            row[f"is_missing_{column}"] = missing
            if missing:
                missing_count += 1

        key = tuple(row.get(column) for column in dedup_keys) if dedup_keys else tuple(sorted(row.items()))
        first = first_by_key.setdefault(key, dict(row))
        conflict_fields = [
            field
            for field, value in row.items()
            if field in first and first[field] not in (value, None, "") and value not in (None, "")
        ]
        if key in seen:
            duplicate_count += 1
            if conflict_fields:
                conflict_count += 1
            continue
        seen.add(key)

        row["_nexus_conflict_fields"] = conflict_fields
        if conflict_fields:
            conflict_count += 1

        row["_nexus_outlier_fields"] = sorted(outlier_fields.get(id(record), []))
        if row["_nexus_outlier_fields"]:
            outlier_count += 1
        output.append(row)

    return SilverPreparationResult(
        records=output,
        summary={
            "input_count": len(records),
            "output_count": len(output),
            "missing_value_count": missing_count,
            "duplicate_record_count": duplicate_count,
            "outlier_record_count": outlier_count,
            "inconsistency_record_count": conflict_count,
            "dedup_keys": list(dedup_keys),
            "numeric_columns": numeric,
        },
    )


def _numeric_columns(records: Sequence[Record]) -> list[str]:
    columns: set[str] = set()
    for record in records:
        for key, value in record.items():
            if _to_float(value) is not None:
                columns.add(str(key))
    return sorted(columns)


def _iqr_outliers(records: Sequence[Record], numeric_columns: Sequence[str]) -> dict[int, list[str]]:
    output: dict[int, list[str]] = {}
    for column in numeric_columns:
        values = [_to_float(record.get(column)) for record in records]
        clean_values = [value for value in values if value is not None]
        if len(clean_values) < 4:
            continue
        q1, _, q3 = quantiles(clean_values, n=4, method="inclusive")
        iqr = q3 - q1
        if iqr == 0:
            continue
        lower = q1 - (1.5 * iqr)
        upper = q3 + (1.5 * iqr)
        for record, value in zip(records, values):
            if value is not None and (value < lower or value > upper):
                output.setdefault(id(record), []).append(column)
    return output


def _to_float(value: object) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = ["SilverPreparationResult", "prepare_silver_records"]
