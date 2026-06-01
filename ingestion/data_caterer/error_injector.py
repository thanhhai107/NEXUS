# Error injection utilities for Data Caterer generated data.
# Provides configurable error injection profiles and field-level
# corruption functions for testing data quality pipelines.
from __future__ import annotations

import copy
import random
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ErrorConfig:
    null_ratio: float = 0.02
    duplicate_ratio: float = 0.01
    type_error_ratio: float = 0.01
    overflow_ratio: float = 0.005
    fk_violation_ratio: float = 0.01
    schema_drift_ratio: float = 0.005
    format_error_ratio: float = 0.01
    encoding_error_ratio: float = 0.001
    out_of_order_ratio: float = 0.01
    truncation_ratio: float = 0.005
    seed: int | None = 42

    def __post_init__(self) -> None:
        if self.seed is not None:
            random.seed(self.seed)


def inject_nulls(records: list[dict[str, Any]], ratio: float = 0.02) -> list[dict[str, Any]]:
    result = copy.deepcopy(records)
    for record in result:
        if random.random() < ratio and record:
            key = random.choice(list(record.keys()))
            record[key] = None
    return result


def inject_duplicates(records: list[dict[str, Any]], ratio: float = 0.01) -> list[dict[str, Any]]:
    if not records or ratio <= 0:
        return list(records)
    dup_count = max(1, int(len(records) * ratio))
    result = list(records)
    for _ in range(dup_count):
        idx = random.randint(0, len(records) - 1)
        result.insert(idx, copy.deepcopy(records[idx]))
    return result


def inject_type_errors(records: list[dict[str, Any]], ratio: float = 0.01) -> list[dict[str, Any]]:
    result = copy.deepcopy(records)
    for record in result:
        if random.random() < ratio and record:
            key = random.choice(list(record.keys()))
            val = record[key]
            if isinstance(val, (int, float)):
                record[key] = f"INVALID_{val}"
            elif isinstance(val, str) and val.replace(".", "").replace("-", "").isdigit():
                record[key] = "NOT_A_NUMBER"
    return result


def inject_fk_violations(records: list[dict[str, Any]], ratio: float = 0.01) -> list[dict[str, Any]]:
    result = copy.deepcopy(records)
    fk_fields = [k for k in result[0].keys() if k.endswith("key") or k.endswith("_sk")] if result else []
    if not fk_fields:
        return result
    for record in result:
        if random.random() < ratio:
            field = random.choice(fk_fields)
            record[field] = -99999
    return result


def inject_schema_drift(records: list[dict[str, Any]], ratio: float = 0.005) -> list[dict[str, Any]]:
    result = copy.deepcopy(records)
    for record in result:
        if random.random() < ratio:
            record["_drift_extra_field"] = "SCHEMA_DRIFT_DATA"
        if random.random() < ratio and record:
            key = random.choice(list(record.keys()))
            del record[key]
    return result


def inject_format_errors(records: list[dict[str, Any]], ratio: float = 0.01) -> list[dict[str, Any]]:
    result = copy.deepcopy(records)
    date_fields = [k for k in result[0].keys() if "date" in k.lower() or "time" in k.lower() or "ts" in k.lower()] if result else []
    if not date_fields:
        return result
    for record in result:
        if random.random() < ratio and date_fields:
            field = random.choice(date_fields)
            record[field] = "2024-13-45T99:99:99Z"
    return result


def inject_out_of_order(records: list[dict[str, Any]], ratio: float = 0.01) -> list[dict[str, Any]]:
    if len(records) < 2 or ratio <= 0:
        return list(records)
    result = list(records)
    swap_count = max(1, int(len(result) * ratio))
    for _ in range(swap_count):
        i, j = random.randint(0, len(result) - 1), random.randint(0, len(result) - 1)
        result[i], result[j] = result[j], result[i]
    return result


def apply_error_profile(records: list[dict[str, Any]], config: ErrorConfig | None = None) -> list[dict[str, Any]]:
    if config is None:
        config = ErrorConfig()
    result = list(records)
    result = inject_nulls(result, config.null_ratio)
    result = inject_duplicates(result, config.duplicate_ratio)
    result = inject_type_errors(result, config.type_error_ratio)
    result = inject_fk_violations(result, config.fk_violation_ratio)
    result = inject_schema_drift(result, config.schema_drift_ratio)
    result = inject_format_errors(result, config.format_error_ratio)
    result = inject_out_of_order(result, config.out_of_order_ratio)
    return result
