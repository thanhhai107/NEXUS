"""Semantic error injector (M1-M9).

Injectors:
    M1 - same_name_different_meaning: reuse field name with different semantics
    M2 - different_name_same_meaning: rename with semantic equivalence
    M3 - different_units: change unit scale
    M4 - different_timestamp_format: change date format
    M5 - different_time_granularity: roll up dates
    M6 - different_spatial_reference: change CRS (N/A for core TPC-DS)
    M7 - different_aggregation_level: roll up numeric values
    M8 - different_business_definition: transform formula
    M9 - entity_resolution_problems: shift surrogate keys
"""

from __future__ import annotations

import copy
import random
from datetime import datetime, date
from typing import Any


class SemanticInjector:
    """Inject semantic-level errors into TPC-DS records."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        random.seed(seed)

    def inject_same_name_different_meaning(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        source_a_meaning: str = "",
        source_b_meaning: str = "",
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        return copy.deepcopy(records)

    def inject_different_name_same_meaning(
        self,
        records: list[dict[str, Any]],
        old_name: str = "",
        new_name: str = "",
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not old_name or not new_name:
            return list(records)
        result = copy.deepcopy(records)
        for record in result:
            if old_name in record:
                record[new_name] = record.pop(old_name)
        return result

    def inject_different_units(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        multiplier: float = 1.0,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if multiplier == 1.0:
            return list(records)
        result = copy.deepcopy(records)
        for record in result:
            if field in record and isinstance(record[field], (int, float)):
                record[field] = round(record[field] * multiplier, 4)
        return result

    def inject_different_timestamp_format(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        target_format: str = "unix_epoch",
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not field:
            date_fields = [k for k in records[0].keys() if "date" in k.lower()] if records else []
            if not date_fields:
                return list(records)
            field = date_fields[0]

        result = copy.deepcopy(records)
        for record in result:
            if field not in record:
                continue
            val = record[field]
            try:
                if target_format == "unix_epoch":
                    record[field] = str(int(random.randint(946684800, 1609459200)))
                elif target_format == "us_format":
                    parsed = datetime.fromisoformat(str(val))
                    record[field] = parsed.strftime("%m/%d/%Y")
                elif target_format == "iso_8601":
                    record[field] = str(val)
            except (ValueError, TypeError):
                record[field] = str(val)
        return result

    def inject_different_time_granularity(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        granularity: str = "week",
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not field:
            return list(records)
        result = copy.deepcopy(records)
        for record in result:
            if field in record and isinstance(record[field], (int, float)):
                if granularity == "week":
                    record[field] = round(record[field] / 7) * 7
                elif granularity == "month":
                    record[field] = round(record[field] / 30) * 30
                elif granularity == "quarter":
                    record[field] = round(record[field] / 90) * 90
        return result

    def inject_different_spatial_reference(
        self,
        records: list[dict[str, Any]],
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        return copy.deepcopy(records)

    def inject_different_aggregation_level(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        aggregation: str = "sum",
        group_by: str = "",
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not field or not group_by:
            return list(records)
        grouped: dict[Any, list[dict[str, Any]]] = {}
        result = copy.deepcopy(records)
        for record in result:
            key = record.get(group_by)
            grouped.setdefault(key, []).append(record)

        for key, group in grouped.items():
            if aggregation == "sum":
                total = sum(r.get(field, 0) or 0 for r in group)
                for record in group:
                    record[field] = total
            elif aggregation == "avg":
                vals = [r.get(field, 0) or 0 for r in group]
                avg = sum(vals) / len(vals) if vals else 0
                for record in group:
                    record[field] = round(avg, 2)
            elif aggregation == "max":
                max_val = max(r.get(field, 0) or 0 for r in group)
                for record in group:
                    record[field] = max_val
        return result

    def inject_different_business_definition(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        target_field: str = "",
        transform: str = "double",
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not field:
            return list(records)
        result = copy.deepcopy(records)
        target = target_field or field
        for record in result:
            if field in record and isinstance(record[field], (int, float)):
                val = record[field]
                if transform == "double":
                    record[target] = val * 2
                elif transform == "half":
                    record[target] = val / 2
                elif transform == "negate":
                    record[target] = -val
                elif transform == "square":
                    record[target] = val ** 2
        return result

    def inject_entity_resolution_problems(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        key_offset: int = 0,
        key_prefix: str = "",
        probability: float = 1.0,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not field:
            return list(records)
        result = copy.deepcopy(records)
        for record in result:
            if random.random() > probability or field not in record:
                continue
            val = record[field]
            if isinstance(val, int):
                record[field] = val + key_offset
            elif isinstance(val, str) and key_prefix:
                record[field] = f"{key_prefix}{val}"
        return result
