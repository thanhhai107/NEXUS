"""Schema error injector (S1-S5).

Injectors:
    S1 - missing_field: drop a column from output
    S2 - new_field: add synthetic column
    S3 - used_field_removed: drop but keep in metadata
    S4 - field_rename: rename column
    S5 - data_type_change: cast to different type
"""

from __future__ import annotations

import copy
import random
from typing import Any


class SchemaInjector:
    """Inject schema-level errors into TPC-DS records."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        random.seed(seed)

    def inject_missing_field(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        probability: float = 1.0,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        result = copy.deepcopy(records)
        for record in result:
            if random.random() < probability and field in record:
                del record[field]
        return result

    def inject_new_field(
        self,
        records: list[dict[str, Any]],
        field: str = "_new_field",
        generator_type: str = "random_string",
        values: list[Any] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        result = copy.deepcopy(records)

        if generator_type == "categorical" and values:
            pool = values
        elif generator_type == "random_integer":
            lo = kwargs.get("min_val", 0)
            hi = kwargs.get("max_val", 100)
            pool = None
        else:
            pool = None

        for record in result:
            if pool:
                record[field] = random.choice(pool)
            elif generator_type == "random_integer":
                record[field] = random.randint(lo, hi)  # type: ignore[possibly-undefined]
            else:
                record[field] = f"synthetic_{random.randint(10000, 99999)}"
        return result

    def inject_used_field_removed(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        probability: float = 1.0,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        return self.inject_missing_field(records, field, probability)

    def inject_field_rename(
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

    def inject_data_type_change(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        new_type: str = "string",
        probability: float = 1.0,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        result = copy.deepcopy(records)
        for record in result:
            if random.random() > probability or field not in record:
                continue
            val = record[field]
            if new_type == "string" and isinstance(val, (int, float)):
                record[field] = str(val)
            elif new_type == "integer" and isinstance(val, str):
                try:
                    record[field] = int(float(val))
                except (ValueError, TypeError):
                    record[field] = -1
            elif new_type == "float" and isinstance(val, (int, str)):
                try:
                    record[field] = float(val)
                except (ValueError, TypeError):
                    record[field] = -1.0
        return result
