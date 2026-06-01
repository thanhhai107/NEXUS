"""Data quality error injector (Q1-Q6).

Injectors:
    Q1 - invalid_data: out-of-range values
    Q2 - outliers: IQR-based outlier injection
    Q3 - missing_data_mcar: completely random nulls
    Q4 - missing_data_mar: conditionally random nulls
    Q5 - duplicate_data: deep-copy records
    Q6 - cross_source_inconsistency: different values for same PK
"""

from __future__ import annotations

import copy
import random
from typing import Any


class QualityInjector:
    """Inject data quality errors into TPC-DS records."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        random.seed(seed)

    def inject_invalid_data(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        min_val: float = float("-inf"),
        max_val: float = float("inf"),
        ratio: float = 0.05,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not field or ratio <= 0:
            return list(records)
        result = copy.deepcopy(records)
        for record in result:
            if random.random() > ratio or field not in record:
                continue
            val = record[field]
            if isinstance(val, (int, float)):
                if min_val != float("-inf"):
                    record[field] = min_val - abs(min_val) - random.randint(1, 100)
                elif max_val != float("inf"):
                    record[field] = max_val + abs(max_val) + random.randint(1, 100)
                else:
                    record[field] = -999999
            elif isinstance(val, str):
                record[field] = ""
        return result

    def inject_outliers(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        multiplier: float = 3.0,
        ratio: float = 0.02,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not field or ratio <= 0:
            return list(records)

        numeric_vals = [
            r[field] for r in records
            if field in r and isinstance(r[field], (int, float))
        ]
        if not numeric_vals:
            return list(records)

        sorted_vals = sorted(numeric_vals)
        n = len(sorted_vals)
        q1 = sorted_vals[n // 4]
        q3 = sorted_vals[3 * n // 4]
        iqr = q3 - q1

        if iqr == 0:
            return list(records)

        lower = q1 - multiplier * iqr
        upper = q3 + multiplier * iqr

        result = copy.deepcopy(records)
        for record in result:
            if random.random() > ratio or field not in record:
                continue
            if random.random() < 0.5:
                record[field] = lower - abs(lower) * 0.5
            else:
                record[field] = upper + abs(upper) * 0.5
        return result

    def inject_missing_data_mcar(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        ratio: float = 0.10,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not field or ratio <= 0:
            return list(records)
        result = copy.deepcopy(records)
        for record in result:
            if random.random() < ratio and field in record:
                record[field] = None
        return result

    def inject_missing_data_mar(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        condition_field: str = "",
        condition_value: Any = None,
        ratio: float = 0.10,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not field or not condition_field or ratio <= 0:
            return list(records)
        result = copy.deepcopy(records)
        for record in result:
            if field not in record:
                continue
            if record.get(condition_field) == condition_value or (
                condition_value is None and condition_field in record
            ):
                if random.random() < ratio:
                    record[field] = None
        return result

    def inject_duplicate_data(
        self,
        records: list[dict[str, Any]],
        ratio: float = 0.02,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not records or ratio <= 0:
            return list(records)
        dup_count = max(1, int(len(records) * ratio))
        result = list(records)
        for _ in range(dup_count):
            idx = random.randint(0, len(records) - 1)
            result.insert(idx, copy.deepcopy(records[idx]))
        return result

    def inject_cross_source_inconsistency(
        self,
        records: list[dict[str, Any]],
        field: str = "",
        pk_field: str = "",
        ratio: float = 0.05,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not field or ratio <= 0:
            return list(records)
        result = copy.deepcopy(records)
        for record in result:
            if random.random() > ratio or field not in record:
                continue
            val = record[field]
            if isinstance(val, str):
                record[field] = "INCONSISTENT_" + val
            elif isinstance(val, bool):
                record[field] = not val
            elif isinstance(val, (int, float)):
                record[field] = val + random.randint(1000, 9999)
        return result
