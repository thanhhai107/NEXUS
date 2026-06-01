"""Reliability failure injector (R1-R10).

Injectors:
    R1 - api_failures: mark records as simulated API failures
    R2 - timeouts: mark records as timeout-affected
    R3 - rate_limits: mark records as rate-limited
    R4 - partial_downloads: truncate record set
    R5 - retry_duplicates: inject extra copies
    R6 - non_idempotent_writes: duplicate full batch
    R7 - atomic_write_failures: truncate at mid-point
    R8 - late_arriving_data: inject old-timestamp records
    R9 - out_of_order_data: shuffle records
    R10 - poison_records: inject malformed records
"""

from __future__ import annotations

import copy
import random
from typing import Any


class ReliabilityInjector:
    """Inject reliability/failure scenarios."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        random.seed(seed)

    def inject_api_failures(
        self,
        records: list[dict[str, Any]],
        p_failure: float = 0.30,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        result = copy.deepcopy(records)
        for record in result:
            record["_simulated_api_failure"] = random.random() < p_failure
        return result

    def inject_timeouts(
        self,
        records: list[dict[str, Any]],
        p_timeout: float = 0.10,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        result = copy.deepcopy(records)
        for record in result:
            record["_simulated_timeout"] = random.random() < p_timeout
        return result

    def inject_rate_limits(
        self,
        records: list[dict[str, Any]],
        p_limited: float = 0.15,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        result = copy.deepcopy(records)
        for record in result:
            record["_simulated_rate_limited"] = random.random() < p_limited
        return result

    def inject_partial_downloads(
        self,
        records: list[dict[str, Any]],
        truncate_ratio: float = 0.20,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not records or truncate_ratio <= 0:
            return list(records)
        cutoff = max(1, int(len(records) * (1 - truncate_ratio)))
        return list(records[:cutoff])

    def inject_retry_duplicates(
        self,
        records: list[dict[str, Any]],
        p_duplicate: float = 0.05,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not records or p_duplicate <= 0:
            return list(records)
        dup_count = max(1, int(len(records) * p_duplicate))
        result = list(records)
        for _ in range(dup_count):
            idx = random.randint(0, len(records) - 1)
            result.insert(idx, copy.deepcopy(records[idx]))
        return result

    def inject_non_idempotent_writes(
        self,
        records: list[dict[str, Any]],
        replay_count: int = 3,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for _ in range(replay_count):
            result.extend(copy.deepcopy(records))
        return result

    def inject_atomic_write_failures(
        self,
        records: list[dict[str, Any]],
        crash_at_offset_pct: float = 0.70,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not records:
            return []
        cutoff = max(1, int(len(records) * crash_at_offset_pct))
        return list(records[:cutoff])

    def inject_late_arriving_data(
        self,
        records: list[dict[str, Any]],
        delay_days: int = 30,
        date_fields: list[str] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        result = copy.deepcopy(records)
        if not date_fields:
            date_fields = [k for k in (records[0].keys() if records else []) if "date_sk" in k.lower() or "date" in k.lower()]
        for record in result:
            for field in date_fields or []:
                if field in record and isinstance(record[field], (int, float)):
                    record[field] = int(record[field]) - delay_days
        return result

    def inject_out_of_order_data(
        self,
        records: list[dict[str, Any]],
        p_shuffle: float = 0.20,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if len(records) < 2 or p_shuffle <= 0:
            return list(records)
        result = list(records)
        swap_count = max(1, int(len(result) * p_shuffle))
        for _ in range(swap_count):
            i, j = random.randint(0, len(result) - 1), random.randint(0, len(result) - 1)
            result[i], result[j] = result[j], result[i]
        return result

    def inject_poison_records(
        self,
        records: list[dict[str, Any]],
        ratio: float = 0.01,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if not records or ratio <= 0:
            return list(records)
        result = copy.deepcopy(records)
        keys = list(records[0].keys()) if records else []
        poison_count = max(1, int(len(result) * ratio))
        for _ in range(poison_count):
            idx = random.randint(0, len(result) - 1)
            if keys:
                key = random.choice(keys)
                result[idx][key] = "POISON_\x00_INVALID_UTF8_半角"
        return result
