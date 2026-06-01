"""Deterministic record hashing for ground-truth injection tracking.

Every injected error is tracked via SHA-256 hashes of the modified records,
enabling exact TP/FP/FN counting during evaluation without needing to
store full copies of the data.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field as dc_field
from typing import Any


def stable_json_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str)


def hash_record(record: dict[str, Any]) -> str:
    digest = hashlib.sha256(stable_json_dumps(record).encode("utf-8")).hexdigest()
    return digest


def hash_records(records: list[dict[str, Any]]) -> list[str]:
    return [hash_record(r) for r in records]


@dataclass
class InjectionLog:
    scenario_id: str
    table: str
    error_type: str
    error_category: str
    target_field: str | None = None
    injected_record_hashes: list[str] = dc_field(default_factory=list)
    parameters: dict[str, Any] = dc_field(default_factory=dict)
    expected_detection: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "table": self.table,
            "error_type": self.error_type,
            "error_category": self.error_category,
            "field": self.target_field,
            "injected_record_hashes": self.injected_record_hashes,
            "parameters": self.parameters,
            "expected_detection": self.expected_detection,
            "injection_count": len(self.injected_record_hashes),
        }
