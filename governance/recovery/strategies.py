"""
Repair strategies for TPC-DI error recovery.

Each strategy implements ``RepairStrategy`` and handles a specific error type:
- ``DropExtraFieldStrategy`` — field_count_mismatch caused by extra field
- ``SafeCastStrategy`` — type_coercion_error: try int/float/date conversion
- ``DedupStrategy`` — duplicate_primary_key: suppress duplicate lines
"""

from __future__ import annotations

import csv
import io
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from common.tpcdi_sources import get_source_config, list_source_files, resolve_batch_path


@dataclass
class RepairResult:
    success: bool
    description: str
    confidence: float  # 0.0 - 1.0
    before: str = ""
    after: str = ""


@dataclass
class RepairContext:
    """Context passed to each strategy for decision making."""
    source_name: str
    batch_id: str
    relative_file: str
    expected_columns: list[str]
    delimiter: str
    line_number: int
    raw_line: str
    mutation_id: str | None = None
    field_index: int | None = None
    target_field: str | None = None
    original_value: str | None = None
    mutated_value: str | None = None
    mutation_type: str | None = None


class RepairStrategy(ABC):
    @abstractmethod
    def can_repair(self, context: RepairContext) -> bool:
        ...

    @abstractmethod
    def repair(self, context: RepairContext) -> RepairResult:
        ...


# ── DropExtraFieldStrategy ────────────────────────────────────────────────

class DropExtraFieldStrategy(RepairStrategy):
    """Remove the extra field at the known field_index.

    If field_index is known, pop exactly that field.
    Otherwise fallback: truncate to expected column count.
    Confidence: 0.9 — deterministic.
    """

    def can_repair(self, context: RepairContext) -> bool:
        return True

    def repair(self, context: RepairContext) -> RepairResult:
        fields = context.raw_line.split(context.delimiter)
        expected = len(context.expected_columns)

        if context.field_index is not None and context.field_index < len(fields):
            removed = fields.pop(context.field_index)
            return RepairResult(
                success=True,
                description=f"Removed extra field at index {context.field_index}: '{removed}'",
                confidence=0.9,
                before=context.raw_line,
                after=context.delimiter.join(fields),
            )

        # Fallback: if actual > expected, truncate
        if len(fields) > expected:
            trimmed = context.delimiter.join(fields[:expected])
            return RepairResult(
                success=True,
                description=f"Truncated {len(fields) - expected} extra field(s) from end",
                confidence=0.7,
                before=context.raw_line,
                after=trimmed,
            )

        return RepairResult(False, "No extra field found to drop", 0.0)


# ── SafeCastStrategy ──────────────────────────────────────────────────────

class SafeCastStrategy(RepairStrategy):
    """Repair type coercion errors by restoring original_value from manifest.

    If original_value is available (from manifest), restore it exactly.
    Otherwise, try safe-cast on the specific field_index only.
    Never touch non-mutated fields.
    Confidence: 0.9 with original_value, 0.5 with heuristic.
    """

    def can_repair(self, context: RepairContext) -> bool:
        return True

    def repair(self, context: RepairContext) -> RepairResult:
        fields = context.raw_line.split(context.delimiter)

        # Strategy 1: restore original_value from manifest (best)
        if context.original_value and context.field_index is not None and context.field_index < len(fields):
            fields[context.field_index] = context.original_value
            return RepairResult(
                success=True,
                description=f"Restored original_value '{context.original_value}' at field {context.field_index}",
                confidence=0.9,
                before=context.raw_line,
                after=context.delimiter.join(fields),
            )

        # Strategy 2: restore original_value without field_index (search)
        if context.original_value:
            for idx, field in enumerate(fields):
                if field == context.mutated_value:
                    fields[idx] = context.original_value
                    return RepairResult(
                        success=True,
                        description=f"Restored original_value '{context.original_value}' at field {idx}",
                        confidence=0.85,
                        before=context.raw_line,
                        after=context.delimiter.join(fields),
                    )

        # Strategy 3: safe-cast on mutated field only (if field_index known)
        if context.field_index is not None and context.field_index < len(fields):
            val = fields[context.field_index]
            cleaned = "".join(c for c in val if c.isdigit() or c in ".-")
            if cleaned and cleaned.lstrip("-").replace(".", "").isdigit():
                fields[context.field_index] = cleaned
            else:
                fields[context.field_index] = "0"
            return RepairResult(
                success=True,
                description=f"Safe-cast field {context.field_index}: '{val}' -> '{fields[context.field_index]}'",
                confidence=0.5,
                before=context.raw_line,
                after=context.delimiter.join(fields),
            )

        return RepairResult(False, "No original_value or field_index available for type repair", 0.0)


# ── DedupStrategy ──────────────────────────────────────────────────────────

class DedupStrategy(RepairStrategy):
    """Suppress duplicate lines.

    Confidence: 0.95 — safe deterministic dedup.
    The engine.py handles the actual line removal; this strategy just
    reports the finding.
    """

    def can_repair(self, context: RepairContext) -> bool:
        return True

    def repair(self, context: RepairContext) -> RepairResult:
        return RepairResult(
            success=True,
            description=f"Removed duplicate at line {context.line_number}",
            confidence=0.95,
            before=context.raw_line,
            after="(line removed)",
        )


__all__ = [
    "RepairResult", "RepairContext", "RepairStrategy",
    "DropExtraFieldStrategy", "SafeCastStrategy", "DedupStrategy",
]
