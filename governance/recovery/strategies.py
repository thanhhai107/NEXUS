"""
Repair strategies for TPC-DI error recovery.

Each strategy implements ``RepairStrategy`` and handles a specific error type:
- ``DropExtraFieldStrategy`` — field_count_mismatch caused by extra field
- ``InferMissingFieldStrategy`` — field_count_mismatch caused by missing field
- ``SafeCastStrategy`` — type_coercion_error: try int/float/date conversion
- ``DedupStrategy`` — duplicate_primary_key: suppress duplicate lines
- ``DefaultValueStrategy`` — null_required_field / missing data: restore or default
- ``OutlierCorrectionStrategy`` — outlier_value: restore original
- ``BusinessRuleCorrectionStrategy`` — business_rule_violation: restore original
- ``SchemaRevertStrategy`` — schema-level errors: revert schema from backup
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


# ── InferMissingFieldStrategy ──────────────────────────────────────────────

class InferMissingFieldStrategy(RepairStrategy):
    """Repair a missing field by inserting a sensible default at the correct index.

    Strategy:
    1. If original_value is available from the manifest, restore it exactly (confidence 0.9).
    2. If field type can be inferred from expected_columns naming:
       - *_id, *_key, sk_* → "0"
       - *_dts, *_date, *_time → current UTC timestamp
       - *_price, *_amount, *_qty, *_quantity → "0"
       - *_flag, is_*, has_* → "false"
       - others → "" (empty string)
       Confidence: 0.7 with naming heuristic, 0.5 without.
    """

    def can_repair(self, context: RepairContext) -> bool:
        return True

    def repair(self, context: RepairContext) -> RepairResult:
        fields = context.raw_line.split(context.delimiter)
        expected_count = len(context.expected_columns)

        # Strategy 1: Restore original_value from manifest
        if context.original_value:
            fi = context.field_index if context.field_index is not None else len(fields)
            if fi >= len(fields):
                fi = len(fields)
            fields.insert(fi, context.original_value)
            return RepairResult(
                success=True,
                description=f"Restored missing field '{context.original_value}' at index {fi}",
                confidence=0.9,
                before=context.raw_line,
                after=context.delimiter.join(fields),
            )

        # Strategy 2: Infer default from column name
        if 0 <= context.field_index < len(context.expected_columns):
            target_col = context.expected_columns[context.field_index].lower()
            default = _infer_default_value(target_col)
            fi = context.field_index if context.field_index is not None else len(fields)
            if fi > len(fields):
                fi = len(fields)
            fields.insert(fi, default)
            return RepairResult(
                success=True,
                description=f"Inferred default '{default}' for missing field '{target_col}'",
                confidence=0.7 if default else 0.5,
                before=context.raw_line,
                after=context.delimiter.join(fields),
            )

        # Strategy 3: Insert empty string at field_index or at end
        fi = len(fields)
        fields.insert(fi, "")
        return RepairResult(
            success=True,
            description="Inserted empty string for unknown missing field",
            confidence=0.5,
            before=context.raw_line,
            after=context.delimiter.join(fields),
        )


def _infer_default_value(col_name: str) -> str:
    """Infer a sensible default for a column based on its name."""
    col = col_name.lower()
    if any(k in col for k in ("_id", "_key", "sk_", "pk_", "fk_")):
        return "0"
    if any(k in col for k in ("_dts", "_date", "_time", "timestamp")):
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if any(k in col for k in ("_price", "_amount", "_qty", "_quantity", "_volume")):
        return "0"
    if any(k in col for k in ("_flag", "is_", "has_")):
        return "false"
    return ""


# ── DefaultValueStrategy ──────────────────────────────────────────────────

class DefaultValueStrategy(RepairStrategy):
    """Repair null/missing required values by restoring original_value.

    Handles null_required_field and similar missing-data mutations.
    The injector stores the original value that was replaced with \\N,
    so we can restore it exactly.
    """

    def can_repair(self, context: RepairContext) -> bool:
        return True

    def repair(self, context: RepairContext) -> RepairResult:
        fields = context.raw_line.split(context.delimiter)
        mutated = context.mutated_value  # e.g. "\\N"

        # Strategy 1: Restore original_value from manifest
        if context.original_value:
            if context.field_index is not None and context.field_index < len(fields):
                fields[context.field_index] = context.original_value
                return RepairResult(
                    success=True,
                    description=f"Restored null field with original '{context.original_value}'",
                    confidence=0.9,
                    before=context.raw_line,
                    after=context.delimiter.join(fields),
                )

        # Strategy 2: Find the \\N sentinel and replace with 0
        if mutated and context.field_index is not None and context.field_index < len(fields):
            if fields[context.field_index] == mutated or fields[context.field_index] == r"\N":
                fields[context.field_index] = "0"
                return RepairResult(
                    success=True,
                    description=f"Replaced null sentinel '{mutated}' with default '0'",
                    confidence=0.6,
                    before=context.raw_line,
                    after=context.delimiter.join(fields),
                )

        # Strategy 3: Search and replace \\N anywhere
        for idx, val in enumerate(fields):
            if val.strip() == r"\N" or val.strip() == "\\N":
                fields[idx] = "0"
                return RepairResult(
                    success=True,
                    description=f"Replaced \\N at field {idx} with '0'",
                    confidence=0.6,
                    before=context.raw_line,
                    after=context.delimiter.join(fields),
                )

        return RepairResult(False, "No null sentinel found to replace", 0.0)


# ── OutlierCorrectionStrategy ─────────────────────────────────────────────

class OutlierCorrectionStrategy(RepairStrategy):
    """Repair outlier values by restoring original_value from the injection manifest.

    The source_injector stores the original value before the outlier
    multiplication, making exact recovery possible.
    """

    def can_repair(self, context: RepairContext) -> bool:
        return True

    def repair(self, context: RepairContext) -> RepairResult:
        fields = context.raw_line.split(context.delimiter)

        if context.original_value:
            if context.field_index is not None and context.field_index < len(fields):
                fields[context.field_index] = context.original_value
                return RepairResult(
                    success=True,
                    description=f"Restored outlier field to original '{context.original_value}'",
                    confidence=0.9,
                    before=context.raw_line,
                    after=context.delimiter.join(fields),
                )

        # Fallback: divide by factor if we can infer the factor
        if context.field_index is not None and context.field_index < len(fields):
            val = fields[context.field_index].strip()
            try:
                n = float(val)
                if abs(n) >= 1_000_000:
                    # Probably multiplied by 1M — divide back
                    restored = str(int(n / 1_000_000)) if n == int(n) else str(n / 1_000_000)
                    fields[context.field_index] = restored
                    return RepairResult(
                        success=True,
                        description=f"De-scaled outlier {val} → {restored}",
                        confidence=0.6,
                        before=context.raw_line,
                        after=context.delimiter.join(fields),
                    )
            except ValueError:
                pass

        return RepairResult(False, "No original_value or heuristic available", 0.0)


# ── BusinessRuleCorrectionStrategy ────────────────────────────────────────

class BusinessRuleCorrectionStrategy(RepairStrategy):
    """Repair business rule violations by restoring original_value.

    The source_injector stores the pre-negation value; we restore it.
    For formula-based errors without original_value, revert the formula.
    """

    def can_repair(self, context: RepairContext) -> bool:
        return True

    def repair(self, context: RepairContext) -> RepairResult:
        fields = context.raw_line.split(context.delimiter)

        if context.original_value:
            if context.field_index is not None and context.field_index < len(fields):
                fields[context.field_index] = context.original_value
                return RepairResult(
                    success=True,
                    description=f"Restored negated field to original '{context.original_value}'",
                    confidence=0.9,
                    before=context.raw_line,
                    after=context.delimiter.join(fields),
                )

        # Fallback: if value is negative but should be positive, flip sign
        if context.field_index is not None and context.field_index < len(fields):
            val = fields[context.field_index].strip()
            try:
                n = float(val)
                if n < 0:
                    restored = str(abs(int(n))) if n == int(n) else str(abs(n))
                    fields[context.field_index] = restored
                    return RepairResult(
                        success=True,
                        description=f"Fliped negative value {val} → {restored}",
                        confidence=0.6,
                        before=context.raw_line,
                        after=context.delimiter.join(fields),
                    )
            except ValueError:
                pass

        return RepairResult(False, "No original_value available for business rule repair", 0.0)


# ── SchemaRevertStrategy ──────────────────────────────────────────────────

class SchemaRevertStrategy(RepairStrategy):
    """Repair schema-level errors by reverting to a backed-up schema file.

    Operates on JSON Schema files rather than data lines.
    """

    def can_repair(self, context: RepairContext) -> bool:
        return True

    def repair(self, context: RepairContext) -> RepairResult:
        from common.tpcdi_sources import get_schema_path

        # Try to find and restore schema backup
        schema_name = context.source_name
        try:
            schema_path = get_schema_path(schema_name)
        except Exception:
            schema_path = None

        if schema_path and schema_path.exists():
            backup_dir = schema_path.parent / "schema_backups"
            backup = backup_dir / schema_path.name
            if backup.exists():
                import shutil
                shutil.copy(backup, schema_path)
                return RepairResult(
                    success=True,
                    description=f"Reverted schema {schema_path.name} from backup",
                    confidence=0.95,
                    before="(mutated schema)",
                    after="(restored schema)",
                )

        return RepairResult(
            False,
            f"No schema backup found for {schema_name}",
            0.0,
        )


__all__ = [
    "RepairResult", "RepairContext", "RepairStrategy",
    "DropExtraFieldStrategy", "InferMissingFieldStrategy",
    "SafeCastStrategy", "DedupStrategy",
    "DefaultValueStrategy", "OutlierCorrectionStrategy",
    "BusinessRuleCorrectionStrategy", "SchemaRevertStrategy",
]
