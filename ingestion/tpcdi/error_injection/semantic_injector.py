"""
TPC-DI Semantic Error Injector.

Applies column-level semantic transformations to a scenario copy of the source
data.  These mutations are syntactically valid — parsers pass without error —
but produce analytically wrong values.  Detection requires semantic validation
rules (Phase 3 quality pipeline) or Gold-level ground-truth comparison.

Supported mutation_types:
  unit_changed                — Multiply all ``amount_fields`` by a conversion factor.
  timestamp_format_changed    — Reformat ``timestamp_fields`` (YYYY-MM-DD → DD/MM/YYYY).
  timestamp_granularity_changed — Truncate datetime to date-only (lose time precision).
  entity_id_ambiguity         — Duplicate an entity with a different technical ID.
  rename_to_synonym           — Rename a field to a synonym in ``semantic_meta``.
  pre_aggregate_records       — Group-by key fields and sum amount fields.

Usage::

    from ingestion.tpcdi.error_injection.semantic_injector import SemanticInjector

    si = SemanticInjector(seed=42)
    scenario_root = si.create_scenario(
        "sem_unit_001",
        target_source="daily_market",
        batch_id="batch1",
        mutation_type="unit_changed",
        factor=100,
    )
"""

from __future__ import annotations

import json
import random
import shutil
from pathlib import Path
from typing import Any

from common.tpcdi_sources import source_root, get_source_config

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCENARIO_BASE = Path("runtime/tpcdi/scenarios")

# Semantic metadata: fields whose semantics are meaningful for injection.
# Maps source_name → semantic_meta dict.
# Populated partially here; extended by tpcdi_sources.yml ``semantic_meta``
# section in Phase 3 when that annotation is added.
_SEMANTIC_META: dict[str, dict[str, Any]] = {
    "daily_market": {
        "amount_fields": ["dm_close", "dm_high", "dm_low"],
        "volume_fields": ["dm_volume"],
        "timestamp_fields": ["dm_date"],
        "entity_id_fields": ["dm_s_symb"],
        "timestamp_format": "%Y-%m-%d",
        "unit": "USD",
    },
    "trade": {
        "amount_fields": ["trade_price", "cash_amount", "fee", "commission", "tax"],
        "volume_fields": ["trade_quantity"],
        "timestamp_fields": ["trade_dts"],
        "entity_id_fields": ["trade_id", "s_symbol"],
        "timestamp_format": "%Y-%m-%d %H:%M:%S",
        "unit": "USD",
    },
    "cash_transaction": {
        "amount_fields": ["ct_amt"],
        "timestamp_fields": ["ct_dts"],
        "entity_id_fields": ["ct_ca_id"],
        "timestamp_format": "%Y-%m-%d %H:%M:%S",
        "unit": "USD",
    },
    "holding_history": {
        "amount_fields": [],
        "volume_fields": ["hh_before_qty", "hh_after_qty"],
        "entity_id_fields": ["hh_h_t_id", "hh_t_id"],
        "timestamp_fields": [],
    },
}


def get_semantic_meta(source_name: str) -> dict[str, Any]:
    """Return semantic metadata for a source. Falls back to empty dict."""
    return _SEMANTIC_META.get(source_name, {})


class SemanticInjector:
    """Column-level semantic mutation injector for TPC-DI DIGen data."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        self.rng = random.Random(seed)

    def create_scenario(
        self,
        scenario_id: str,
        *,
        target_source: str,
        batch_id: str = "batch1",
        mutation_type: str,
        **kwargs: Any,
    ) -> Path:
        """Create a scenario directory with one semantic mutation applied.

        Returns
        -------
        Path to the scenario root.
        """
        clean_root = source_root()
        scenario_root_dir = PROJECT_ROOT / SCENARIO_BASE / scenario_id
        src_dir = scenario_root_dir / "source"

        if src_dir.exists():
            shutil.rmtree(src_dir)
        shutil.copytree(clean_root, src_dir)

        method = getattr(self, f"_inject_{mutation_type}", None)
        if method is None:
            raise ValueError(f"Unknown semantic mutation_type: {mutation_type!r}")

        mutations = method(src_dir, target_source, batch_id, **kwargs)

        manifest = {
            "scenario_id": scenario_id,
            "seed": self.seed,
            "base_source_root": str(clean_root),
            "scenario_root": str(scenario_root_dir),
            "scenario_source_root": str(src_dir),
            "target_source": target_source,
            "batch": batch_id,
            "mutation_type": mutation_type,
            "mutations": mutations,
        }
        (scenario_root_dir / "injection_manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
        return scenario_root_dir

    # ── File helpers ─────────────────────────────────────────────────────────

    def _resolve_files(
        self, src_dir: Path, source_name: str, batch_id: str
    ) -> list[Path]:
        cfg = get_source_config(source_name)
        if batch_id not in cfg.get("batches", []):
            return []
        batch_name = {"batch1": "Batch1", "batch2": "Batch2", "batch3": "Batch3"}.get(
            batch_id, batch_id.capitalize()
        )
        batch_dir = src_dir / batch_name
        exclude = cfg.get("exclude_patterns", [])
        result: list[Path] = []
        for pat in cfg.get("files", []):
            for f in sorted(batch_dir.glob(pat)):
                if f.is_file() and not any(f.match(ex) for ex in exclude):
                    result.append(f)
        return result

    def _get_col_indices(
        self, source_name: str, field_names: list[str]
    ) -> list[int]:
        columns = get_source_config(source_name).get("columns", [])
        return [columns.index(f) for f in field_names if f in columns]

    # ── Mutation implementations ─────────────────────────────────────────────

    def _inject_unit_changed(
        self,
        src_dir: Path,
        source_name: str,
        batch_id: str,
        *,
        factor: float = 100,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Multiply all amount fields by ``factor``."""
        meta = get_semantic_meta(source_name)
        amount_fields = meta.get("amount_fields", [])
        if not amount_fields:
            return [{"mutation_type": "unit_changed", "skipped": "no_amount_fields"}]

        cfg = get_source_config(source_name)
        delimiter = cfg.get("delimiter", "|")
        col_indices = self._get_col_indices(source_name, amount_fields)

        mutations: list[dict[str, Any]] = []
        for filepath in self._resolve_files(src_dir, source_name, batch_id):
            lines = filepath.read_text(encoding="utf-8").splitlines(keepends=True)
            new_lines: list[str] = []
            for line in lines:
                stripped = line.rstrip("\n\r")
                fields = stripped.split(delimiter)
                for idx in col_indices:
                    if idx < len(fields):
                        s = fields[idx].strip()
                        if s.lstrip("-").replace(".", "").isdigit():
                            try:
                                orig = float(s)
                                if "." in s:
                                    fields[idx] = str(orig * factor)
                                else:
                                    fields[idx] = str(int(orig * factor))
                            except ValueError:
                                pass
                new_lines.append(delimiter.join(fields) + "\n")
            filepath.write_text("".join(new_lines), encoding="utf-8")
            mutations.append({
                "mutation_type": "unit_changed",
                "source": source_name,
                "affected_fields": amount_fields,
                "factor": factor,
                "expected_detection": "semantic_anomaly",
                "expected_stage": "semantic_validation",
                "recoverable": False,
                "recovery_hint": "manual_correction",
            })
        return mutations

    def _inject_timestamp_format_changed(
        self,
        src_dir: Path,
        source_name: str,
        batch_id: str,
        *,
        new_format: str = "%d/%m/%Y",
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Reformat timestamp fields from ISO to ``new_format``."""
        from datetime import datetime

        meta = get_semantic_meta(source_name)
        ts_fields = meta.get("timestamp_fields", [])
        orig_fmt = meta.get("timestamp_format", "%Y-%m-%d")
        if not ts_fields:
            return [{"mutation_type": "timestamp_format_changed", "skipped": "no_timestamp_fields"}]

        cfg = get_source_config(source_name)
        delimiter = cfg.get("delimiter", "|")
        col_indices = self._get_col_indices(source_name, ts_fields)

        mutations: list[dict[str, Any]] = []
        for filepath in self._resolve_files(src_dir, source_name, batch_id):
            lines = filepath.read_text(encoding="utf-8").splitlines(keepends=True)
            new_lines: list[str] = []
            for line in lines:
                stripped = line.rstrip("\n\r")
                fields = stripped.split(delimiter)
                for idx in col_indices:
                    if idx < len(fields):
                        val = fields[idx].strip()
                        # Try to parse with original format (date portion only)
                        try:
                            date_part = val[:len("YYYY-MM-DD")]
                            dt = datetime.strptime(date_part, "%Y-%m-%d")
                            fields[idx] = dt.strftime(new_format)
                        except (ValueError, IndexError):
                            pass  # Non-parseable values left as-is
                new_lines.append(delimiter.join(fields) + "\n")
            filepath.write_text("".join(new_lines), encoding="utf-8")
            mutations.append({
                "mutation_type": "timestamp_format_changed",
                "source": source_name,
                "affected_fields": ts_fields,
                "new_format": new_format,
                "expected_detection": "timestamp_parse_error",
                "expected_stage": "bronze_validation",
                "recoverable": False,
                "recovery_hint": "quarantine",
            })
        return mutations

    def _inject_timestamp_granularity_changed(
        self,
        src_dir: Path,
        source_name: str,
        batch_id: str,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Truncate datetime fields to date-only (removes time component)."""
        meta = get_semantic_meta(source_name)
        ts_fields = meta.get("timestamp_fields", [])
        if not ts_fields:
            return [{"mutation_type": "timestamp_granularity_changed", "skipped": "no_timestamp_fields"}]

        cfg = get_source_config(source_name)
        delimiter = cfg.get("delimiter", "|")
        col_indices = self._get_col_indices(source_name, ts_fields)

        mutations: list[dict[str, Any]] = []
        for filepath in self._resolve_files(src_dir, source_name, batch_id):
            lines = filepath.read_text(encoding="utf-8").splitlines(keepends=True)
            new_lines = []
            for line in lines:
                stripped = line.rstrip("\n\r")
                fields = stripped.split(delimiter)
                for idx in col_indices:
                    if idx < len(fields) and len(fields[idx].strip()) > 10:
                        # Keep only YYYY-MM-DD (first 10 chars)
                        fields[idx] = fields[idx].strip()[:10]
                new_lines.append(delimiter.join(fields) + "\n")
            filepath.write_text("".join(new_lines), encoding="utf-8")
            mutations.append({
                "mutation_type": "timestamp_granularity_changed",
                "source": source_name,
                "affected_fields": ts_fields,
                "expected_detection": "semantic_anomaly",
                "expected_stage": "semantic_validation",
                "recoverable": False,
                "recovery_hint": "manual_correction",
            })
        return mutations

    def _inject_pre_aggregate_records(
        self,
        src_dir: Path,
        source_name: str,
        batch_id: str,
        *,
        group_by: list[str] | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Group and sum records to simulate different aggregation levels."""
        meta = get_semantic_meta(source_name)
        amount_fields = meta.get("amount_fields", []) + meta.get("volume_fields", [])
        cfg = get_source_config(source_name)
        delimiter = cfg.get("delimiter", "|")
        columns = cfg.get("columns", [])
        key_fields = group_by or meta.get("entity_id_fields", columns[:1])

        if not amount_fields or not key_fields:
            return [{"mutation_type": "pre_aggregate_records", "skipped": "no_fields"}]

        key_indices = self._get_col_indices(source_name, key_fields)
        amt_indices = self._get_col_indices(source_name, amount_fields)

        mutations: list[dict[str, Any]] = []
        for filepath in self._resolve_files(src_dir, source_name, batch_id):
            lines = filepath.read_text(encoding="utf-8").splitlines()
            groups: dict[tuple, list[str]] = {}
            for line in lines:
                if not line.strip():
                    continue
                fields = line.split(delimiter)
                key = tuple(fields[i] if i < len(fields) else "" for i in key_indices)
                groups.setdefault(key, fields)
                # Sum amount fields
                for amt_idx in amt_indices:
                    if amt_idx < len(groups[key]) and amt_idx < len(fields):
                        try:
                            prev = groups[key][amt_idx]
                            cur = fields[amt_idx]
                            existing = float(prev)
                            adding = float(cur)
                            # Preserve integer representation to avoid _to_int failures
                            if "." not in prev and "." not in cur:
                                groups[key][amt_idx] = str(int(existing + adding))
                            else:
                                groups[key][amt_idx] = str(existing + adding)
                        except ValueError:
                            pass

            new_lines = [delimiter.join(v) for v in groups.values()]
            filepath.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
            mutations.append({
                "mutation_type": "pre_aggregate_records",
                "source": source_name,
                "group_by": key_fields,
                "summed_fields": amount_fields,
                "original_records": len(lines),
                "aggregated_records": len(groups),
                "expected_detection": "semantic_anomaly",
                "expected_stage": "semantic_validation",
                "recoverable": False,
                "recovery_hint": "manual_correction",
            })
        return mutations


__all__ = ["SemanticInjector", "get_semantic_meta"]
