"""
TPC-DI Semantic Error Injector.

Applies column-level semantic transformations to a scenario copy of the source
data.  These mutations are syntactically valid — parsers pass without error —
but produce analytically wrong values.  Detection requires semantic validation
rules (Phase 3 quality pipeline) or Gold-level ground-truth comparison.

Supported mutation_types:
  unit_changed                     — Multiply ``amount_fields`` by a conversion factor.
  timestamp_format_changed         — Reformat ``timestamp_fields`` (YYYY-MM-DD → DD/MM/YYYY).
  timestamp_granularity_changed    — Truncate datetime to date-only (lose time precision).
  entity_id_ambiguity              — Duplicate an entity with a different technical ID.
  rename_to_synonym                — Rename a field to a synonym in ``semantic_meta``.
  pre_aggregate_records            — Group-by key fields and sum amount fields.
  same_name_different_meaning      — Swap values between two identically-named fields
                                      that carry different business semantics.
  different_business_definitions   — Apply a formula shift (e.g. net_price → gross_price).
  different_spatial_ref            — Inject a spatial CRS field or modify coordinate values.
  cross_source_inconsistency       — Modify values to create conflicts between sources.

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
                "batch_id": batch_id,
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
                "batch_id": batch_id,
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
                "batch_id": batch_id,
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

    # ── New mutation types (extended taxonomy) ───────────────────────────

    def _inject_rename_to_synonym(
        self,
        src_dir: Path,
        source_name: str,
        batch_id: str,
        *,
        target_field: str | None = None,
        synonym: str | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Rename a field column and all its occurrences in source files.

        Picks a random field name and replaces it with a known synonym
        (e.g. ``price`` → ``cost``, ``quantity`` → ``volume``, ``date`` → ``day``).
        """
        cfg = get_source_config(source_name)
        columns = list(cfg.get("columns", []))
        if not columns:
            return [{"mutation_type": "rename_to_synonym", "skipped": "no_columns"}]

        synonym_map = {
            "price": "cost", "quantity": "volume", "amount": "sum",
            "date": "day", "time": "moment", "id": "uid",
            "name": "label", "type": "category", "status": "state",
            "value": "score", "rate": "ratio", "fee": "charge",
        }
        if target_field and target_field in columns:
            original = target_field
        else:
            candidates = [c for c in columns if any(k in c.lower() for k in synonym_map)]
            if not candidates:
                candidates = columns
            original = self.rng.choice(candidates)

        syn = synonym or synonym_map.get(original.lower(), original + "_renamed")
        delimiter = cfg.get("delimiter", "|")
        col_idx = columns.index(original)

        mutations: list[dict[str, Any]] = []
        for filepath in self._resolve_files(src_dir, source_name, batch_id):
            lines = filepath.read_text(encoding="utf-8").splitlines()
            for i, line in enumerate(lines):
                fields = line.split(delimiter)
                if col_idx < len(fields):
                    fields[col_idx] = syn  # replace column value with synonym marker
                    lines[i] = delimiter.join(fields)
            filepath.write_text("\n".join(lines) + "\n" if lines else "", encoding="utf-8")
            mutations.append({
                "mutation_type": "rename_to_synonym",
                "source": source_name,
                "batch_id": batch_id,
                "original_field": original,
                "synonym": syn,
                "expected_detection": "field_rename_candidate",
                "expected_stage": "bronze_validation",
                "recoverable": True,
                "recovery_hint": "field_alias_lookup",
            })
        return mutations

    def _inject_entity_id_ambiguity(
        self,
        src_dir: Path,
        source_name: str,
        batch_id: str,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Duplicate an entity record with a slightly different technical ID.

        Copies a random record and changes its entity ID, simulating the case
        where two different technical IDs refer to the same real-world entity
        but the system cannot resolve them.
        """
        meta = get_semantic_meta(source_name)
        id_fields = meta.get("entity_id_fields", [])
        cfg = get_source_config(source_name)
        delimiter = cfg.get("delimiter", "|")
        columns = cfg.get("columns", [])

        if not id_fields:
            id_fields = [c for c in columns[:3] if "id" in c.lower() or "sym" in c.lower()]
        if not id_fields:
            return [{"mutation_type": "entity_id_ambiguity", "skipped": "no_id_fields"}]

        id_idx = columns.index(id_fields[0]) if id_fields[0] in columns else 0

        mutations: list[dict[str, Any]] = []
        for filepath in self._resolve_files(src_dir, source_name, batch_id):
            lines = filepath.read_text(encoding="utf-8").splitlines()
            if len(lines) < 2:
                continue
            # Pick a random non-empty line to duplicate
            candidates = [i for i, line in enumerate(lines) if line.strip()]
            if not candidates:
                continue
            dup_idx = self.rng.choice(candidates)
            original_line = lines[dup_idx].rstrip("\n\r")
            fields = original_line.split(delimiter)

            # Modify the ID field slightly
            if id_idx < len(fields):
                orig_id = fields[id_idx]
                # Append a suffix to create ambiguity
                fields[id_idx] = orig_id + "_AMBIGUOUS"
            mutated_line = delimiter.join(fields)

            # Insert the mutated duplicate after the original
            lines.insert(dup_idx + 1, mutated_line)
            filepath.write_text("\n".join(lines) + "\n", encoding="utf-8")
            mutations.append({
                "mutation_type": "entity_id_ambiguity",
                "source": source_name,
                "batch_id": batch_id,
                "id_field": id_fields[0],
                "original_id": orig_id if id_idx < len(fields) else "",
                "ambiguous_id": fields[id_idx] if id_idx < len(fields) else "",
                "dup_line_number": dup_idx + 1,
                "expected_detection": "entity_resolution_problem",
                "expected_stage": "silver_validation",
                "recoverable": False,
                "recovery_hint": "entity_resolution_engine",
            })
        return mutations

    def _inject_same_name_different_meaning(
        self,
        src_dir: Path,
        source_name: str,
        batch_id: str,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Swap numeric values between two fields that share a common keyword
        in their names but have different business meanings.

        Example: swap ``trade_price`` ↔ ``cash_amount`` — both are amounts
        but represent different concepts (unit price vs total cash).
        """
        cfg = get_source_config(source_name)
        columns = cfg.get("columns", [])
        delimiter = cfg.get("delimiter", "|")

        # Find pairs of columns sharing a keyword but with different roles
        keyword_groups: dict[str, list[int]] = {}
        keywords = ["price", "amount", "value", "fee", "commission", "tax", "rate", "quantity", "volume"]
        for kw in keywords:
            matching = [i for i, c in enumerate(columns) if kw in c.lower()]
            if len(matching) >= 2:
                keyword_groups[kw] = matching

        if not keyword_groups:
            # Fallback: pick any two numeric-ish columns
            all_idx = list(range(min(len(columns), 6)))
            if len(all_idx) >= 2:
                a, b = self.rng.sample(all_idx, 2)
                keyword_groups["generic"] = [a, b]
            else:
                return [{"mutation_type": "same_name_different_meaning", "skipped": "no_col_pair"}]

        kw = self.rng.choice(list(keyword_groups.keys()))
        idx_pair = keyword_groups[kw]
        a_idx, b_idx = idx_pair[0], idx_pair[1]

        mutations: list[dict[str, Any]] = []
        for filepath in self._resolve_files(src_dir, source_name, batch_id):
            lines = filepath.read_text(encoding="utf-8").splitlines()
            for i, line in enumerate(lines):
                fields = line.split(delimiter)
                if a_idx < len(fields) and b_idx < len(fields):
                    fields[a_idx], fields[b_idx] = fields[b_idx], fields[a_idx]
                lines[i] = delimiter.join(fields)
            filepath.write_text("\n".join(lines) + "\n" if lines else "", encoding="utf-8")
            mutations.append({
                "mutation_type": "same_name_different_meaning",
                "source": source_name,
                "batch_id": batch_id,
                "swapped_fields": [columns[a_idx], columns[b_idx]],
                "keyword": kw,
                "expected_detection": "semantic_anomaly",
                "expected_stage": "semantic_validation",
                "recoverable": False,
                "recovery_hint": "field_semantic_validation",
            })
        return mutations

    def _inject_different_business_definitions(
        self,
        src_dir: Path,
        source_name: str,
        batch_id: str,
        *,
        formula: str = "net_to_gross",
        tax_rate: float = 0.10,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Apply a formula shift to numeric fields simulating different
        business definition (e.g. net_price → gross_price by adding tax).

        Supported formulas:
          net_to_gross — Multiply amount fields by (1 + tax_rate)
          gross_to_net — Divide amount fields by (1 + tax_rate)
          add_markup   — Multiply by 1.5
        """
        meta = get_semantic_meta(source_name)
        amount_fields = meta.get("amount_fields", [])
        if not amount_fields:
            return [{"mutation_type": "different_business_definitions", "skipped": "no_amount_fields"}]

        cfg = get_source_config(source_name)
        delimiter = cfg.get("delimiter", "|")
        col_indices = self._get_col_indices(source_name, amount_fields)

        if formula == "net_to_gross":
            factor = 1 + tax_rate
        elif formula == "gross_to_net":
            factor = 1 / (1 + tax_rate)
        elif formula == "add_markup":
            factor = 1.5
        else:
            factor = 1 + tax_rate

        mutations: list[dict[str, Any]] = []
        for filepath in self._resolve_files(src_dir, source_name, batch_id):
            lines = filepath.read_text(encoding="utf-8").splitlines()
            new_lines = []
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
                new_lines.append(delimiter.join(fields))
            filepath.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
            mutations.append({
                "mutation_type": "different_business_definitions",
                "source": source_name,
                "batch_id": batch_id,
                "formula": formula,
                "factor": factor,
                "affected_fields": amount_fields,
                "expected_detection": "semantic_anomaly",
                "expected_stage": "semantic_validation",
                "recoverable": False,
                "recovery_hint": "reapply_business_rules",
            })
        return mutations

    def _inject_different_spatial_ref(
        self,
        src_dir: Path,
        source_name: str,
        batch_id: str,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Inject a spatial Coordinate Reference System (CRS) field or modify
        coordinate values to simulate spatial reference system mismatch.

        Since TPC-DI has no spatial data, this injector adds a synthetic
        ``_crs`` field and/or modifies mock coordinate fields.
        """
        cfg = get_source_config(source_name)
        delimiter = cfg.get("delimiter", "|")

        crs_values = ["EPSG:4326", "EPSG:3857", "EPSG:27700", "EPSG:32633"]
        selected_crs = self.rng.choice(crs_values)

        mutations: list[dict[str, Any]] = []
        for filepath in self._resolve_files(src_dir, source_name, batch_id):
            lines = filepath.read_text(encoding="utf-8").splitlines()
            new_lines = []
            for line in lines:
                stripped = line.rstrip("\n\r")
                if not stripped:
                    new_lines.append(line)
                    continue
                # Append CRS field to each record
                new_lines.append(stripped + delimiter + selected_crs)
            filepath.write_text("\n".join(new_lines) + "\n" if new_lines else "", encoding="utf-8")
            mutations.append({
                "mutation_type": "different_spatial_ref",
                "source": source_name,
                "batch_id": batch_id,
                "injected_crs": selected_crs,
                "expected_detection": "semantic_anomaly",
                "expected_stage": "semantic_validation",
                "recoverable": False,
                "recovery_hint": "reproject_coordinates",
            })
        return mutations

    def _inject_cross_source_inconsistency(
        self,
        src_dir: Path,
        source_name: str,
        batch_id: str,
        *,
        conflict_source: str | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Modify values in the current source to create inconsistency with
        another source that shares a common identifier.

        Picks a second source (e.g. ``trade`` vs ``cash_transaction``) that
        shares entity IDs and negates or scales amount values in the current
        source so the two sources no longer agree on the same entity.
        """
        cfg = get_source_config(source_name)
        delimiter = cfg.get("delimiter", "|")

        # Pick a conflict source
        conflict_candidates = []
        for sn, meta in _SEMANTIC_META.items():
            if sn != source_name:
                conflict_candidates.append(sn)
        conflict = conflict_source or (self.rng.choice(conflict_candidates) if conflict_candidates else None)

        # Find shared ID fields
        meta = get_semantic_meta(source_name)
        amount_fields = meta.get("amount_fields", []) or meta.get("volume_fields", [])

        if not amount_fields:
            return [{"mutation_type": "cross_source_inconsistency", "skipped": "no_amount_fields"}]

        amt_indices = self._get_col_indices(source_name, amount_fields)

        mutations: list[dict[str, Any]] = []
        for filepath in self._resolve_files(src_dir, source_name, batch_id):
            lines = filepath.read_text(encoding="utf-8").splitlines()
            new_lines = []
            for line in lines:
                stripped = line.rstrip("\n\r")
                fields = stripped.split(delimiter)
                for idx in amt_indices:
                    if idx < len(fields):
                        s = fields[idx].strip()
                        if s.lstrip("-").replace(".", "").isdigit():
                            try:
                                orig = float(s)
                                # Negate or zero out to create inconsistency
                                if orig > 0:
                                    if "." in s:
                                        fields[idx] = str(-orig)
                                    else:
                                        fields[idx] = str(-abs(int(orig)))
                            except ValueError:
                                pass
                new_lines.append(delimiter.join(fields))
            filepath.write_text("\n".join(new_lines) + "\n" if new_lines else "", encoding="utf-8")
            mutations.append({
                "mutation_type": "cross_source_inconsistency",
                "source": source_name,
                "batch_id": batch_id,
                "conflict_source": conflict,
                "affected_fields": amount_fields,
                "expected_detection": "cross_source_inconsistency",
                "expected_stage": "gold_audit",
                "recoverable": False,
                "recovery_hint": "reconciliation_engine",
            })
        return mutations


__all__ = ["SemanticInjector", "get_semantic_meta"]
