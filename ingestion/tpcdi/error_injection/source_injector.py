"""
TPC-DI Source-Layer Error Injection — deterministic, file-level mutations.

Copies clean DIGen source files into a scenario directory, injects errors
at the file level (CSV/TXT line mutations), and records a manifest.

Usage::

    from ingestion.tpcdi.error_injection.source_injector import TpcdiSourceInjector

    injector = TpcdiSourceInjector()
    scenario_root = injector.create_scenario(
        "missing_field_trade_001",
        target_source="trade", batch_id="batch1",
        mutation_type="missing_field",
        line_numbers=[100, 200],
    )
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import shutil
from pathlib import Path
from typing import Any

from common.tpcdi_sources import source_root, resolve_batch_path, get_source_config, list_source_files

SCENARIO_BASE = Path("runtime/tpcdi/scenarios")


class TpcdiSourceInjector:
    """Deterministic source-file error injector for TPC-DI DIGen data."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        self.rng = random.Random(seed)

    def create_scenario(
        self,
        scenario_id: str,
        *,
        target_source: str,
        batch_id: str = "batch1",
        line_numbers: list[int] | None = None,
        mutation_type: str = "missing_field",
        field_index: int | None = None,
    ) -> Path:
        """Create a scenario directory with injected errors.

        Returns
        -------
        Path to the scenario root (``runtime/tpcdi/scenarios/{scenario_id}/``).
        """
        clean_root = source_root()
        scenario_root = PROJECT_ROOT / SCENARIO_BASE / scenario_id
        source_dir = scenario_root / "source"
        batch_dir = source_dir / resolve_batch_path(batch_id).relative_to(clean_root)

        # Copy full clean source root (all batches + reports)
        if source_dir.exists():
            shutil.rmtree(source_dir)
        shutil.copytree(clean_root, source_dir)

        # Find target files
        src_cfg = get_source_config(target_source)
        target_files = list_source_files(target_source, batch_id)
        mutations: list[dict[str, Any]] = []
        mut_counter = 0
        columns = src_cfg.get("columns", [])

        for filepath in target_files:
            relative = filepath.relative_to(clean_root)
            scenario_file = source_dir / relative
            if not scenario_file.exists():
                continue

            lines = scenario_file.read_text(encoding="utf-8").splitlines(keepends=True)
            total_lines = len(lines)
            delimiter = src_cfg.get("delimiter", "|")

            if line_numbers:
                lnums = [ln for ln in line_numbers if 1 <= ln <= total_lines]
            else:
                count = min(3, total_lines)
                lnums = sorted(self.rng.sample(range(1, total_lines + 1), count))

            for phys_ln in lnums:
                mut_counter += 1
                idx = phys_ln - 1
                original_line = lines[idx].rstrip("\n\r")

                if mutation_type == "duplicate_pk":
                    # Insert a separate duplicate line after the original
                    lines.insert(idx + 1, original_line + "\n")
                logical_rn = phys_ln  # simplified; real blank-skip handled later

                record_hash_before = hashlib.sha256(original_line.encode()).hexdigest()[:16]

                mutated_line, meta = self._mutate_line(
                    original_line, mutation_type, delimiter, field_index, phys_ln, columns
                )
                lines[idx] = mutated_line + "\n"

                if meta.get("skipped"):
                    continue  # don't add skipped mutations to manifest

                record_hash_after = hashlib.sha256(mutated_line.encode()).hexdigest()[:16]
                meta.update({
                    "mutation_id": f"mut-{mut_counter:06d}",
                    "source_name": target_source,
                    "batch_id": batch_id,
                    "relative_file": str(relative),
                    "physical_line_number": phys_ln,
                    "logical_record_number": logical_rn,
                    "record_hash_before": record_hash_before,
                    "record_hash_after": record_hash_after,
                    "mutation_type": mutation_type,
                })
                mutations.append(meta)

            scenario_file.write_text("".join(lines), encoding="utf-8")

        manifest = {
            "scenario_id": scenario_id,
            "run_id": "",
            "seed": self.seed,
            "base_source_root": str(clean_root),
            "scenario_root": str(scenario_root),
            "scenario_source_root": str(source_dir),
            "recovered_source_root": str(scenario_root / "recovered_source"),
            "target_source": target_source,
            "batch": batch_id,
            "mutation_type": mutation_type,
            "mutations": mutations,
        }
        manifest_path = scenario_root / "injection_manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        return scenario_root

    def _mutate_line(
        self,
        line: str,
        mutation_type: str,
        delimiter: str,
        field_index: int | None,
        line_number: int,
        columns: list[str] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        fields = line.split(delimiter)
        meta: dict[str, Any] = {
            "target_field": None,
            "original_value": None,
            "mutated_value": None,
            "expected_detection": None,
            "expected_stage": None,
            "recoverable": None,
            "recovery_hint": None,
        }

        if mutation_type == "missing_field":
            if len(fields) <= 1:
                return line, {**meta, "skipped": "only_one_field"}
            fi = field_index if field_index is not None and field_index < len(fields) else self.rng.randint(0, len(fields) - 1)
            removed = fields.pop(fi)
            meta.update({
                "target_field": columns[fi] if columns and fi < len(columns) else f"col_{fi}",
                "original_value": removed,
                "mutated_value": "(removed)",
                "expected_detection": "field_count_mismatch",
                "expected_stage": "bronze_validation",
                "recoverable": True,
                "recovery_hint": "infer_missing_field",
            })

        elif mutation_type == "extra_field":
            fi = field_index if field_index is not None and field_index <= len(fields) else self.rng.randint(0, len(fields))
            extra = f"EXTRA{self.rng.randint(100, 999)}"
            fields.insert(fi, extra)
            meta.update({
                "target_field": columns[fi] if columns and fi < len(columns) else f"col_{fi}",
                "original_value": None,
                "mutated_value": extra,
                "expected_detection": "field_count_mismatch",
                "expected_stage": "bronze_validation",
                "recoverable": True,
                "recovery_hint": "drop_extra_field",
            })

        elif mutation_type == "type_error":
            numeric_candidates = [i for i, f in enumerate(fields) if f.strip().lstrip("-").replace(".", "").isdigit()]
            if not numeric_candidates:
                return line, {**meta, "skipped": "no_numeric_field"}
            fi = field_index if field_index is not None and field_index in numeric_candidates else self.rng.choice(numeric_candidates)
            original = fields[fi]
            fields[fi] = "NOT_A_NUMBER"
            meta.update({
                "target_field": columns[fi] if columns and fi < len(columns) else f"col_{fi}",
                "original_value": original,
                "mutated_value": "NOT_A_NUMBER",
                "expected_detection": "type_coercion_error",
                "expected_stage": "bronze_validation",
                "recoverable": True,
                "recovery_hint": "safe_cast",
            })

        elif mutation_type == "duplicate_pk":
            meta.update({
                "target_field": "all",
                "original_value": line,
                "mutated_value": "(duplicated line)",
                "expected_detection": "duplicate_primary_key",
                "expected_stage": "gold_audit",
                "recoverable": True,
                "recovery_hint": "dedup",
            })
            return line, meta  # line unchanged; duplicate is already inserted in create_scenario()

        else:
            raise ValueError(f"Unknown mutation_type: {mutation_type}")

        return delimiter.join(fields), meta


PROJECT_ROOT = Path(__file__).resolve().parents[3]

__all__ = ["TpcdiSourceInjector"]
