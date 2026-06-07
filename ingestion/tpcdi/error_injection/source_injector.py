"""
TPC-DI Source-Layer Error Injection — deterministic, file-level mutations.

Copies clean DIGen source files into a scenario directory, injects errors
at the file level (CSV/TXT line mutations), and records a manifest.

Usage::

    from ingestion.tpcdi.error_injection.source_injector import TpcdiSourceInjector

    injector = TpcdiSourceInjector()
    scenario_root = injector.create_scenario("missing_field", target="trade", batch="batch1", line_numbers=[100, 200])
    print(scenario_root)
"""

from __future__ import annotations

import json
import os
import random
import shutil
from pathlib import Path
from typing import Any

from common.tpcdi_sources import source_root, load_tpcdi_sources_config, resolve_batch_path

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

        Parameters
        ----------
        scenario_id:
            Unique name for this scenario (e.g. ``malformed_trade_001``).
        target_source:
            Source name from tpcdi_sources.yml (e.g. ``trade``, ``daily_market``).
        batch_id:
            batch1, batch2, or batch3.
        line_numbers:
            1-indexed line numbers to mutate (excluding header lines).
            If None, picks ``random.sample`` from available lines.
        mutation_type:
            One of: ``missing_field``, ``extra_field``, ``type_error``, ``duplicate_pk``.
        field_index:
            0-indexed field position for mutations that target a specific field.
            If None and mutation is field-level, picks a random index.

        Returns
        -------
        Path to the scenario source_root (``runtime/tpcdi/scenarios/{scenario_id}/``).
        """
        from common.tpcdi_sources import get_source_config

        src_cfg = get_source_config(target_source)
        clean_root = source_root()
        scenario_root = PROJECT_ROOT / SCENARIO_BASE / scenario_id
        batch_dir = scenario_root / resolve_batch_path(batch_id).relative_to(source_root())

        # Copy batch directory
        clean_batch = resolve_batch_path(batch_id)
        if clean_batch.exists():
            shutil.copytree(clean_batch, batch_dir, dirs_exist_ok=True)

        # Copy root-level reports
        for f in clean_root.glob("*.txt"):
            shutil.copy2(f, scenario_root)
        for f in clean_root.glob("*_audit.csv"):
            shutil.copy2(f, scenario_root)
        for f in clean_root.glob("Generator_audit.csv"):
            shutil.copy2(f, scenario_root)

        # Find the target file
        target_files = list_source_files(target_source, batch_id)
        mutations: list[dict[str, Any]] = []

        for filepath in target_files:
            relative = filepath.relative_to(clean_root)
            scenario_file = scenario_root / relative

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

            for ln in lnums:
                idx = ln - 1
                original = lines[idx].rstrip("\n\r")
                mutated, meta = self._mutate_line(
                    original, mutation_type, delimiter, field_index, ln
                )
                lines[idx] = mutated + "\n"
                mutations.append(meta)

            scenario_file.write_text("".join(lines), encoding="utf-8")

        # Write manifest
        manifest = {
            "scenario_id": scenario_id,
            "base_source_root": str(clean_root),
            "target_source": target_source,
            "batch": batch_id,
            "seed": self.seed,
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
    ) -> tuple[str, dict[str, Any]]:
        fields = line.split(delimiter)
        meta: dict[str, Any] = {
            "line_number": line_number,
            "type": mutation_type,
            "original": line,
        }

        if mutation_type == "missing_field":
            if len(fields) <= 1:
                return line, {**meta, "skipped": "only_one_field"}
            fi = field_index if field_index is not None and field_index < len(fields) else self.rng.randint(0, len(fields) - 1)
            removed = fields.pop(fi)
            meta["field_index"] = fi
            meta["removed_value"] = removed
            meta["expected_detection"] = "field_count_mismatch"

        elif mutation_type == "extra_field":
            fi = field_index if field_index is not None and field_index <= len(fields) else self.rng.randint(0, len(fields))
            extra = f"EXTRA{self.rng.randint(100, 999)}"
            fields.insert(fi, extra)
            meta["field_index"] = fi
            meta["extra_value"] = extra
            meta["expected_detection"] = "field_count_mismatch"

        elif mutation_type == "type_error":
            numeric_candidates = [i for i, f in enumerate(fields) if f.strip().lstrip("-").isdigit()]
            if not numeric_candidates:
                return line, {**meta, "skipped": "no_numeric_field"}
            fi = field_index if field_index is not None and field_index in numeric_candidates else self.rng.choice(numeric_candidates)
            original = fields[fi]
            fields[fi] = "NOT_A_NUMBER"
            meta["field_index"] = fi
            meta["original_value"] = original
            meta["replacement"] = "NOT_A_NUMBER"
            meta["expected_detection"] = "type_coercion_error"

        elif mutation_type == "duplicate_pk":
            # Duplicate the entire line
            fields_copy = list(fields)
            meta["expected_detection"] = "duplicate_primary_key"
            return line + delimiter.join(fields_copy) + "\n", meta

        else:
            raise ValueError(f"Unknown mutation_type: {mutation_type}")

        return delimiter.join(fields), meta


PROJECT_ROOT = Path(__file__).resolve().parents[3]
from common.tpcdi_sources import list_source_files

__all__ = ["TpcdiSourceInjector"]
