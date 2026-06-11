"""
Scenario Builder — compose multiple injectors into a single scenario.

Combines row-level (SourceInjector), schema-level (SchemaInjector),
and reliability (ReliabilityInjector) mutations into one scenario directory
with a unified injection_manifest.json.

Usage::

    from ingestion.tpcdi.error_injection.scenario_builder import ScenarioBuilder
    from ingestion.tpcdi.error_injection.source_injector import TpcdiSourceInjector
    from ingestion.tpcdi.error_injection.reliability_injector import ReliabilityInjector

    builder = ScenarioBuilder("compound_001", seed=42)
    builder.add_source("trade", "batch1", "type_error")
    builder.add_reliability("trade", "batch1", "partial_file")
    scenario_root = builder.build()
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from common.tpcdi_sources import source_root

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCENARIO_BASE = Path("runtime/tpcdi/scenarios")


class ScenarioBuilder:
    """Compose multiple injectors into one scenario directory."""

    def __init__(self, scenario_id: str, seed: int = 42):
        self.scenario_id = scenario_id
        self.seed = seed
        self._steps: list[dict[str, Any]] = []

    # ── Step registration ────────────────────────────────────────────────────

    def add_source(
        self,
        target_source: str,
        batch_id: str = "batch1",
        mutation_type: str = "missing_field",
        *,
        line_numbers: list[int] | None = None,
        field_index: int | None = None,
    ) -> "ScenarioBuilder":
        """Register a row-level source mutation."""
        self._steps.append({
            "injector": "source",
            "target_source": target_source,
            "batch_id": batch_id,
            "mutation_type": mutation_type,
            "line_numbers": line_numbers,
            "field_index": field_index,
        })
        return self

    def add_reliability(
        self,
        target_source: str,
        batch_id: str = "batch1",
        mutation_type: str = "partial_file",
        **kwargs: Any,
    ) -> "ScenarioBuilder":
        """Register a reliability mutation."""
        self._steps.append({
            "injector": "reliability",
            "target_source": target_source,
            "batch_id": batch_id,
            "mutation_type": mutation_type,
            **kwargs,
        })
        return self

    # ── Build ────────────────────────────────────────────────────────────────

    def build(self) -> Path:
        """Run all registered steps and write a combined manifest.

        Returns the scenario root path.
        """
        from ingestion.tpcdi.error_injection.source_injector import TpcdiSourceInjector
        from ingestion.tpcdi.error_injection.reliability_injector import ReliabilityInjector

        scenario_root_dir = PROJECT_ROOT / SCENARIO_BASE / self.scenario_id
        source_dir = scenario_root_dir / "source"

        # Always start from a clean copy
        if source_dir.exists():
            shutil.rmtree(source_dir)
        shutil.copytree(source_root(), source_dir)

        all_mutations: list[dict[str, Any]] = []

        src_injector = TpcdiSourceInjector(seed=self.seed)
        rel_injector = ReliabilityInjector(seed=self.seed)

        for step in self._steps:
            injector_type = step["injector"]

            if injector_type == "source":
                # Re-use _mutate_line logic without re-copying the source tree.
                # We call _apply_source_step which mutates files in source_dir directly.
                muts = self._apply_source_step(src_injector, source_dir, step)
                all_mutations.extend(muts)

            elif injector_type == "reliability":
                method_name = f"_inject_{step['mutation_type']}"
                method = getattr(rel_injector, method_name, None)
                if method is None:
                    raise ValueError(f"Unknown reliability mutation: {step['mutation_type']!r}")
                kwargs = {k: v for k, v in step.items()
                          if k not in ("injector", "target_source", "batch_id", "mutation_type")}
                muts = method(source_dir, step["target_source"], step["batch_id"], **kwargs)
                all_mutations.extend(muts)

        manifest = {
            "scenario_id": self.scenario_id,
            "seed": self.seed,
            "base_source_root": str(source_root()),
            "scenario_root": str(scenario_root_dir),
            "scenario_source_root": str(source_dir),
            "step_count": len(self._steps),
            "mutations": all_mutations,
        }
        manifest_path = scenario_root_dir / "injection_manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return scenario_root_dir

    # ── Internal ─────────────────────────────────────────────────────────────

    def _apply_source_step(
        self,
        injector: Any,
        source_dir: Path,
        step: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Apply a source-level mutation directly to source_dir (no re-copy)."""
        import hashlib
        from common.tpcdi_sources import get_source_config

        target_source = step["target_source"]
        batch_id = step["batch_id"]
        mutation_type = step["mutation_type"]
        line_numbers: list[int] | None = step.get("line_numbers")
        field_index: int | None = step.get("field_index")

        src_cfg = get_source_config(target_source)
        columns = src_cfg.get("columns", [])
        delimiter = src_cfg.get("delimiter", "|")
        batch_path_name = {
            "batch1": "Batch1", "batch2": "Batch2", "batch3": "Batch3"
        }.get(batch_id, batch_id.capitalize())
        batch_dir = source_dir / batch_path_name

        exclude = src_cfg.get("exclude_patterns", [])
        target_files: list[Path] = []
        for pattern in src_cfg.get("files", []):
            for f in sorted(batch_dir.glob(pattern)):
                if f.is_file() and not any(f.match(ex) for ex in exclude):
                    target_files.append(f)

        mutations: list[dict[str, Any]] = []
        mut_counter = 0

        for filepath in target_files:
            lines = filepath.read_text(encoding="utf-8").splitlines(keepends=True)
            total_lines = len(lines)
            if line_numbers:
                lnums = [ln for ln in line_numbers if 1 <= ln <= total_lines]
            else:
                count = min(3, total_lines)
                lnums = sorted(injector.rng.sample(range(1, total_lines + 1), count))

            for phys_ln in lnums:
                mut_counter += 1
                idx = phys_ln - 1
                original_line = lines[idx].rstrip("\n\r")
                if mutation_type == "duplicate_pk":
                    lines.insert(idx + 1, original_line + "\n")
                record_hash_before = hashlib.sha256(original_line.encode()).hexdigest()[:16]
                mutated_line, meta = injector._mutate_line(
                    original_line, mutation_type, delimiter, field_index, phys_ln, columns
                )
                lines[idx] = mutated_line + "\n"
                if meta.get("skipped"):
                    continue
                record_hash_after = hashlib.sha256(mutated_line.encode()).hexdigest()[:16]
                meta.update({
                    "mutation_id": f"mut-{mut_counter:06d}",
                    "source_name": target_source,
                    "batch_id": batch_id,
                    "relative_file": filepath.name,
                    "physical_line_number": phys_ln,
                    "record_hash_before": record_hash_before,
                    "record_hash_after": record_hash_after,
                    "mutation_type": mutation_type,
                })
                mutations.append(meta)
            filepath.write_text("".join(lines), encoding="utf-8")

        return mutations


__all__ = ["ScenarioBuilder"]
