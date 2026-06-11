"""
TPC-DI Reliability & Failure Error Injector.

Injects file/batch-level faults into a scenario copy of the DIGen source tree.
Each call to ``create_scenario()`` copies the clean source, applies one mutation,
and writes an ``injection_manifest.json``.

Supported mutation_types:
  partial_file              — Truncate a source file mid-record.
  duplicate_batch           — Copy a batch directory (simulate non-idempotent retry).
  poison_record             — Insert a binary/non-UTF-8 line that breaks parsers.
  missing_batch             — Delete the target batch directory entirely.
  rate_limit_partial_batch  — Keep only the first N% of records (simulate rate limit).
  atomic_write_failure      — Write only partial content to simulate crashed write.

Usage::

    from ingestion.tpcdi.error_injection.reliability_injector import ReliabilityInjector

    ri = ReliabilityInjector(seed=42)
    scenario_root = ri.create_scenario(
        "reliability_partial_001",
        target_source="trade",
        batch_id="batch1",
        mutation_type="partial_file",
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


class ReliabilityInjector:
    """File/batch-level reliability fault injector for TPC-DI DIGen data."""

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
        """Create a scenario directory with one reliability fault injected.

        Returns
        -------
        Path to the scenario root (``runtime/tpcdi/scenarios/{scenario_id}/``).
        """
        clean_root = source_root()
        scenario_root_dir = PROJECT_ROOT / SCENARIO_BASE / scenario_id
        source_dir = scenario_root_dir / "source"

        if source_dir.exists():
            shutil.rmtree(source_dir)
        shutil.copytree(clean_root, source_dir)

        method = getattr(self, f"_inject_{mutation_type}", None)
        if method is None:
            raise ValueError(f"Unknown reliability mutation_type: {mutation_type!r}")

        mutations = method(source_dir, target_source, batch_id, **kwargs)

        manifest = {
            "scenario_id": scenario_id,
            "seed": self.seed,
            "base_source_root": str(clean_root),
            "scenario_root": str(scenario_root_dir),
            "scenario_source_root": str(source_dir),
            "target_source": target_source,
            "batch": batch_id,
            "mutation_type": mutation_type,
            "mutations": mutations,
        }
        manifest_path = scenario_root_dir / "injection_manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return scenario_root_dir

    # ── Mutation implementations ─────────────────────────────────────────────

    def _resolve_source_files(
        self, source_dir: Path, source_name: str, batch_id: str
    ) -> list[Path]:
        """Resolve source file paths within the scenario source directory."""
        src_cfg = get_source_config(source_name)
        batch_path_name = {
            "batch1": "Batch1", "batch2": "Batch2", "batch3": "Batch3"
        }.get(batch_id, batch_id.capitalize())

        if batch_id not in src_cfg.get("batches", []):
            return []

        batch_dir = source_dir / batch_path_name
        exclude = src_cfg.get("exclude_patterns", [])
        found: list[Path] = []
        for pattern in src_cfg.get("files", []):
            for f in sorted(batch_dir.glob(pattern)):
                if f.is_file() and not any(f.match(ex) for ex in exclude):
                    found.append(f)
        return found

    def _inject_partial_file(
        self, source_dir: Path, source_name: str, batch_id: str, **_: Any
    ) -> list[dict[str, Any]]:
        """Truncate each source file at 30–70% of its byte size."""
        files = self._resolve_source_files(source_dir, source_name, batch_id)
        mutations: list[dict[str, Any]] = []
        for filepath in files:
            original_size = filepath.stat().st_size
            if original_size < 10:
                continue
            offset = self.rng.randint(
                max(1, int(original_size * 0.30)),
                int(original_size * 0.70),
            )
            with filepath.open("r+b") as fh:
                fh.truncate(offset)
            mutations.append({
                "mutation_type": "partial_file",
                "source_name": source_name,
                "batch_id": batch_id,
                "relative_file": filepath.name,
                "original_size": original_size,
                "truncate_offset": offset,
                "expected_detection": "incomplete_batch",
                "expected_stage": "ingestion",
                "recoverable": False,
                "recovery_hint": "re_download",
            })
        return mutations

    def _inject_duplicate_batch(
        self, source_dir: Path, source_name: str, batch_id: str, **_: Any
    ) -> list[dict[str, Any]]:
        """Copy the entire batch directory to simulate a non-idempotent retry."""
        batch_path_name = {
            "batch1": "Batch1", "batch2": "Batch2", "batch3": "Batch3"
        }.get(batch_id, batch_id.capitalize())
        original_batch = source_dir / batch_path_name
        retry_batch = source_dir / f"{batch_path_name}_retry"
        shutil.copytree(original_batch, retry_batch)
        return [{
            "mutation_type": "duplicate_batch",
            "original_batch": batch_path_name,
            "duplicate_batch": f"{batch_path_name}_retry",
            "expected_detection": "retry_duplicate",
            "expected_stage": "bronze_validation",
            "recoverable": True,
            "recovery_hint": "dedup",
        }]

    def _inject_poison_record(
        self,
        source_dir: Path,
        source_name: str,
        batch_id: str,
        *,
        insert_position: float = 0.5,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Insert a non-UTF-8 binary line to trigger a parser exception."""
        files = self._resolve_source_files(source_dir, source_name, batch_id)
        mutations: list[dict[str, Any]] = []
        for filepath in files:
            content = filepath.read_bytes()
            lines = content.split(b"\n")
            if not lines:
                continue
            insert_at = max(1, int(len(lines) * insert_position))
            poison = b"\xff\xfe\x00BAD_BINARY_INJECTION\xff\xfe"
            lines.insert(insert_at, poison)
            filepath.write_bytes(b"\n".join(lines))
            mutations.append({
                "mutation_type": "poison_record",
                "source_name": source_name,
                "batch_id": batch_id,
                "relative_file": filepath.name,
                "insert_line": insert_at,
                "expected_detection": "parser_exception",
                "expected_stage": "ingestion",
                "recoverable": False,
                "recovery_hint": "quarantine",
            })
        return mutations

    def _inject_missing_batch(
        self, source_dir: Path, source_name: str, batch_id: str, **_: Any
    ) -> list[dict[str, Any]]:
        """Delete the target batch directory entirely."""
        batch_path_name = {
            "batch1": "Batch1", "batch2": "Batch2", "batch3": "Batch3"
        }.get(batch_id, batch_id.capitalize())
        batch_dir = source_dir / batch_path_name
        if batch_dir.exists():
            shutil.rmtree(batch_dir)
        return [{
            "mutation_type": "missing_batch",
            "batch": batch_path_name,
            "expected_detection": "batch_not_found",
            "expected_stage": "ingestion",
            "recoverable": False,
            "recovery_hint": "re_download",
        }]

    def _inject_rate_limit_partial_batch(
        self,
        source_dir: Path,
        source_name: str,
        batch_id: str,
        *,
        ratio: float = 0.40,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Keep only the first ``ratio`` fraction of lines to simulate rate limiting."""
        files = self._resolve_source_files(source_dir, source_name, batch_id)
        mutations: list[dict[str, Any]] = []
        for filepath in files:
            lines = filepath.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
            original_count = len(lines)
            keep = max(1, int(original_count * ratio))
            filepath.write_text("".join(lines[:keep]), encoding="utf-8")
            mutations.append({
                "mutation_type": "rate_limit_partial_batch",
                "source_name": source_name,
                "batch_id": batch_id,
                "relative_file": filepath.name,
                "original_lines": original_count,
                "kept_lines": keep,
                "ratio": ratio,
                "expected_detection": "row_count_mismatch",
                "expected_stage": "gold_audit",
                "recoverable": False,
                "recovery_hint": "re_download",
            })
        return mutations

    def _inject_atomic_write_failure(
        self,
        source_dir: Path,
        source_name: str,
        batch_id: str,
        *,
        completion_pct: float | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Simulate an atomic write failure: write only partial file content.

        Writes the first N% of lines, then inserts a ``__WRITE_FAILED__``
        sentinel and truncates, simulating a crash mid-write.
        """
        files = self._resolve_source_files(source_dir, source_name, batch_id)
        if not files:
            return [{"mutation_type": "atomic_write_failure",
                "source_name": source_name,
                "batch_id": batch_id, "skipped": "no_files"}]

        pct = completion_pct if completion_pct is not None else self.rng.uniform(0.40, 0.80)
        mutations: list[dict[str, Any]] = []

        for filepath in files:
            lines = filepath.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
            original_count = len(lines)
            cut_point = max(1, int(original_count * pct))

            # Write partial content with sentinel
            partial = lines[:cut_point]
            sentinel = "__ATOMIC_WRITE_FAILED__\n"
            partial.append(sentinel)
            filepath.write_text("".join(partial), encoding="utf-8")

            mutations.append({
                "mutation_type": "atomic_write_failure",
                "source_name": source_name,
                "batch_id": batch_id,
                "relative_file": filepath.name,
                "original_lines": original_count,
                "written_lines": cut_point,
                "completion_pct": round(pct, 3),
                "expected_detection": "incomplete_batch",
                "expected_stage": "ingestion",
                "recoverable": False,
                "recovery_hint": "rollback_and_retry",
            })

        return mutations


__all__ = ["ReliabilityInjector"]
