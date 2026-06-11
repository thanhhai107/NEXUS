"""
TPC-DI Format/Structure Error Injector.

Converts source files to different formats or structures to test adapter
heterogeneity handling (Source Heterogeneity category).

Supported mutation_types:
  csv_to_json              — Convert pipe-delimited file to JSONL.
  flat_to_nested           — Convert flat columns to nested JSON objects.
  split_batch_to_microfiles — Split one batch file into N smaller files.

Usage::

    from ingestion.tpcdi.error_injection.format_injector import FormatInjector

    fi = FormatInjector(seed=42)
    scenario_root = fi.create_scenario(
        "fmt_json_001",
        target_source="trade",
        batch_id="batch1",
        mutation_type="csv_to_json",
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

# Logical column groupings for flat_to_nested
_NEST_GROUPS: dict[str, dict[str, list[str]]] = {
    "trade": {
        "execution": ["execution_id", "execution_broker_id"],
        "financials": ["trade_price", "cash_amount", "fee", "commission", "tax"],
    },
    "daily_market": {
        "prices": ["dm_close", "dm_high", "dm_low"],
    },
}


class FormatInjector:
    """File-format mutation injector for TPC-DI DIGen data."""

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
        """Create a scenario directory with one format mutation applied."""
        clean_root = source_root()
        scenario_root_dir = PROJECT_ROOT / SCENARIO_BASE / scenario_id
        src_dir = scenario_root_dir / "source"

        if src_dir.exists():
            shutil.rmtree(src_dir)
        shutil.copytree(clean_root, src_dir)

        method = getattr(self, f"_inject_{mutation_type}", None)
        if method is None:
            raise ValueError(f"Unknown format mutation_type: {mutation_type!r}")

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

    # ── Mutation implementations ─────────────────────────────────────────────

    def _inject_csv_to_json(
        self,
        src_dir: Path,
        source_name: str,
        batch_id: str,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Convert pipe-delimited CSV to JSONL (one JSON object per line)."""
        cfg = get_source_config(source_name)
        columns = cfg.get("columns", [])
        delimiter = cfg.get("delimiter", "|")

        if not columns:
            return [{"mutation_type": "csv_to_json", "skipped": "no_columns_defined"}]

        mutations: list[dict[str, Any]] = []
        for filepath in self._resolve_files(src_dir, source_name, batch_id):
            lines = filepath.read_text(encoding="utf-8").splitlines()
            json_lines: list[str] = []
            for line in lines:
                if not line.strip():
                    continue
                fields = line.split(delimiter)
                record = dict(zip(columns, fields))
                json_lines.append(json.dumps(record))

            # Write .jsonl alongside original, remove .txt
            jsonl_path = filepath.with_suffix(".jsonl")
            jsonl_path.write_text("\n".join(json_lines), encoding="utf-8")
            original_size = filepath.stat().st_size
            filepath.unlink()

            mutations.append({
                "mutation_type": "csv_to_json",
                "source_name": source_name,
                "batch_id": batch_id,
                "original_file": filepath.name,
                "converted_file": jsonl_path.name,
                "record_count": len(json_lines),
                "original_size": original_size,
                "expected_detection": "format_mismatch",
                "expected_stage": "ingestion",
                "recoverable": True,
                "recovery_hint": "use_json_adapter",
            })
        return mutations

    def _inject_flat_to_nested(
        self,
        src_dir: Path,
        source_name: str,
        batch_id: str,
        *,
        group_map: dict[str, list[str]] | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Restructure flat columns into nested JSON objects."""
        cfg = get_source_config(source_name)
        columns = cfg.get("columns", [])
        delimiter = cfg.get("delimiter", "|")
        groups = group_map or _NEST_GROUPS.get(source_name, {})

        if not columns or not groups:
            return [{"mutation_type": "flat_to_nested", "skipped": "no_columns_or_groups"}]

        # Fields that go into nested objects
        nested_fields: set[str] = set()
        for group_cols in groups.values():
            nested_fields.update(group_cols)

        mutations: list[dict[str, Any]] = []
        for filepath in self._resolve_files(src_dir, source_name, batch_id):
            lines = filepath.read_text(encoding="utf-8").splitlines()
            json_lines: list[str] = []
            for line in lines:
                if not line.strip():
                    continue
                fields = line.split(delimiter)
                flat = dict(zip(columns, fields))
                record: dict[str, Any] = {}
                # Keep un-grouped fields flat
                for col, val in flat.items():
                    if col not in nested_fields:
                        record[col] = val
                # Add grouped fields as nested objects
                for group_name, group_cols in groups.items():
                    record[group_name] = {c: flat.get(c, "") for c in group_cols}
                json_lines.append(json.dumps(record))

            jsonl_path = filepath.with_suffix(".nested.jsonl")
            jsonl_path.write_text("\n".join(json_lines), encoding="utf-8")
            filepath.unlink()

            mutations.append({
                "mutation_type": "flat_to_nested",
                "source_name": source_name,
                "batch_id": batch_id,
                "original_file": filepath.name,
                "converted_file": jsonl_path.name,
                "nested_groups": list(groups.keys()),
                "expected_detection": "format_mismatch",
                "expected_stage": "ingestion",
                "recoverable": True,
                "recovery_hint": "use_nested_adapter",
            })
        return mutations

    def _inject_split_batch_to_microfiles(
        self,
        src_dir: Path,
        source_name: str,
        batch_id: str,
        *,
        n_chunks: int = 5,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Split a batch file into ``n_chunks`` smaller files."""
        mutations: list[dict[str, Any]] = []
        for filepath in self._resolve_files(src_dir, source_name, batch_id):
            lines = filepath.read_text(encoding="utf-8").splitlines(keepends=True)
            if not lines:
                continue
            chunk_size = max(1, len(lines) // n_chunks)
            stem = filepath.stem
            suffix = filepath.suffix
            created: list[str] = []
            for i in range(n_chunks):
                start = i * chunk_size
                end = start + chunk_size if i < n_chunks - 1 else len(lines)
                chunk_path = filepath.parent / f"{stem}_part{i + 1:03d}{suffix}"
                chunk_path.write_text("".join(lines[start:end]), encoding="utf-8")
                created.append(chunk_path.name)
            filepath.unlink()

            mutations.append({
                "mutation_type": "split_batch_to_microfiles",
                "source_name": source_name,
                "batch_id": batch_id,
                "original_file": filepath.name,
                "micro_files": created,
                "n_chunks": n_chunks,
                "expected_detection": "format_mismatch",
                "expected_stage": "ingestion",
                "recoverable": True,
                "recovery_hint": "merge_microfiles",
            })
        return mutations


__all__ = ["FormatInjector"]
