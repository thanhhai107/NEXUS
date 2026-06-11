"""
TPC-DI Lineage Error Injector.

Corrupts or suppresses lineage/audit metadata in a scenario copy to test
lineage tracking robustness (Lineage category in the 35-issue taxonomy).

Supported mutation_types:
  suppress_lineage_emission    — Remove audit/lineage fields from records.
  corrupt_audit_run_id         — Replace run_id with a fake value.
  remove_source_mapping        — Delete source_mapping entries from manifest.
  corrupt_quarantine_metadata  — Corrupt quarantine record metadata fields.
  split_emission_targets       — Duplicate lineage records across two "targets".

These mutations are structural/metadata-level, so detection depends on
lineage reconciliation (Gold or audit layer), not bronze validation.

Usage::

    from ingestion.tpcdi.error_injection.lineage_injector import LineageInjector

    li = LineageInjector(seed=42)
    scenario_root = li.create_scenario(
        "lineage_suppress_001",
        mutation_type="suppress_lineage_emission",
    )
"""

from __future__ import annotations

import json
import random
import shutil
from pathlib import Path
from typing import Any

from common.tpcdi_sources import source_root

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCENARIO_BASE = Path("runtime/tpcdi/scenarios")

# Canonical lineage fields in TPC-DI audit/metadata records
_LINEAGE_FIELDS = [
    "audit_run_id",
    "source_name",
    "batch_id",
    "source_file",
    "record_number",
    "lineage_hash",
    "ingested_at",
]

# Fields considered "audit" fields that suppression targets
_AUDIT_FIELDS = ["audit_run_id", "lineage_hash", "ingested_at"]

_FAKE_RUN_ID = "00000000-dead-beef-dead-000000000000"


class LineageInjector:
    """Lineage/metadata mutation injector for TPC-DI scenarios."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        self.rng = random.Random(seed)

    def create_scenario(
        self,
        scenario_id: str,
        *,
        mutation_type: str,
        **kwargs: Any,
    ) -> Path:
        """Create a scenario directory with one lineage mutation applied."""
        clean_root = source_root()
        scenario_root_dir = PROJECT_ROOT / SCENARIO_BASE / scenario_id
        src_dir = scenario_root_dir / "source"

        if src_dir.exists():
            shutil.rmtree(src_dir)
        shutil.copytree(clean_root, src_dir)

        method = getattr(self, f"_inject_{mutation_type}", None)
        if method is None:
            raise ValueError(f"Unknown lineage mutation_type: {mutation_type!r}")

        mutations = method(scenario_root_dir, src_dir, **kwargs)

        manifest = {
            "scenario_id": scenario_id,
            "seed": self.seed,
            "base_source_root": str(clean_root),
            "scenario_root": str(scenario_root_dir),
            "scenario_source_root": str(src_dir),
            "mutation_type": mutation_type,
            "mutations": mutations,
        }
        (scenario_root_dir / "injection_manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
        return scenario_root_dir

    # ── Mutation implementations ─────────────────────────────────────────────

    def _inject_suppress_lineage_emission(
        self,
        scenario_root_dir: Path,
        src_dir: Path,
        *,
        target_jsonl: str | None = None,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Remove lineage/audit fields from JSONL metadata files in scenario."""
        mutations: list[dict[str, Any]] = []

        # Look for any audit/lineage JSONL files in the scenario root or sub-dirs
        jsonl_files = list(scenario_root_dir.rglob("*.jsonl"))
        if target_jsonl:
            target_path = scenario_root_dir / target_jsonl
            jsonl_files = [target_path] if target_path.exists() else []

        if not jsonl_files:
            # Create a synthetic audit log to demonstrate suppression
            audit_dir = scenario_root_dir / "audit"
            audit_dir.mkdir(exist_ok=True)
            audit_file = audit_dir / "lineage_log.jsonl"
            sample_records = [
                {
                    "audit_run_id": "run-001",
                    "source_name": "trade",
                    "batch_id": "batch1",
                    "record_number": i,
                    "lineage_hash": f"hash{i:06d}",
                    "ingested_at": "2024-01-01T00:00:00",
                }
                for i in range(10)
            ]
            audit_file.write_text(
                "\n".join(json.dumps(r) for r in sample_records), encoding="utf-8"
            )
            jsonl_files = [audit_file]

        suppressed_fields: list[str] = []
        for jf in jsonl_files:
            lines = jf.read_text(encoding="utf-8").splitlines()
            new_lines: list[str] = []
            for line in lines:
                if not line.strip():
                    continue
                rec = json.loads(line)
                removed = [f for f in _AUDIT_FIELDS if f in rec]
                for field in _AUDIT_FIELDS:
                    rec.pop(field, None)
                new_lines.append(json.dumps(rec))
                suppressed_fields.extend(removed)
            jf.write_text("\n".join(new_lines), encoding="utf-8")

        mutations.append({
            "mutation_type": "suppress_lineage_emission",
            "suppressed_fields": list(set(suppressed_fields)),
            "affected_files": [str(jf.relative_to(scenario_root_dir)) for jf in jsonl_files],
            "expected_detection": "lineage_gap",
            "expected_stage": "audit",
            "recoverable": False,
        })
        return mutations

    def _inject_corrupt_audit_run_id(
        self,
        scenario_root_dir: Path,
        src_dir: Path,
        *,
        fake_run_id: str = _FAKE_RUN_ID,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Replace all audit_run_id values with a fake/invalid run ID."""
        mutations: list[dict[str, Any]] = []

        # Apply to injection_manifest.json if exists
        manifest_path = scenario_root_dir / "injection_manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
            original_run_id = manifest.get("run_id", "")
            manifest["run_id"] = fake_run_id
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        else:
            original_run_id = ""
            # Create a minimal manifest with corrupt run_id
            manifest_path.write_text(
                json.dumps({"run_id": fake_run_id, "_injected": True}, indent=2),
                encoding="utf-8",
            )

        # Also corrupt any audit JSONL files
        affected_files: list[str] = []
        for jf in scenario_root_dir.rglob("*.jsonl"):
            lines = jf.read_text(encoding="utf-8").splitlines()
            new_lines: list[str] = []
            modified = False
            for line in lines:
                if not line.strip():
                    continue
                rec = json.loads(line)
                if "audit_run_id" in rec:
                    rec["audit_run_id"] = fake_run_id
                    modified = True
                new_lines.append(json.dumps(rec))
            if modified:
                jf.write_text("\n".join(new_lines), encoding="utf-8")
                affected_files.append(str(jf.relative_to(scenario_root_dir)))

        mutations.append({
            "mutation_type": "corrupt_audit_run_id",
            "original_run_id": original_run_id,
            "injected_run_id": fake_run_id,
            "affected_files": affected_files,
            "expected_detection": "run_id_mismatch",
            "expected_stage": "audit",
            "recoverable": False,
        })
        return mutations

    def _inject_remove_source_mapping(
        self,
        scenario_root_dir: Path,
        src_dir: Path,
        *,
        source_name: str = "trade",
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Remove the source_mapping entry for a given source from manifests."""
        mutations: list[dict[str, Any]] = []

        manifest_path = scenario_root_dir / "injection_manifest.json"
        if not manifest_path.exists():
            manifest_path.write_text(
                json.dumps({
                    "source_mapping": {
                        "trade": "Batch1/Trade.txt",
                        "daily_market": "Batch1/DailyMarket.txt",
                    }
                }, indent=2),
                encoding="utf-8",
            )

        manifest = json.loads(manifest_path.read_text())
        mapping = manifest.get("source_mapping", {})
        removed_entry = mapping.pop(source_name, None)
        manifest["source_mapping"] = mapping
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        mutations.append({
            "mutation_type": "remove_source_mapping",
            "target_source": source_name,
            "removed_mapping": removed_entry,
            "expected_detection": "lineage_gap",
            "expected_stage": "audit",
            "recoverable": False,
        })
        return mutations

    def _inject_corrupt_quarantine_metadata(
        self,
        scenario_root_dir: Path,
        src_dir: Path,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Corrupt metadata fields in quarantine JSONL records."""
        mutations: list[dict[str, Any]] = []

        quarantine_dir = PROJECT_ROOT / "runtime" / "lake" / "quarantine"
        q_files = list(quarantine_dir.rglob("*.jsonl")) if quarantine_dir.exists() else []

        # Create synthetic quarantine records in scenario if none exist
        if not q_files:
            local_q = scenario_root_dir / "quarantine"
            local_q.mkdir(exist_ok=True)
            q_file = local_q / "quarantine_sample.jsonl"
            records = [
                {
                    "mutation_id": f"mut-{i:04d}",
                    "source_name": "trade",
                    "batch_id": "batch1",
                    "source_file": "Batch1/Trade.txt",
                    "record_number": i * 10,
                    "error_type": "field_count_mismatch",
                    "raw_record": f"field1|field2|{i}",
                }
                for i in range(5)
            ]
            q_file.write_text(
                "\n".join(json.dumps(r) for r in records), encoding="utf-8"
            )
            q_files = [q_file]

        corrupted_count = 0
        for qf in q_files:
            lines = qf.read_text(encoding="utf-8").splitlines()
            new_lines: list[str] = []
            for line in lines:
                if not line.strip():
                    continue
                rec = json.loads(line)
                # Corrupt: remove source_name, scramble record_number
                rec.pop("source_name", None)
                if "record_number" in rec:
                    rec["record_number"] = -1
                rec.pop("mutation_id", None)
                new_lines.append(json.dumps(rec))
                corrupted_count += 1
            qf.write_text("\n".join(new_lines), encoding="utf-8")

        mutations.append({
            "mutation_type": "corrupt_quarantine_metadata",
            "corrupted_records": corrupted_count,
            "removed_fields": ["source_name", "mutation_id"],
            "corrupted_fields": ["record_number"],
            "expected_detection": "quarantine_metadata_invalid",
            "expected_stage": "audit",
            "recoverable": False,
        })
        return mutations

    def _inject_split_emission_targets(
        self,
        scenario_root_dir: Path,
        src_dir: Path,
        *,
        n_targets: int = 2,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Duplicate lineage records across N emission target files."""
        mutations: list[dict[str, Any]] = []

        # Create a synthetic lineage emit log
        emit_dir = scenario_root_dir / "lineage_emit"
        emit_dir.mkdir(exist_ok=True)
        source_records = [
            {
                "audit_run_id": "run-SPLIT-001",
                "source_name": "trade",
                "batch_id": "batch1",
                "record_number": i,
                "lineage_hash": f"hash{i:06d}",
            }
            for i in range(20)
        ]

        target_files: list[str] = []
        for t in range(n_targets):
            target_file = emit_dir / f"target_{t + 1}_lineage.jsonl"
            target_file.write_text(
                "\n".join(json.dumps(r) for r in source_records), encoding="utf-8"
            )
            target_files.append(str(target_file.relative_to(scenario_root_dir)))

        mutations.append({
            "mutation_type": "split_emission_targets",
            "n_targets": n_targets,
            "records_per_target": len(source_records),
            "total_emitted": len(source_records) * n_targets,
            "target_files": target_files,
            "expected_detection": "lineage_duplication",
            "expected_stage": "audit",
            "recoverable": True,
            "recovery_hint": "deduplicate_lineage_records",
        })
        return mutations


__all__ = ["LineageInjector"]
