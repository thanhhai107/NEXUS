"""
Recovery Engine — orchestrate repair of pipeline-detected errors.

1. Read injection_manifest.json + detected_errors
2. Copy scenario/source → recovered_source
3. Apply repair strategies to the file lines
4. Write recovery_log.json
5. Retry pipeline if requested
"""

from __future__ import annotations

import csv
import io
import json
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from governance.recovery.strategies import (
    RepairContext,
    RepairResult,
    RepairStrategy,
    DropExtraFieldStrategy,
    InferMissingFieldStrategy,
    SafeCastStrategy,
    DedupStrategy,
    DefaultValueStrategy,
    OutlierCorrectionStrategy,
    BusinessRuleCorrectionStrategy,
    SchemaRevertStrategy,
)
from common.tpcdi_sources import get_source_config


@dataclass
class RecoveryReport:
    scenario_id: str = ""
    run_id: str = ""
    total_quarantined: int = 0
    repaired: int = 0
    repair_candidates: int = 0
    unrecoverable: int = 0
    strategy_breakdown: dict[str, int] = field(default_factory=dict)
    records: list[dict[str, Any]] = field(default_factory=list)

    @property
    def recovery_rate(self) -> float:
        if self.total_quarantined == 0:
            return 1.0
        return self.repaired / self.total_quarantined

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "run_id": self.run_id,
            "total_quarantined": self.total_quarantined,
            "repaired": self.repaired,
            "repair_candidates": self.repair_candidates,
            "unrecoverable": self.unrecoverable,
            "strategy_breakdown": self.strategy_breakdown,
            "recovery_rate": round(self.recovery_rate, 4),
        }

class RecoveryEngine:
    """Orchestrate repair of source files for a given scenario."""

    def __init__(self, seed: int = 42):
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        self.strategies: dict[str, RepairStrategy] = {
            "field_count_mismatch_extra": DropExtraFieldStrategy(),
            "field_count_mismatch_missing": InferMissingFieldStrategy(),
            "type_coercion_error": SafeCastStrategy(),
            "duplicate_primary_key": DedupStrategy(),
            "missing_field": InferMissingFieldStrategy(),
            "null_required_field": DefaultValueStrategy(),
            "missing_required_field": DefaultValueStrategy(),
            "outlier_detected": OutlierCorrectionStrategy(),
            "business_rule_fail": BusinessRuleCorrectionStrategy(),
            "field_type_change": SafeCastStrategy(),
            "dropped_downstream_field": InferMissingFieldStrategy(),
            "schema_mutation": SchemaRevertStrategy(),
        }

    def run(
        self,
        scenario_root: str | Path,
        *,
        detected_errors: list[dict[str, Any]] | None = None,
        bronze_validation: dict[str, Any] | None = None,
        audit_results: list[dict[str, Any]] | None = None,
        quarantine_root: str | Path | None = None,
        retry_pipeline: bool = False,
    ) -> RecoveryReport:
        """Run recovery for a scenario.

        Uses manifest mutations as the primary source of "what to repair".
        Detected errors are matched to mutations by error_type + mutation_id.
        This way we always have physical_line_number from the manifest.
        """
        scenario_root = Path(scenario_root)
        source_dir = scenario_root / "source"
        recovered_dir = scenario_root / "recovered_source"
        manifest_path = scenario_root / "injection_manifest.json"

        if not manifest_path.exists():
            return RecoveryReport(scenario_id="unknown", total_quarantined=0)

        manifest = json.loads(manifest_path.read_text())
        scenario_id = manifest.get("scenario_id", "unknown")
        target_source = manifest.get("target_source", "trade")
        batch_id = manifest.get("batch", "batch1")
        mutations = manifest.get("mutations", [])

        report = RecoveryReport(scenario_id=scenario_id, run_id=self.run_id)

        if not mutations or not source_dir.exists():
            return report

        # Collect detected errors if not provided
        if detected_errors is None:
            from ingestion.tpcdi.error_injection.error_collector import collect_detected_errors
            detected_errors = collect_detected_errors(
                scenario_id, self.run_id,
                bronze_validation=bronze_validation,
                audit_results=audit_results,
                quarantine_root=quarantine_root,
            )

        # Build a lookup: (mutation_id) → detected error
        detected_by_mid: dict[str, dict[str, Any]] = {}
        for err in detected_errors:
            mid = err.get("mutation_id")
            if mid:
                detected_by_mid[mid] = err

        # Also build by error_type (fallback for aggregate errors without mutation_id)
        detected_by_type: dict[str, list[dict[str, Any]]] = {}
        for err in detected_errors:
            et = err.get("error_type", "")
            detected_by_type.setdefault(et, []).append(err)

        # Copy source → recovered_source
        if recovered_dir.exists():
            shutil.rmtree(recovered_dir)
        shutil.copytree(source_dir, recovered_dir)

        # Load source config
        try:
            src_cfg = get_source_config(target_source)
        except KeyError:
            src_cfg = {}
        expected_columns = src_cfg.get("columns", [])
        delimiter = src_cfg.get("delimiter", "|")
        report.total_quarantined = len(mutations)

        # Process each mutation from the manifest
        # Group DedupStrategy mutations by file → process descending line number
        dedup_by_file: dict[str, list[tuple[int, dict]]] = {}
        regular_mutations: list[dict] = []

        for mutation in mutations:
            error_type = mutation.get("expected_detection", "")
            if error_type == "duplicate_primary_key":
                rel_file = mutation.get("relative_file", f"{target_source}.txt")
                phys_line = mutation.get("physical_line_number")
                if phys_line:
                    dedup_by_file.setdefault(rel_file, []).append((phys_line, mutation))
            else:
                regular_mutations.append(mutation)

        # Process dedup mutations grouped by file, descending line number
        for rel_file, entries in dedup_by_file.items():
            entries.sort(key=lambda x: x[0], reverse=True)  # descending
            src_path = recovered_dir / rel_file
            if not src_path.exists():
                matches = list(recovered_dir.rglob(rel_file.split("/")[-1]))
                src_path = matches[0] if matches else None
            if not src_path or not src_path.exists():
                for _, m in entries:
                    report.unrecoverable += 1
                    report.records.append({"mutation_id": m["mutation_id"], "action": "unrecoverable", "reason": "file not found", "applied": False})
                continue

            for phys_line, mutation in entries:
                idx = phys_line - 1
                report.repaired += 1
                report.strategy_breakdown.setdefault("DedupStrategy", 0)
                report.strategy_breakdown["DedupStrategy"] += 1
                report.records.append({
                    "mutation_id": mutation.get("mutation_id"),
                    "strategy": "DedupStrategy",
                    "action": "repaired",
                    "confidence": 0.95,
                    "before": "(duplicate line)",
                    "after": "(removed)",
                    "applied": True,
                })

            # Rewrite file with duplicates removed
            lines = src_path.read_text(encoding="utf-8").splitlines(keepends=True)
            for phys_line, mutation in entries:
                idx = phys_line - 1
                if idx < len(lines):
                    lines.pop(idx)
            src_path.write_text("".join(lines), encoding="utf-8")

        # Process regular (non-dedup) mutations
        for mutation in regular_mutations:
            mutation_id = mutation.get("mutation_id", "")
            error_type = mutation.get("expected_detection", "")
            phys_line = mutation.get("physical_line_number")
            rel_file = mutation.get("relative_file", f"{target_source}.txt")

            if not phys_line:
                report.unrecoverable += 1
                continue

            # Find the matching detected error
            detected = detected_by_mid.get(mutation_id)
            if not detected:
                candidates = detected_by_type.get(error_type, [])
                if candidates:
                    detected = candidates.pop(0)

            if not detected:
                report.unrecoverable += 1
                report.records.append({"mutation_id": mutation_id, "action": "unrecoverable", "reason": "error not detected", "applied": False})
                continue

            # Select strategy
            strategy = self._select_strategy(error_type, detected, mutation)
            if strategy is None:
                report.unrecoverable += 1
                report.records.append({"mutation_id": mutation_id, "action": "unrecoverable", "reason": f"No strategy for {error_type}", "applied": False})
                continue

            # Find the file
            src_path = recovered_dir / rel_file
            if not src_path.exists():
                matches = list(recovered_dir.rglob(rel_file.split("/")[-1]))
                src_path = matches[0] if matches else None
            if not src_path or not src_path.exists():
                report.unrecoverable += 1
                continue

            try:
                lines = src_path.read_text(encoding="utf-8").splitlines(keepends=True)
            except Exception:
                report.unrecoverable += 1
                continue

            if phys_line > len(lines):
                report.unrecoverable += 1
                continue

            idx = phys_line - 1
            raw_line = lines[idx].rstrip("\n\r")

            # Parse field_index from target_field if not in mutation
            target_field = mutation.get("target_field", "")
            field_index = mutation.get("field_index")
            if field_index is None and target_field:
                if target_field.startswith("col_"):
                    try:
                        field_index = int(target_field.split("_")[1])
                    except (ValueError, IndexError):
                        pass
                else:
                    # Real column name — look up in expected_columns
                    try:
                        field_index = expected_columns.index(target_field)
                    except (ValueError, IndexError):
                        pass

            context = RepairContext(
                source_name=target_source,
                batch_id=batch_id,
                relative_file=str(rel_file),
                expected_columns=expected_columns,
                delimiter=delimiter,
                line_number=phys_line,
                raw_line=raw_line,
                mutation_id=mutation_id,
                field_index=field_index,
                target_field=target_field,
                original_value=mutation.get("original_value"),
                mutated_value=mutation.get("mutated_value"),
                mutation_type=mutation.get("mutation_type"),
            )

            result: RepairResult = strategy.repair(context)

            if result.success and result.confidence >= 0.8:
                report.strategy_breakdown.setdefault(strategy.__class__.__name__, 0)
                report.strategy_breakdown[strategy.__class__.__name__] += 1
                report.repaired += 1
                lines[idx] = result.after + "\n"
                action = "repaired"
            elif result.success and result.confidence >= 0.5:
                report.repair_candidates += 1
                action = "repair_candidate"
            else:
                report.unrecoverable += 1
                action = "unrecoverable"

            src_path.write_text("".join(lines), encoding="utf-8")
            report.records.append({
                "mutation_id": mutation_id,
                "strategy": strategy.__class__.__name__,
                "action": action,
                "confidence": result.confidence,
                "before": result.before[:100],
                "after": result.after[:100] if result.success else "(unchanged)",
                "applied": result.success and result.confidence >= 0.8,
            })

        report.total_quarantined = len(mutations)

        # Write recovery_log.json
        recovery_log = {
            "scenario_id": scenario_id,
            "run_id": self.run_id,
            "recovered_source_root": str(recovered_dir),
            "stats": {
                "total_quarantined": report.total_quarantined,
                "repaired": report.repaired,
                "repair_candidates": report.repair_candidates,
                "unrecoverable": report.unrecoverable,
            },
            "records": report.records,
        }
        (scenario_root / "recovery_log.json").write_text(
            json.dumps(recovery_log, indent=2, default=str), encoding="utf-8"
        )

        if retry_pipeline:
            self._retry_pipeline(recovered_dir, report)

        return report

    def _select_strategy(self, error_type: str, error: dict[str, Any], mutation: dict[str, Any] | None = None) -> RepairStrategy | None:
        """Map error_type to the best repair strategy.

        Differentiates missing vs extra field_count_mismatch by checking
        actual vs expected field count from the mutation context.
        """
        # 1. Exact match via strategy dict
        if error_type in self.strategies:
            return self.strategies[error_type]

        # 2. field_count_mismatch — determine direction
        if error_type == "field_count_mismatch":
            mt = mutation.get("mutation_type", "") if mutation else ""
            if mt == "missing_field":
                return InferMissingFieldStrategy()
            return DropExtraFieldStrategy()

        # 3. type_coercion_error
        if error_type == "type_coercion_error":
            return SafeCastStrategy()

        # 4. duplicate_primary_key / row_count_mismatch
        if error_type in ("duplicate_primary_key", "duplicate_batch", "retry_duplicate"):
            return DedupStrategy()

        # 5. Schema errors
        if error_type in ("missing_required_field", "dropped_downstream_field"):
            return DefaultValueStrategy()

        if error_type in ("field_type_change", "field_rename_candidate"):
            return SchemaRevertStrategy()

        # 6. Data quality errors
        if error_type in ("null_required_field", "missing_value", "missing_data"):
            return DefaultValueStrategy()
        if error_type == "outlier_detected":
            return OutlierCorrectionStrategy()
        if error_type == "business_rule_fail":
            return BusinessRuleCorrectionStrategy()

        # 7. Reliability — dedup for retry issues
        if error_type == "row_count_mismatch":
            return None  # Cannot auto-fix; needs re-download

        # 8. Semantic/format/lineage — not recoverable at file level
        return None

    def _retry_pipeline(self, recovered_source_root: Path, report: RecoveryReport) -> None:
        """Run the pipeline on recovered_source and score."""
        original = os.environ.get("TPCDI_SOURCE_ROOT")
        os.environ["TPCDI_SOURCE_ROOT"] = str(recovered_source_root)
        try:
            from benchmark.tpcdi.runner import TpcdiRunner
            runner = TpcdiRunner(scale_factor=int(os.environ.get("TPCDI_SCALE_FACTOR", "3")))
            result = runner.run_milestone4(clean_outputs=True)
            report._pipeline_result = result.to_dict()
        finally:
            if original:
                os.environ["TPCDI_SOURCE_ROOT"] = original
            else:
                del os.environ["TPCDI_SOURCE_ROOT"]

    # ═════════════════════════════════════════════════════════════════════
    # Quarantine-based recovery (production mode — no manifest dependency)
    # ═════════════════════════════════════════════════════════════════════

    def recover_from_quarantine(
        self,
        quarantine_dir: str | Path,
        *,
        target_source: str | None = None,
        delimiter: str = "|",
    ) -> RecoveryReport:
        """Recover records directly from quarantine without an injection manifest.

        Scans quarantine JSONL files, applies repair strategies to each
        failed record, and writes recovered records back to a replay-ready
        file in ``runtime/lake/recovered/``.

        This is the production recovery path — no manifest needed.
        """
        qpath = Path(quarantine_dir)
        if not qpath.exists():
            return RecoveryReport(scenario_id="quarantine_recovery", total_quarantined=0)

        report = RecoveryReport(
            scenario_id="quarantine_recovery",
            run_id=self.run_id,
        )

        recovered_dir = qpath.parent.parent / "recovered"
        recovered_dir.mkdir(parents=True, exist_ok=True)

        # Collect all quarantine records
        quarantined: list[dict[str, Any]] = []
        for jf in sorted(qpath.rglob("*.jsonl")):
            for line in jf.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    if target_source and rec.get("source_name") != target_source:
                        continue
                    quarantined.append(rec)
                except json.JSONDecodeError:
                    continue

        report.total_quarantined = len(quarantined)
        if not quarantined:
            return report

        # Group by source for batch processing
        by_source: dict[str, list[dict[str, Any]]] = {}
        for rec in quarantined:
            src = rec.get("source_name", "unknown")
            by_source.setdefault(src, []).append(rec)

        for source_name, records in by_source.items():
            try:
                src_cfg = get_source_config(source_name)
            except KeyError:
                src_cfg = {}
            expected_columns = src_cfg.get("columns", [])
            delim = src_cfg.get("delimiter", delimiter)

            replay_file = recovered_dir / f"{source_name}_recovered.jsonl"
            repaired_this_source = 0

            for rec in records:
                raw_line = rec.get("raw_record", "")
                if not raw_line:
                    report.unrecoverable += 1
                    continue

                # Select strategy from error_type
                error_type = rec.get("error_type", "")
                strategy = self.strategies.get(error_type) or self._select_strategy(error_type, rec)

                if strategy is None:
                    report.unrecoverable += 1
                    report.records.append({
                        "source_name": source_name,
                        "error_type": error_type,
                        "action": "unrecoverable",
                        "reason": f"No strategy for {error_type}",
                        "applied": False,
                    })
                    continue

                # Build repair context from quarantine record
                context = RepairContext(
                    source_name=source_name,
                    batch_id=rec.get("batch_id", "batch1"),
                    relative_file=rec.get("source_file", ""),
                    expected_columns=expected_columns,
                    delimiter=delim,
                    line_number=rec.get("record_number", 1) or 1,
                    raw_line=raw_line,
                    mutation_id=None,
                    field_index=None,
                    target_field=rec.get("field", ""),
                    original_value=rec.get("original_value", ""),
                    mutated_value=rec.get("mutated_value", ""),
                    mutation_type=None,
                )

                result = strategy.repair(context)

                if result.success and result.confidence >= 0.5:
                    report.repaired += 1
                    repaired_this_source += 1
                    report.strategy_breakdown.setdefault(strategy.__class__.__name__, 0)
                    report.strategy_breakdown[strategy.__class__.__name__] += 1
                    report.records.append({
                        "source_name": source_name,
                        "error_type": error_type,
                        "strategy": strategy.__class__.__name__,
                        "action": "repaired",
                        "confidence": result.confidence,
                        "before": result.before[:100],
                        "after": result.after[:100],
                        "applied": True,
                    })
                    # Write recovered record
                    recovered_rec = {**rec, "_recovered_line": result.after, "_repaired_by": strategy.__class__.__name__}
                    with replay_file.open("a", encoding="utf-8") as fh:
                        fh.write(json.dumps(recovered_rec, default=str) + "\n")
                else:
                    report.unrecoverable += 1
                    report.records.append({
                        "source_name": source_name,
                        "error_type": error_type,
                        "strategy": strategy.__class__.__name__,
                        "action": "unrecoverable" if not result.success else "repair_candidate",
                        "confidence": result.confidence,
                        "applied": False,
                    })

            if repaired_this_source > 0:
                report.repair_candidates += repaired_this_source

        # Write recovery log
        recovery_log_path = recovered_dir / "quarantine_recovery_log.json"
        recovery_log_path.write_text(
            json.dumps(report.to_dict(), indent=2, default=str), encoding="utf-8"
        )

        return report

    def replay_recovered(
        self,
        recovered_dir: str | Path,
        *,
        target_source: str | None = None,
    ) -> dict[str, Any]:
        """Replay recovered records back through bronze validation.

        Reads recovered JSONL files, feeds the repaired lines through
        validate_bronze_tpcdi_file, and returns the validation result.
        """
        from governance.quality.bronze_validator import validate_bronze_file

        rdir = Path(recovered_dir)
        results: dict[str, Any] = {}

        for rec_file in sorted(rdir.glob("*_recovered.jsonl")):
            source_name = rec_file.stem.replace("_recovered", "")
            if target_source and source_name != target_source:
                continue

            recovered_lines: list[str] = []
            for line in rec_file.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                rec = json.loads(line)
                recovered_line = rec.get("_recovered_line", "")
                if recovered_line:
                    recovered_lines.append(recovered_line)

            if not recovered_lines:
                continue

            # Re-validate
            try:
                result = validate_bronze_file(
                    source_name=source_name,
                    records=recovered_lines,
                )
                results[source_name] = {
                    "status": result.get("status", "unknown"),
                    "recovered_count": len(recovered_lines),
                    "still_invalid": result.get("details", {}).get("field_count_errors", 0)
                                      + result.get("details", {}).get("type_coercion_errors", 0),
                }
            except Exception as e:
                results[source_name] = {"status": "error", "error": str(e)}

        return results
