"""
TPC-DI Scoring Engine — compare injection_manifest against pipeline results.

Phases:
  detect-only: TP/FP/FN cho detection
  full: + recovery metrics (repaired vs total_injected)

Usage::

    from benchmark.tpcdi.scoring import TpcdiScoringEngine, ScoringReport

    scorer = TpcdiScoringEngine()
    report = scorer.score_detection(
        manifest_path="runtime/tpcdi/scenarios/.../injection_manifest.json",
        detected_errors=[...],
    )
    print(report.to_dict())
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ingestion.tpcdi.error_injection.manifest_reader import load_manifest, iter_mutations
from ingestion.tpcdi.error_injection.error_collector import collect_detected_errors


@dataclass
class ScoringReport:
    scenario_id: str = ""
    run_id: str = ""
    total_injected: int = 0

    # Detection
    true_positives: int = 0
    false_positives: int = 0
    false_negatives: int = 0
    detection_rate: float = 0.0
    precision: float = 0.0
    false_discovery_rate: float = 0.0

    # Recovery (optional, populated in full mode)
    repaired: int = 0
    repair_candidates: int = 0
    unrecoverable: int = 0
    total_quarantined: int = 0
    repair_rate_on_detected: float = 0.0
    end_to_end_recovery_rate: float = 0.0

    # Leakage
    leaked_to_gold: int = 0
    leakage_rate: float = 0.0

    # Detail
    details: dict[str, Any] = field(default_factory=dict)
    summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "run_id": self.run_id,
            "total_injected": self.total_injected,
            "detection": {
                "true_positives": self.true_positives,
                "false_positives": self.false_positives,
                "false_negatives": self.false_negatives,
                "detection_rate": round(self.detection_rate, 4),
                "precision": round(self.precision, 4),
                "false_discovery_rate": round(self.false_discovery_rate, 4),
            },
            "recovery": {
                "repaired": self.repaired,
                "repair_candidates": self.repair_candidates,
                "unrecoverable": self.unrecoverable,
                "total_quarantined": self.total_quarantined,
                "repair_rate_on_detected": round(self.repair_rate_on_detected, 4),
                "end_to_end_recovery_rate": round(self.end_to_end_recovery_rate, 4),
            },
            "leakage": {
                "leaked_to_gold": self.leaked_to_gold,
                "leakage_rate": round(self.leakage_rate, 4),
            },
            "details": self.details,
            "summary": self.summary,
        }


class TpcdiScoringEngine:
    """Compare injection manifest against pipeline detection results."""

    def __init__(self):
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    def score_detection(
        self,
        manifest_path: str | Path,
        *,
        bronze_validation: dict[str, Any] | None = None,
        audit_results: list[dict[str, Any]] | None = None,
        quarantine_root: str | Path | None = None,
        detected_errors: list[dict[str, Any]] | None = None,
    ) -> ScoringReport:
        """Score detection-only: compare injected vs detected errors.

        Parameters
        ----------
        manifest_path: Path to injection_manifest.json or scenario root dir.
        bronze_validation: Output of validate_bronze_tpcdi_file().
        audit_results: List of correctness audit dicts (from runner results).
        quarantine_root: Path to runtime/lake/quarantine/tpcdi/.
        detected_errors: Pre-collected detected errors (bypass internal collect).
        """
        manifest = load_manifest(manifest_path)
        scenario_id = manifest.get("scenario_id", "unknown")
        injected = iter_mutations(manifest)
        total = len(injected)

        # Collect detected errors if not provided
        if detected_errors is None:
            detected_errors = collect_detected_errors(
                scenario_id, self.run_id,
                bronze_validation=bronze_validation,
                audit_results=audit_results,
                quarantine_root=quarantine_root,
            )

        # Match injected vs detected
        tp: list[dict[str, Any]] = []
        fn: list[dict[str, Any]] = []
        fp: list[dict[str, Any]] = []

        # Use a pool of unmatched detections to ensure one-to-one matching
        unmatched_detections = list(detected_errors)

        for injection in injected:
            mid = injection.get("mutation_id")
            found_idx = None

            for idx, det in enumerate(unmatched_detections):
                # Match by mutation_id first (exact)
                if mid and det.get("mutation_id") == mid:
                    found_idx = idx
                    break

            if found_idx is None:
                # Fallback level 1: source + batch + file + physical_line + error_type (exact line match)
                for idx, det in enumerate(unmatched_detections):
                    if (str(det.get("physical_line_number")) == str(injection.get("physical_line_number"))
                            and det.get("source_name") == injection.get("source_name")
                            and det.get("batch_id") == injection.get("batch_id")
                            and det.get("relative_file") == injection.get("relative_file")
                            and det.get("error_type") == injection.get("expected_detection")):
                        found_idx = idx
                        break

            if found_idx is None:
                # Fallback level 2: source + batch + error_type (table-level, no line precision)
                for idx, det in enumerate(unmatched_detections):
                    if (det.get("source_name") == injection.get("source_name")
                            and det.get("batch_id") == injection.get("batch_id")
                            and det.get("error_type") == injection.get("expected_detection")):
                        found_idx = idx
                        break

            if found_idx is not None:
                tp.append(injection)
                unmatched_detections.pop(found_idx)  # remove matched detection from pool
            else:
                fn.append(injection)

        # Remaining unmatched = false positives
        fp = unmatched_detections

        # Safe division
        tp_count = len(tp)
        fp_count = len(fp)
        fn_count = len(fn)

        if total == 0:
            det_rate = 1.0
        else:
            det_rate = tp_count / total if total > 0 else 0.0

        if tp_count + fp_count == 0:
            prec = 1.0 if total == 0 else 0.0
            fdr = 0.0
        else:
            prec = tp_count / (tp_count + fp_count)
            fdr = fp_count / (tp_count + fp_count)

        if total == 0:
            leak_rate = 0.0
        else:
            leak_rate = fn_count / total if total > 0 else 0.0

        self._summary = (
            f"Detected {tp_count}/{total} ({det_rate:.0%}), "
            f"precision {prec:.0%}, "
            f"leaked {fn_count}/{total} ({leak_rate:.0%})"
        )

        return ScoringReport(
            scenario_id=scenario_id,
            run_id=self.run_id,
            total_injected=total,
            true_positives=tp_count,
            false_positives=fp_count,
            false_negatives=fn_count,
            detection_rate=det_rate,
            precision=prec,
            false_discovery_rate=fdr,
            leaked_to_gold=fn_count,
            leakage_rate=leak_rate,
            details={
                "tp": tp[:5],
                "fp": fp[:5],
                "fn": fn[:5],
            },
            summary=self._summary,
        )

    def score_full(
        self,
        manifest_path: str | Path,
        *,
        bronze_validation: dict[str, Any] | None = None,
        audit_results: list[dict[str, Any]] | None = None,
        quarantine_root: str | Path | None = None,
        detected_errors: list[dict[str, Any]] | None = None,
        recovery_log_path: str | Path | None = None,
    ) -> ScoringReport:
        """Score detection + recovery: includes repaired/leakage metrics.

        Reads ``recovery_log.json`` from the scenario directory to extract
        recovery stats, then merges them into the detection report.

        Parameters
        ----------
        manifest_path: Path to injection_manifest.json or scenario root dir.
        recovery_log_path: Path to recovery_log.json.
            If None, inferred from manifest_path parent.
        """
        # Step 1: Detection scoring
        report = self.score_detection(
            manifest_path,
            bronze_validation=bronze_validation,
            audit_results=audit_results,
            quarantine_root=quarantine_root,
            detected_errors=detected_errors,
        )

        # Step 2: Load recovery log
        if recovery_log_path is None:
            manifest_dir = Path(manifest_path)
            if manifest_dir.is_dir():
                recovery_log_path = manifest_dir / "recovery_log.json"
            else:
                recovery_log_path = manifest_dir.parent / "recovery_log.json"

        rpath = Path(recovery_log_path)
        if not rpath.exists():
            report.summary += " | recovery_log not found"
            return report

        try:
            recovery_log = json.loads(rpath.read_text())
        except (json.JSONDecodeError, OSError):
            report.summary += " | recovery_log unreadable"
            return report

        stats = recovery_log.get("stats", {})
        repaired = stats.get("repaired", 0)
        repair_candidates = stats.get("repair_candidates", 0)
        unrecoverable = stats.get("unrecoverable", 0)
        total_quarantined = stats.get("total_quarantined", 0)

        # Safe division — only repaired (auto-applied) counts as recovered
        total_injected = report.total_injected
        tp = report.true_positives

        repair_rate_on_detected = repaired / tp if tp > 0 else 0.0
        end_to_end_recovery_rate = repaired / total_injected if total_injected > 0 else 1.0

        report.repaired = repaired
        report.repair_candidates = repair_candidates
        report.unrecoverable = unrecoverable
        report.total_quarantined = total_quarantined
        report.repair_rate_on_detected = repair_rate_on_detected
        report.end_to_end_recovery_rate = end_to_end_recovery_rate
        report.details["recovery"] = {
            "repaired": repaired,
            "repair_candidates": repair_candidates,
            "unrecoverable": unrecoverable,
            "total_quarantined": total_quarantined,
            "repair_rate_on_detected": round(repair_rate_on_detected, 4),
            "end_to_end_recovery_rate": round(end_to_end_recovery_rate, 4),
        }

        # Update summary — only repaired counts
        report.summary = (
            f"Detected {report.true_positives}/{total_injected} "
            f"({report.detection_rate:.0%}), "
            f"precision {report.precision:.0%}, "
            f"recovered {repaired}/{total_injected} "
            f"({end_to_end_recovery_rate:.0%}), "
            f"candidates {repair_candidates}, "
            f"leaked {report.false_negatives}/{total_injected} "
            f"({report.leakage_rate:.0%})"
        )

        return report

    def write_report(self, report: ScoringReport, scenario_root: str | Path) -> Path:
        """Write scoring_report.json to scenario directory."""
        path = Path(scenario_root) / "scoring_report.json"
        path.write_text(json.dumps(report.to_dict(), indent=2, default=str), encoding="utf-8")
        return path
