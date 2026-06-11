"""
TPC-DI Benchmark Evaluator — comprehensive multi-category scoring.

Aggregates error-injection scenario results across all 7 categories
of the 35-error taxonomy to produce a unified benchmark report.

Usage::

    from benchmark.tpcdi.evaluator import BenchmarkEvaluator

    evaluator = BenchmarkEvaluator()
    report = evaluator.run_full_benchmark(scenario_runner, require_source=False)
    print(evaluator.format_report(report))
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from benchmark.tpcdi.scenario_runner import TpcdiScenarioRunner

from common.tpcdi_sources import resolve_scale_factor

# ═══════════════════════════════════════════════════════════════════════════
# Error Taxonomy — all 35 error types grouped by category
# ═══════════════════════════════════════════════════════════════════════════

SCENARIO_CATALOG: dict[str, list[dict[str, Any]]] = {
    "schema": [
        {"error": "missing_field", "mutation_type": "missing_field", "target_source": "trade",
         "description": "Missing Field — nguồn thiếu field so với schema", "injector": "source"},
        {"error": "extra_field", "mutation_type": "extra_field", "target_source": "trade",
         "description": "New Field Appears — nguồn tự thêm field mới", "injector": "source"},
        {"error": "downstream_field_removed", "mutation_type": "missing_field", "target_source": "trade",
         "description": "Used Field Removed — field đang dùng bị xóa", "injector": "schema", "schema_mutation": "remove_downstream_field"},
        {"error": "field_renamed", "mutation_type": "rename_to_synonym", "target_source": "trade",
         "description": "Field Renamed — field bị đổi tên", "injector": "semantic"},
        {"error": "type_changed", "mutation_type": "type_error", "target_source": "trade",
         "description": "Data Type Changed — kiểu dữ liệu thay đổi", "injector": "source"},
    ],
    "semantic": [
        {"error": "same_name_different_meaning", "mutation_type": "same_name_different_meaning", "target_source": "trade",
         "description": "Same Name - Different Meaning", "injector": "semantic"},
        {"error": "different_name_same_meaning", "mutation_type": "rename_to_synonym", "target_source": "trade",
         "description": "Different Name - Same Meaning", "injector": "semantic"},
        {"error": "unit_changed", "mutation_type": "unit_changed", "target_source": "trade",
         "description": "Different Units — cùng chỉ số khác đơn vị", "injector": "semantic"},
        {"error": "timestamp_format_changed", "mutation_type": "timestamp_format_changed", "target_source": "trade",
         "description": "Different Timestamp Formats", "injector": "semantic"},
        {"error": "timestamp_granularity_changed", "mutation_type": "timestamp_granularity_changed", "target_source": "trade",
         "description": "Different Time Granularity", "injector": "semantic"},
        {"error": "different_spatial_ref", "mutation_type": "different_spatial_ref", "target_source": "trade",
         "description": "Different Spatial Reference Systems", "injector": "semantic"},
        {"error": "pre_aggregate_records", "mutation_type": "pre_aggregate_records", "target_source": "trade",
         "description": "Different Aggregation Levels", "injector": "semantic"},
        {"error": "different_business_definitions", "mutation_type": "different_business_definitions", "target_source": "trade",
         "description": "Different Business Definitions", "injector": "semantic"},
        {"error": "entity_id_ambiguity", "mutation_type": "entity_id_ambiguity", "target_source": "trade",
         "description": "Entity Resolution Problem", "injector": "semantic"},
    ],
    "quality": [
        {"error": "invalid_format", "mutation_type": "invalid_format", "target_source": "trade",
         "description": "Invalid Data — dữ liệu không hợp lệ", "injector": "source"},
        {"error": "outlier_value", "mutation_type": "outlier_value", "target_source": "trade",
         "description": "Outlier — giá trị bất thường", "injector": "source"},
        {"error": "null_required_field", "mutation_type": "null_required_field", "target_source": "trade",
         "description": "Missing Data — thiếu giá trị", "injector": "source"},
        {"error": "duplicate_pk", "mutation_type": "duplicate_pk", "target_source": "trade",
         "description": "Duplicate Data — trùng bản ghi", "injector": "source"},
        {"error": "cross_source_inconsistency", "mutation_type": "cross_source_inconsistency", "target_source": "trade",
         "description": "Cross-source Inconsistency", "injector": "semantic"},
    ],
    "heterogeneity": [
        {"error": "csv_to_json", "mutation_type": "csv_to_json", "target_source": "trade",
         "description": "Different Data Formats", "injector": "format"},
        {"error": "flat_to_nested", "mutation_type": "flat_to_nested", "target_source": "trade",
         "description": "Different Data Models", "injector": "format"},
        {"error": "mock_rest_adapter", "mutation_type": "mock_rest_adapter", "target_source": "trade",
         "description": "Different Protocols", "injector": "config"},
        {"error": "batch_frequency_mismatch", "mutation_type": "batch_frequency_mismatch", "target_source": "trade",
         "description": "Different Update Frequencies", "injector": "config"},
        {"error": "split_batch_to_microfiles", "mutation_type": "split_batch_to_microfiles", "target_source": "trade",
         "description": "Different Ingestion Modes", "injector": "format"},
    ],
    "lineage": [
        {"error": "suppress_lineage_emission", "mutation_type": "suppress_lineage_emission", "target_source": "trade",
         "description": "Source→Bronze Lineage Missing", "injector": "lineage"},
        {"error": "suppress_transform_lineage", "mutation_type": "suppress_transform_lineage", "target_source": "trade",
         "description": "Transform Lineage Missing", "injector": "lineage"},
        {"error": "corrupt_audit_run_id", "mutation_type": "corrupt_audit_run_id", "target_source": "trade",
         "description": "Missing Debug/Audit Metadata", "injector": "lineage"},
        {"error": "break_downstream_impact", "mutation_type": "break_downstream_impact", "target_source": "trade",
         "description": "Downstream Impact Unknown", "injector": "lineage"},
        {"error": "corrupt_quarantine_metadata", "mutation_type": "corrupt_quarantine_metadata", "target_source": "trade",
         "description": "Quarantine Record Untraceable", "injector": "lineage"},
        {"error": "split_emission_targets", "mutation_type": "split_emission_targets", "target_source": "trade",
         "description": "Fragmented Lineage Across Tools", "injector": "lineage"},
    ],
    "reliability": [
        {"error": "simulate_api_failure", "mutation_type": "simulate_api_failure", "target_source": "trade",
         "description": "API Failure & Timeout", "injector": "config"},
        {"error": "rate_limit_partial_batch", "mutation_type": "rate_limit_partial_batch", "target_source": "trade",
         "description": "Rate Limit Handling", "injector": "reliability"},
        {"error": "partial_file", "mutation_type": "partial_file", "target_source": "trade",
         "description": "Partial Download", "injector": "reliability"},
        {"error": "duplicate_batch", "mutation_type": "duplicate_batch", "target_source": "trade",
         "description": "Retry Duplicate / Non-idempotent Write", "injector": "reliability"},
        {"error": "atomic_write_failure", "mutation_type": "atomic_write_failure", "target_source": "trade",
         "description": "Atomic Write", "injector": "reliability"},
        {"error": "batch_frequency_mismatch_outoforder", "mutation_type": "batch_frequency_mismatch", "target_source": "trade",
         "description": "Late-arriving / Out-of-order Data", "injector": "config",
         "extra_kwargs": {"mismatch_type": "out_of_order"}},
        {"error": "poison_record", "mutation_type": "poison_record", "target_source": "trade",
         "description": "Poison Record / Bad Message", "injector": "reliability"},
    ],
}

CATEGORY_LABELS: dict[str, str] = {
    "schema": "Schema Issues",
    "semantic": "Semantic Issues",
    "quality": "Data Quality",
    "heterogeneity": "Source Heterogeneity",
    "lineage": "Lineage Issues",
    "reliability": "Reliability & Failure Handling",
}

CATEGORY_WEIGHTS: dict[str, float] = {
    "schema": 0.20,
    "semantic": 0.15,
    "quality": 0.20,
    "heterogeneity": 0.10,
    "lineage": 0.15,
    "reliability": 0.20,
}


# ═══════════════════════════════════════════════════════════════════════════
# Report dataclass
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ScenarioResult:
    scenario_id: str = ""
    category: str = ""
    error_type: str = ""
    description: str = ""
    status: str = "skipped"  # passed | failed | skipped | error
    run_mode: str = "simulated"  # live | simulated
    detection_rate: float = 0.0
    precision: float = 0.0
    total_injected: int = 0
    true_positives: int = 0
    false_positives: int = 0
    false_negatives: int = 0
    recovery_rate: float = 0.0
    repaired: int = 0
    error_message: str = ""

    @property
    def f1_score(self) -> float:
        if self.true_positives + self.false_positives + self.false_negatives == 0:
            return 1.0 if self.total_injected == 0 else 0.0
        p = self.precision if self.precision > 0 else 0.0
        r = self.detection_rate if self.detection_rate > 0 else 0.0
        if p + r == 0:
            return 0.0
        return 2 * p * r / (p + r)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "category": self.category,
            "error_type": self.error_type,
            "description": self.description,
            "status": self.status,
            "run_mode": self.run_mode,
            "detection_rate": round(self.detection_rate, 4),
            "precision": round(self.precision, 4),
            "f1_score": round(self.f1_score, 4),
            "total_injected": self.total_injected,
            "true_positives": self.true_positives,
            "false_positives": self.false_positives,
            "false_negatives": self.false_negatives,
            "recovery_rate": round(self.recovery_rate, 4) if self.recovery_rate else 0.0,
            "repaired": self.repaired,
        }


@dataclass
class CategoryScore:
    category: str = ""
    label: str = ""
    weight: float = 0.0
    total_scenarios: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    avg_detection_rate: float = 0.0
    avg_precision: float = 0.0
    avg_f1: float = 0.0
    avg_recovery_rate: float = 0.0
    weighted_score: float = 0.0
    scenarios: list[ScenarioResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "label": self.label,
            "weight": self.weight,
            "total_scenarios": self.total_scenarios,
            "passed": self.passed,
            "failed": self.failed,
            "skipped": self.skipped,
            "coverage": round(self.passed / self.total_scenarios, 4) if self.total_scenarios > 0 else 0.0,
            "avg_detection_rate": round(self.avg_detection_rate, 4),
            "avg_precision": round(self.avg_precision, 4),
            "avg_f1": round(self.avg_f1, 4),
            "avg_recovery_rate": round(self.avg_recovery_rate, 4),
            "weighted_score": round(self.weighted_score, 4),
            "scenarios": [s.to_dict() for s in self.scenarios],
        }


@dataclass
class BenchmarkReport:
    run_mode: str = "simulated"
    total_categories: int = 0
    total_scenarios: int = 0
    total_passed: int = 0
    total_failed: int = 0
    total_skipped: int = 0
    overall_score: float = 0.0
    overall_detection_rate: float = 0.0
    overall_precision: float = 0.0
    overall_f1: float = 0.0
    categories: list[CategoryScore] = field(default_factory=list)
    summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_mode": self.run_mode,
            "total_categories": self.total_categories,
            "total_scenarios": self.total_scenarios,
            "total_passed": self.total_passed,
            "total_failed": self.total_failed,
            "total_skipped": self.total_skipped,
            "overall_score": round(self.overall_score, 4),
            "overall_detection_rate": round(self.overall_detection_rate, 4),
            "overall_precision": round(self.overall_precision, 4),
            "overall_f1": round(self.overall_f1, 4),
            "categories": [c.to_dict() for c in self.categories],
            "summary": self.summary,
        }


# ═══════════════════════════════════════════════════════════════════════════
# Benchmark Evaluator
# ═══════════════════════════════════════════════════════════════════════════

class BenchmarkEvaluator:
    """Run and score all 35 error scenarios across 7 categories."""

    def __init__(self, require_source: bool = False, scale_factor: int = 3):
        self.require_source = require_source
        self.scale_factor = resolve_scale_factor(scale_factor)
        self.results: list[ScenarioResult] = []

    def run_full_benchmark(
        self,
        scenario_runner: TpcdiScenarioRunner | None = None,
    ) -> BenchmarkReport:
        """Run all scenarios across all categories.

        Parameters
        ----------
        scenario_runner:
            For live-mode runs, an instance of TpcdiScenarioRunner.
            When None, runs in simulated mode using detector validation.
        """
        self.results = []
        run_mode = "live" if scenario_runner else "simulated"
        old_scale = os.environ.get("TPCDI_SCALE_FACTOR")
        os.environ["TPCDI_SCALE_FACTOR"] = str(self.scale_factor)

        try:
            for category, scenarios in SCENARIO_CATALOG.items():
                for scenario_def in scenarios:
                    result = self._run_one_scenario(
                        category, scenario_def, scenario_runner
                    )
                    self.results.append(result)

            return self._build_report(run_mode)
        finally:
            if old_scale is None:
                os.environ.pop("TPCDI_SCALE_FACTOR", None)
            else:
                os.environ["TPCDI_SCALE_FACTOR"] = old_scale

    def _run_one_scenario(
        self,
        category: str,
        scenario_def: dict[str, Any],
        runner: TpcdiScenarioRunner | None,
    ) -> ScenarioResult:
        error_type = scenario_def["error"]
        description = scenario_def["description"]
        injector_type = scenario_def["injector"]
        scenario_id = f"bench_{category}_{error_type}"

        result = ScenarioResult(
            scenario_id=scenario_id,
            category=category,
            error_type=error_type,
            description=description,
        )

        if runner is None:
            return self._run_simulated(result, scenario_def, scenario_id)

        # Live mode — run through scenario_runner
        return self._run_live(result, scenario_def, scenario_id, runner, injector_type)

    def _run_simulated(
        self,
        result: ScenarioResult,
        scenario_def: dict[str, Any],
        scenario_id: str,
    ) -> ScenarioResult:
        """Simulate detection by running the injector + error_collector."""
        from pathlib import Path
        from datetime import datetime, timezone

        try:
            # Run the appropriate injector
            source_root = _get_source_root()
            if source_root is None or not Path(source_root).exists():
                result.status = "skipped"
                result.run_mode = "simulated"
                result.error_message = "Source root unavailable — run generate first"
                return result

            injector_type = scenario_def["injector"]
            mutation_type = scenario_def["mutation_type"]
            target_source = scenario_def["target_source"]
            batch_id = scenario_def.get("batch_id", "batch1")
            extra_kwargs = scenario_def.get("extra_kwargs", {})
            # Schema mutation may be at top level of scenario_def
            schema_mutation = scenario_def.get("schema_mutation")
            if schema_mutation:
                extra_kwargs = dict(extra_kwargs)
                extra_kwargs["schema_mutation"] = schema_mutation

            path = _create_simulated_scenario(
                injector_type, scenario_id, mutation_type, target_source, batch_id, extra_kwargs
            )

            if path is None:
                result.status = "skipped"
                result.run_mode = "simulated"
                result.error_message = f"Injector {injector_type} could not create scenario"
                return result

            # Run detection via error_collector
            from ingestion.tpcdi.error_injection.error_collector import collect_detected_errors
            from ingestion.tpcdi.error_injection.manifest_reader import load_manifest

            run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            manifest = load_manifest(path)
            mutations = manifest.get("mutations", [])
            total_injected = len([m for m in mutations if "skipped" not in m])

            # Set source root to scenario source for detection
            source_dir = Path(path) / "source"
            old_root = os.environ.get("TPCDI_SOURCE_ROOT")
            os.environ["TPCDI_SOURCE_ROOT"] = str(source_dir)

            try:
                bronze = None
                if injector_type == "source":
                    # In simulated mode, skip expensive bronze validation.
                    # Detection rate is inferred from the manifest's expected
                    # detections (the injector recorded them as self-evident).
                    pass

                detected = collect_detected_errors(
                    scenario_id, run_id, bronze_validation=bronze
                )
            finally:
                if old_root is not None:
                    os.environ["TPCDI_SOURCE_ROOT"] = old_root
                elif "TPCDI_SOURCE_ROOT" in os.environ:
                    del os.environ["TPCDI_SOURCE_ROOT"]

            # Score detection
            tp, fp, fn = _score_detections(mutations, detected)
            det_rate = tp / total_injected if total_injected > 0 else 1.0
            prec = tp / (tp + fp) if (tp + fp) > 0 else (1.0 if total_injected == 0 else 0.0)

            result.total_injected = total_injected
            result.true_positives = tp
            result.false_positives = fp
            result.false_negatives = fn
            result.detection_rate = det_rate
            result.precision = prec
            result.status = "passed" if det_rate >= 0.5 else "failed"
            result.run_mode = "simulated"

        except Exception as e:
            result.status = "error"
            result.run_mode = "simulated"
            result.error_message = str(e)

        return result

    def _run_live(
        self,
        result: ScenarioResult,
        scenario_def: dict[str, Any],
        scenario_id: str,
        runner: TpcdiScenarioRunner,
        injector_type: str,
    ) -> ScenarioResult:
        """Run scenario through scenario_runner for end-to-end scoring."""
        try:
            mutation_type = scenario_def["mutation_type"]
            target_source = scenario_def["target_source"]
            extra_kwargs = scenario_def.get("extra_kwargs", {})
            schema_mutation = scenario_def.get("schema_mutation")

            if schema_mutation:
                extra_kwargs["schema_mutation"] = schema_mutation

            output = runner.run_scenario(
                scenario_id=scenario_id,
                mutation_type=mutation_type,
                target_source=target_source,
                injector_type=injector_type,
                line_numbers=[100, 200, 300],
                seed=42,
                recover=True,
                extra_kwargs=extra_kwargs,
            )

            report = output.get("scoring_report", {})
            det = report.get("detection", {})
            rec = report.get("recovery", {})
            total = report.get("total_injected", 0)

            result.total_injected = total
            result.true_positives = det.get("true_positives", 0)
            result.false_positives = det.get("false_positives", 0)
            result.false_negatives = det.get("false_negatives", 0)
            result.detection_rate = det.get("detection_rate", 0.0)
            result.precision = det.get("precision", 0.0)
            result.recovery_rate = rec.get("end_to_end_recovery_rate", 0.0)
            result.repaired = rec.get("repaired", 0)
            result.status = "passed" if result.detection_rate >= 0.5 else "failed"
            result.run_mode = "live"

        except Exception as e:
            result.status = "error"
            result.run_mode = "live"
            result.error_message = str(e)

        return result

    def _build_report(self, run_mode: str) -> BenchmarkReport:
        """Aggregate all scenario results into a comprehensive report."""
        categories: list[CategoryScore] = []
        total_passed = 0
        total_failed = 0
        total_skipped = 0
        weighted_sum = 0.0
        total_weight = 0.0

        for cat_key in SCENARIO_CATALOG:
            cat_scenarios = [r for r in self.results if r.category == cat_key]
            evaluated = [r for r in cat_scenarios if r.status != "skipped"]
            passed = sum(1 for r in cat_scenarios if r.status == "passed")
            failed = sum(1 for r in cat_scenarios if r.status == "failed")
            skipped = sum(1 for r in cat_scenarios if r.status == "skipped")

            total_passed += passed
            total_failed += failed
            total_skipped += skipped

            if evaluated:
                avg_dr = sum(r.detection_rate for r in evaluated) / len(evaluated)
                avg_prec = sum(r.precision for r in evaluated) / len(evaluated)
                avg_f1 = sum(r.f1_score for r in evaluated) / len(evaluated)
                avg_rec = sum(r.recovery_rate for r in evaluated) / len(evaluated)
            else:
                avg_dr = avg_prec = avg_f1 = avg_rec = 0.0

            weight = CATEGORY_WEIGHTS.get(cat_key, 0.1)
            cat_score = avg_dr  # detection rate as category score
            weighted = cat_score * weight
            weighted_sum += weighted
            total_weight += weight

            categories.append(CategoryScore(
                category=cat_key,
                label=CATEGORY_LABELS.get(cat_key, cat_key),
                weight=weight,
                total_scenarios=len(cat_scenarios),
                passed=passed,
                failed=failed,
                skipped=skipped,
                avg_detection_rate=avg_dr,
                avg_precision=avg_prec,
                avg_f1=avg_f1,
                avg_recovery_rate=avg_rec,
                weighted_score=weighted,
                scenarios=cat_scenarios,
            ))

        overall_score = weighted_sum / total_weight if total_weight > 0 else 0.0

        all_evaluated = [r for r in self.results if r.status != "skipped" and r.status != "error"]
        if all_evaluated:
            overall_dr = sum(r.detection_rate for r in all_evaluated) / len(all_evaluated)
            overall_prec = sum(r.precision for r in all_evaluated) / len(all_evaluated)
            overall_f1 = sum(r.f1_score for r in all_evaluated) / len(all_evaluated)
        else:
            overall_dr = overall_prec = overall_f1 = 0.0

        total_scenarios = sum(c.total_scenarios for c in categories)

        summary = (
            f"Benchmark: {total_passed}/{total_scenarios} passed, "
            f"score={overall_score:.2%}, "
            f"detection={overall_dr:.2%}, F1={overall_f1:.2%}"
        )

        return BenchmarkReport(
            run_mode=run_mode,
            total_categories=len(categories),
            total_scenarios=total_scenarios,
            total_passed=total_passed,
            total_failed=total_failed,
            total_skipped=total_skipped,
            overall_score=overall_score,
            overall_detection_rate=overall_dr,
            overall_precision=overall_prec,
            overall_f1=overall_f1,
            categories=categories,
            summary=summary,
        )

    def format_report(self, report: BenchmarkReport) -> str:
        """Format a human-readable benchmark report string."""
        lines = []
        lines.append("=" * 70)
        lines.append("  NEXUS BENCHMARK REPORT — 35-Error Taxonomy")
        lines.append("=" * 70)
        lines.append(f"  Mode: {report.run_mode}")
        lines.append(f"  Overall Score:     {report.overall_score:.1%}")
        lines.append(f"  Detection Rate:    {report.overall_detection_rate:.1%}")
        lines.append(f"  Precision:         {report.overall_precision:.1%}")
        lines.append(f"  F1 Score:          {report.overall_f1:.1%}")
        lines.append(f"  Scenarios:         {report.total_passed}/{report.total_scenarios} passed, "
                     f"{report.total_failed} failed, {report.total_skipped} skipped")
        lines.append("")

        lines.append(f"  {'Category':<35s} {'Scenarios':>9s} {'Detect':>8s} {'F1':>7s} {'Score':>7s}")
        lines.append(f"  {'-'*35} {'-'*9} {'-'*8} {'-'*7} {'-'*7}")
        for cat in report.categories:
            pct = f"{cat.passed}/{cat.total_scenarios}"
            lines.append(
                f"  {cat.label:<35s} {pct:>9s} "
                f"{cat.avg_detection_rate:>7.1%} "
                f"{cat.avg_f1:>7.1%} "
                f"{cat.weighted_score:>7.1%}"
            )

        lines.append("")
        lines.append("  Legend: Score = Detection Rate × Category Weight")
        lines.append("=" * 70)
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════════════════

def _get_source_root() -> str | None:
    """Get TPCDI_SOURCE_ROOT from environment."""
    import os
    from common.tpcdi_sources import source_root as tpcdi_source_root
    root = os.environ.get("TPCDI_SOURCE_ROOT")
    if root and Path(root).exists():
        return root
    if tpcdi_source_root().exists():
        return str(tpcdi_source_root())
    return None


def _create_simulated_scenario(
    injector_type: str,
    scenario_id: str,
    mutation_type: str,
    target_source: str,
    batch_id: str,
    extra_kwargs: dict[str, Any],
) -> Path | None:
    """Create a scenario using the appropriate injector for simulated detection."""
    try:
        if injector_type == "semantic":
            from ingestion.tpcdi.error_injection.semantic_injector import SemanticInjector
            inj = SemanticInjector(seed=42)
            return inj.create_scenario(
                scenario_id, target_source=target_source,
                batch_id=batch_id, mutation_type=mutation_type, **extra_kwargs
            )
        elif injector_type == "format":
            from ingestion.tpcdi.error_injection.format_injector import FormatInjector
            inj = FormatInjector(seed=42)
            return inj.create_scenario(
                scenario_id, target_source=target_source,
                batch_id=batch_id, mutation_type=mutation_type, **extra_kwargs
            )
        elif injector_type == "lineage":
            from ingestion.tpcdi.error_injection.lineage_injector import LineageInjector
            inj = LineageInjector(seed=42)
            return inj.create_scenario(
                scenario_id, mutation_type=mutation_type, **extra_kwargs
            )
        elif injector_type == "config":
            from ingestion.tpcdi.error_injection.config_injector import ConfigInjector
            inj = ConfigInjector(seed=42)
            return inj.create_scenario(
                scenario_id, mutation_type=mutation_type,
                target_source=target_source, batch_id=batch_id, **extra_kwargs
            )
        elif injector_type == "reliability":
            from ingestion.tpcdi.error_injection.reliability_injector import ReliabilityInjector
            inj = ReliabilityInjector(seed=42)
            return inj.create_scenario(
                scenario_id, target_source=target_source,
                batch_id=batch_id, mutation_type=mutation_type, **extra_kwargs
            )
        elif injector_type == "source":
            from ingestion.tpcdi.error_injection.source_injector import TpcdiSourceInjector
            inj = TpcdiSourceInjector(seed=42)
            return inj.create_scenario(
                scenario_id, target_source=target_source,
                batch_id=batch_id, mutation_type=mutation_type
            )
        elif injector_type == "schema":
            from ingestion.tpcdi.error_injection.schema_injector import SchemaInjector
            inj = SchemaInjector(seed=42)
            schema_mutation = extra_kwargs.pop("schema_mutation", mutation_type)
            return inj.create_scenario(
                scenario_id, target_source=target_source,
                batch_id=batch_id, mutation_type=mutation_type,
                schema_mutation=schema_mutation, **extra_kwargs
            )
    except Exception:
        return None
    return None


def _score_detections(
    mutations: list[dict[str, Any]],
    detected_errors: list[dict[str, Any]],
) -> tuple[int, int, int]:
    """Match mutations to detected errors and compute TP/FP/FN.

    Returns (true_positives, false_positives, false_negatives).
    """
    # Filter out skipped mutations (not actually injected)
    active_mutations = [m for m in mutations if "skipped" not in m]
    if not active_mutations:
        # If all mutations were skipped, treat as 0 injected = pass
        return 0, len(detected_errors), 0

    unmatched = list(detected_errors)
    tp = 0

    for mut in active_mutations:
        mid = mut.get("mutation_id")
        expected = mut.get("expected_detection", "")
        found = False

        for i, det in enumerate(unmatched):
            if mid and det.get("mutation_id") == mid:
                unmatched.pop(i)
                found = True
                break
            if det.get("error_type") == expected:
                unmatched.pop(i)
                found = True
                break

        if found:
            tp += 1

    fn = len(mutations) - tp
    fp = len(unmatched)
    return tp, fp, fn


__all__ = [
    "BenchmarkEvaluator",
    "BenchmarkReport",
    "CategoryScore",
    "ScenarioResult",
    "SCENARIO_CATALOG",
    "CATEGORY_WEIGHTS",
]
