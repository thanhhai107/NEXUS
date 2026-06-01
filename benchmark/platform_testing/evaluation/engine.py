"""Evaluation Engine — orchestrates metric computation across all capabilities.

Loads platform outputs and injection logs, computes all 8 capability metrics,
generates per-scenario scorecards and aggregate reports.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from benchmark.platform_testing.evaluation.metrics import (
    MetricResult,
    schema_discovery_metrics,
    schema_drift_metrics,
    semantic_mapping_metrics,
    join_discovery_metrics,
    entity_resolution_metrics,
    data_quality_metrics,
    lineage_completeness_metrics,
    reliability_metrics,
    compute_aggregate_score,
)
from benchmark.utils.io import load_scenario_config, save_scorecard


class EvaluationEngine:
    """Orchestrates evaluation across all capabilities and scenarios."""

    def __init__(self, ground_truth_path: Path | None = None):
        self.ground_truth: dict[str, Any] = {}
        if ground_truth_path and ground_truth_path.exists():
            self.ground_truth = json.loads(ground_truth_path.read_text(encoding="utf-8"))

    def evaluate_scenario(
        self,
        scenario_id: str,
        platform_outputs: dict[str, Any],
        injection_log_path: Path | None = None,
    ) -> dict[str, Any]:
        injection_log = []
        if injection_log_path and injection_log_path.exists():
            injection_log = json.loads(injection_log_path.read_text(encoding="utf-8"))

        all_metrics: list[MetricResult] = []

        # 1. Schema Discovery
        if "schemas" in platform_outputs:
            gt_schemas = self.ground_truth.get("schemas", {})
            all_metrics.extend(schema_discovery_metrics(platform_outputs["schemas"], gt_schemas))

        # 2. Schema Drift Detection
        if "schema_drifts" in platform_outputs and injection_log:
            all_metrics.extend(schema_drift_metrics(platform_outputs["schema_drifts"], injection_log))

        # 3. Semantic Mapping
        if "semantics" in platform_outputs:
            gt_semantics = self.ground_truth.get("semantics", {})
            all_metrics.extend(semantic_mapping_metrics(platform_outputs["semantics"], gt_semantics, injection_log))

        # 4. Join Discovery
        if "foreign_keys" in platform_outputs:
            gt_fks = self.ground_truth.get("relationships", {}).get("foreign_keys", {})
            all_metrics.extend(join_discovery_metrics(platform_outputs["foreign_keys"], gt_fks))

        # 5. Entity Resolution
        if "entity_clusters" in platform_outputs or "entities" in platform_outputs:
            gt_entities = self.ground_truth.get("entities", {})
            all_metrics.extend(entity_resolution_metrics(
                platform_outputs.get("entity_clusters", []), gt_entities
            ))

        # 6. Data Quality
        if "quality_results" in platform_outputs and injection_log:
            all_metrics.extend(data_quality_metrics(platform_outputs["quality_results"], injection_log))

        # 7. Lineage Completeness
        if "lineage" in platform_outputs:
            all_metrics.extend(lineage_completeness_metrics(platform_outputs["lineage"]))

        # 8. Reliability
        if "reliability" in platform_outputs and injection_log:
            all_metrics.extend(reliability_metrics(platform_outputs["reliability"], injection_log))

        aggregate = compute_aggregate_score(all_metrics)

        scorecard: dict[str, Any] = {
            "scenario_id": scenario_id,
            "metrics": [m.to_dict() for m in all_metrics],
            "aggregate": aggregate,
        }

        save_scorecard(scenario_id, scorecard)
        return scorecard

    def evaluate_all(
        self,
        scenario_ids: list[str],
        platform_outputs_map: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        report: dict[str, Any] = {
            "scenarios": {},
            "summary": {},
        }

        for sid in scenario_ids:
            try:
                log_path = Path(f"benchmark/reports/{sid}/injection_log.json")
                outputs = platform_outputs_map.get(sid, {})
                scorecard = self.evaluate_scenario(sid, outputs, log_path)
                report["scenarios"][sid] = scorecard
            except Exception as exc:
                report["scenarios"][sid] = {"error": str(exc)}

        all_f1s = []
        for sid, sc in report["scenarios"].items():
            if isinstance(sc, dict):
                agg = sc.get("aggregate", {})
                all_f1s.append(agg.get("overall_f1_score", 0.0))

        report["summary"] = {
            "scenarios_evaluated": len(report["scenarios"]),
            "scenarios_with_errors": sum(1 for s in report["scenarios"].values() if isinstance(s, dict) and "error" in s),
            "mean_f1_score": round(sum(all_f1s) / len(all_f1s), 4) if all_f1s else 0.0,
            "min_f1_score": round(min(all_f1s), 4) if all_f1s else 0.0,
            "max_f1_score": round(max(all_f1s), 4) if all_f1s else 0.0,
        }

        report_path = Path("benchmark/reports/aggregate_report.json")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, default=str))

        return report
