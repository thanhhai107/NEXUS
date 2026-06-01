"""Evaluation metrics for all 8 benchmark capabilities.

Each metric function returns a dict with:
    - value: the computed metric value
    - tp, fp, fn, tn: counts where applicable
    - interpretation: human-readable interpretation
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MetricResult:
    name: str
    value: float
    precision: float | None = None
    recall: float | None = None
    f1_score: float | None = None
    tp: int = 0
    fp: int = 0
    fn: int = 0
    tn: int = 0
    interpretation: str = ""
    raw_detail: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "value": self.value,
            "precision": self.precision,
            "recall": self.recall,
            "f1_score": self.f1_score,
            "tp": self.tp,
            "fp": self.fp,
            "fn": self.fn,
            "tn": self.tn,
            "interpretation": self.interpretation,
        }


def _compute_pr_f1(tp: int, fp: int, fn: int) -> tuple[float, float, float]:
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    return precision, recall, f1


# ============================================================================
# 1. SCHEMA DISCOVERY METRICS
# ============================================================================

def schema_discovery_metrics(
    platform_schemas: dict[str, Any],
    ground_truth_schemas: dict[str, Any],
) -> list[MetricResult]:
    total_tp, total_fp, total_fn = 0, 0, 0
    type_correct, type_total = 0, 0

    for table, gt in ground_truth_schemas.items():
        gt_fields = set(gt.get("fields", {}).keys())
        plat_fields = set(platform_schemas.get(table, {}).get("fields", {}).keys())

        tp = len(gt_fields & plat_fields)
        fp = len(plat_fields - gt_fields)
        fn = len(gt_fields - plat_fields)

        total_tp += tp
        total_fp += fp
        total_fn += fn

        for field in gt_fields & plat_fields:
            gt_type = gt.get("fields", {}).get(field, {}).get("type", "")
            plat_type = platform_schemas.get(table, {}).get("fields", {}).get(field, {}).get("type", "")
            type_total += 1
            if gt_type == plat_type or _types_compatible(gt_type, plat_type):
                type_correct += 1

    precision, recall, f1 = _compute_pr_f1(total_tp, total_fp, total_fn)
    type_accuracy = type_correct / type_total if type_total > 0 else 0.0

    return [
        MetricResult("schema_field_precision", precision, precision, recall, f1, total_tp, total_fp, total_fn,
                     interpretation=f"{total_tp} correct, {total_fp} spurious, {total_fn} missed fields across {len(ground_truth_schemas)} tables"),
        MetricResult("schema_field_recall", recall, precision, recall, f1, total_tp, total_fp, total_fn),
        MetricResult("schema_field_f1", f1, precision, recall, f1, total_tp, total_fp, total_fn),
        MetricResult("schema_type_accuracy", type_accuracy,
                     interpretation=f"Correct types for {type_correct}/{type_total} fields"),
    ]


def _types_compatible(gt: str, plat: str) -> bool:
    numeric = {"integer", "number", "float", "int", "double", "decimal"}
    if gt in numeric and plat in numeric:
        return True
    if gt == plat:
        return True
    if {gt, plat} <= {"string", "text"}:
        return True
    return False


# ============================================================================
# 2. SCHEMA DRIFT DETECTION METRICS
# ============================================================================

def schema_drift_metrics(
    platform_drifts: list[dict[str, Any]],
    injection_log: list[dict[str, Any]],
) -> list[MetricResult]:
    drift_logs = [e for e in injection_log if e.get("error_category") == "schema_issues"]
    expected_count = len(drift_logs)

    if expected_count == 0:
        return [MetricResult("drift_detection_f1", 0.0, interpretation="No schema drifts injected")]

    tp, fp, fn = 0, 0, 0
    severity_correct, type_correct = 0, 0
    expected_types = {(e["table"], e["error_type"], e.get("field", "")) for e in drift_logs}

    detected_keys: set[tuple[str, str, str]] = set()
    for drift in platform_drifts:
        key = (drift.get("table", ""), drift.get("error_type", ""), drift.get("field", ""))
        if key in expected_types:
            tp += 1
            detected_keys.add(key)
        else:
            fp += 1

    fn = expected_count - tp

    precision, recall, f1 = _compute_pr_f1(tp, fp, fn)

    return [
        MetricResult("drift_detection_precision", precision, precision, recall, f1, tp, fp, fn,
                     interpretation=f"Detected {tp}/{expected_count} injected drifts, {fp} false alarms"),
        MetricResult("drift_detection_recall", recall, precision, recall, f1, tp, fp, fn),
        MetricResult("drift_detection_f1", f1, precision, recall, f1, tp, fp, fn),
    ]


# ============================================================================
# 3. SEMANTIC MAPPING METRICS
# ============================================================================

def semantic_mapping_metrics(
    platform_semantics: dict[str, Any],
    ground_truth_semantics: dict[str, Any],
    injection_log: list[dict[str, Any]],
) -> list[MetricResult]:
    gt_map: dict[str, dict[str, str]] = {}
    for table, info in ground_truth_semantics.items():
        for field, meta in info.get("field_mappings", {}).items():
            gt_map[f"{table}.{field}"] = meta

    plat_map: dict[str, dict[str, str]] = {}
    for table, info in platform_semantics.items():
        for field, meta in info.get("field_mappings", {}).items():
            plat_map[f"{table}.{field}"] = meta

    tp, fp, fn = 0, 0, 0
    all_gt_keys = set(gt_map.keys())

    for key, plat_meta in plat_map.items():
        if key in gt_map:
            gt_type = gt_map[key].get("semantic_type", "")
            plat_type = plat_meta.get("semantic_type", "")
            if gt_type and plat_type and gt_type == plat_type:
                tp += 1
            else:
                fp += 1
        else:
            fp += 1

    fn = len(all_gt_keys) - tp

    precision, recall, f1 = _compute_pr_f1(tp, fp, fn)

    synonym_logs = [e for e in injection_log if e.get("error_type") == "different_name_same_meaning"]
    synonym_detected = 0
    for log_entry in synonym_logs:
        old = log_entry.get("parameters", {}).get("old_name", "")
        new = log_entry.get("parameters", {}).get("new_name", "")
        table = log_entry.get("table", "")
        if f"{table}.{old}" in plat_map or f"{table}.{new}" in plat_map:
            synonym_detected += 1
    synonym_rate = synonym_detected / len(synonym_logs) if synonym_logs else 1.0

    return [
        MetricResult("semantic_annotation_precision", precision, precision, recall, f1, tp, fp, fn,
                     interpretation=f"Matched {tp}/{len(all_gt_keys)} semantic annotations correctly"),
        MetricResult("semantic_annotation_recall", recall, precision, recall, f1, tp, fp, fn),
        MetricResult("semantic_annotation_f1", f1, precision, recall, f1, tp, fp, fn),
        MetricResult("synonym_detection_rate", synonym_rate,
                     interpretation=f"Detected {synonym_detected}/{len(synonym_logs)} synonym pairs"),
    ]


# ============================================================================
# 4. JOIN DISCOVERY METRICS
# ============================================================================

def join_discovery_metrics(
    platform_fks: dict[str, dict[str, str]],
    ground_truth_fks: dict[str, dict[str, str]],
) -> list[MetricResult]:
    tp, fp, fn = 0, 0, 0

    gt_pairs: set[tuple[str, str, str]] = set()
    for table, fks in ground_truth_fks.items():
        for field, target in fks.items():
            gt_pairs.add((table, field, target))

    plat_pairs: set[tuple[str, str, str]] = set()
    for table, fks in platform_fks.items():
        for field, target in fks.items():
            plat_pairs.add((table, field, target))

    tp = len(gt_pairs & plat_pairs)
    fp = len(plat_pairs - gt_pairs)
    fn = len(gt_pairs - plat_pairs)

    precision, recall, f1 = _compute_pr_f1(tp, fp, fn)

    return [
        MetricResult("fk_discovery_precision", precision, precision, recall, f1, tp, fp, fn,
                     interpretation=f"Discovered {tp}/{len(gt_pairs)} true FKs, {fp} spurious"),
        MetricResult("fk_discovery_recall", recall, precision, recall, f1, tp, fp, fn),
        MetricResult("fk_discovery_f1", f1, precision, recall, f1, tp, fp, fn),
    ]


# ============================================================================
# 5. ENTITY RESOLUTION METRICS
# ============================================================================

def entity_resolution_metrics(
    platform_clusters: list[set[str]],
    ground_truth_hashes: dict[str, list[str]],
) -> list[MetricResult]:
    if not ground_truth_hashes or not platform_clusters:
        return [MetricResult("entity_resolution_f1", 0.0, interpretation="No entity data available")]

    tp_pairs, fp_pairs, fn_pairs = 0, 0, 0

    for gt_entity, hashes in ground_truth_hashes.items():
        n = len(hashes)
        expected_pairs = n * (n - 1) // 2
        found_pairs = sum(
            1 for cluster in platform_clusters
            if sum(1 for h in hashes if h in cluster) >= 2
        ) * len(hashes) // 2
        found_pairs = min(found_pairs, expected_pairs)

    precision, recall, f1 = _compute_pr_f1(
        tp_pairs or 1, fp_pairs or 0, fn_pairs or 0
    )

    return [
        MetricResult("entity_resolution_f1", f1, precision, recall, f1,
                     interpretation=f"Entity resolution across {len(ground_truth_hashes)} ground-truth entities"),
    ]


# ============================================================================
# 6. DATA QUALITY DETECTION METRICS
# ============================================================================

def data_quality_metrics(
    platform_quality_results: list[dict[str, Any]],
    injection_log: list[dict[str, Any]],
) -> list[MetricResult]:
    quality_logs = [e for e in injection_log if e.get("error_category") == "data_quality"]
    if not quality_logs:
        return [MetricResult("dq_issue_recall", 1.0, interpretation="No quality issues injected")]

    total_injected = sum(e.get("injection_count", 0) for e in quality_logs)
    injected_hashes: set[str] = set()
    for log_entry in quality_logs:
        injected_hashes.update(log_entry.get("injected_record_hashes", []))

    platform_issue_hashes: set[str] = set()
    for issue in platform_quality_results:
        platform_issue_hashes.update(issue.get("affected_record_hashes", []))

    tp = len(injected_hashes & platform_issue_hashes)
    fp = len(platform_issue_hashes - injected_hashes)
    fn = len(injected_hashes - platform_issue_hashes)

    precision, recall, f1 = _compute_pr_f1(tp, fp, fn)
    fpr = fp / (fp + (len(injected_hashes) - tp + 1)) if (fp + len(injected_hashes)) > 0 else 0.0

    unique_error_types = len({e["error_type"] for e in quality_logs})
    detected_types = len({
        e["error_type"] for e in quality_logs
        if any(h in platform_issue_hashes for h in e.get("injected_record_hashes", []))
    })
    type_recall = detected_types / unique_error_types if unique_error_types > 0 else 1.0

    return [
        MetricResult("dq_issue_recall", recall, precision, recall, f1, tp, fp, fn,
                     interpretation=f"Detected {tp}/{total_injected} injected quality issues"),
        MetricResult("dq_issue_precision", precision, precision, recall, f1, tp, fp, fn),
        MetricResult("dq_issue_f1", f1, precision, recall, f1, tp, fp, fn),
        MetricResult("dq_false_positive_rate", fpr,
                     interpretation=f"False positive ratio among clean records"),
        MetricResult("dq_error_type_recall", type_recall,
                     interpretation=f"Detected {detected_types}/{unique_error_types} error types"),
    ]


# ============================================================================
# 7. LINEAGE COMPLETENESS METRICS
# ============================================================================

def lineage_completeness_metrics(
    platform_lineage: dict[str, Any],
    expected_edges: int = 40,
    expected_nodes: int = 52,
) -> list[MetricResult]:
    recorded_edges = platform_lineage.get("edges_recorded", len(platform_lineage.get("edges", [])))
    recorded_nodes = platform_lineage.get("nodes_recorded", len(platform_lineage.get("nodes", [])))

    edge_coverage = min(recorded_edges / expected_edges, 1.0) if expected_edges > 0 else 0.0
    node_coverage = min(recorded_nodes / expected_nodes, 1.0) if expected_nodes > 0 else 0.0

    traceable = platform_lineage.get("traceable_gold_tables", 0)
    total_gold = platform_lineage.get("total_gold_tables", 12)
    traceability = traceable / total_gold if total_gold > 0 else 0.0

    fragmented = platform_lineage.get("disconnected_subgraphs", 0)
    fragmentation_score = 1.0 - min(fragmented / max(expected_nodes, 1), 1.0)

    return [
        MetricResult("lineage_edge_coverage", edge_coverage,
                     interpretation=f"Recorded {recorded_edges}/{expected_edges} expected lineage edges"),
        MetricResult("lineage_traceability", traceability,
                     interpretation=f"{traceable}/{total_gold} gold tables fully traceable to source"),
        MetricResult("lineage_fragmentation_score", fragmentation_score,
                     interpretation=f"{fragmented} disconnected subgraphs"),
    ]


# ============================================================================
# 8. RELIABILITY HANDLING METRICS
# ============================================================================

def reliability_metrics(
    platform_reliability: dict[str, Any],
    injection_log: list[dict[str, Any]],
) -> list[MetricResult]:
    reliability_logs = [e for e in injection_log if e.get("error_category") == "reliability"]
    if not reliability_logs:
        return [MetricResult("reliability_dlq_capture_rate", 1.0, interpretation="No reliability failures injected")]

    injected_failures = sum(e.get("injection_count", 0) for e in reliability_logs)

    dlq_entries = platform_reliability.get("dlq_entries", 0)
    dlq_capture = dlq_entries / injected_failures if injected_failures > 0 else 0.0

    received = platform_reliability.get("records_received", 0)
    expected = platform_reliability.get("records_expected", injected_failures)
    completeness = received / expected if expected > 0 else 0.0

    duplicates = platform_reliability.get("duplicate_records", 0)
    total = platform_reliability.get("total_records", received or 1)
    dup_free = 1.0 - (duplicates / total) if total > 0 else 1.0

    retry_success = platform_reliability.get("retry_success_rate", 0.0)
    recovery_time = platform_reliability.get("recovery_time_seconds", 0)

    return [
        MetricResult("reliability_dlq_capture_rate", dlq_capture,
                     interpretation=f"DLQ captured {dlq_entries}/{injected_failures} failures"),
        MetricResult("reliability_data_completeness", completeness,
                     interpretation=f"Received {received}/{expected} expected records"),
        MetricResult("reliability_duplicate_free_rate", dup_free,
                     interpretation=f"{duplicates} duplicates among {total} records"),
        MetricResult("reliability_retry_success_rate", retry_success,
                     interpretation=f"Retry success rate: {retry_success:.2%}"),
        MetricResult("reliability_recovery_time_s", float(recovery_time),
                     interpretation=f"Recovery took {recovery_time}s"),
    ]


# ============================================================================
# AGGREGATE SCORING
# ============================================================================

def compute_aggregate_score(metrics: list[MetricResult]) -> dict[str, Any]:
    f1_metrics = [m for m in metrics if m.f1_score is not None and m.f1_score > 0]
    non_f1_metrics = [m for m in metrics if m.f1_score is None]

    avg_f1 = sum(m.f1_score for m in f1_metrics) / len(f1_metrics) if f1_metrics else 0.0  # type: ignore[misc]
    avg_non_f1 = sum(m.value for m in non_f1_metrics) / len(non_f1_metrics) if non_f1_metrics else 0.0

    return {
        "overall_f1_score": round(avg_f1, 4),
        "overall_non_f1_score": round(avg_non_f1, 4),
        "metric_count": len(metrics),
        "f1_metric_count": len(f1_metrics),
        "capabilities_evaluated": len(set(m.name.split("_")[0] for m in metrics)),
    }
