"""Evaluation package — metric calculators and evaluation engine."""

from benchmark.evaluation.engine import EvaluationEngine
from benchmark.evaluation.metrics import (
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

__all__ = [
    "EvaluationEngine",
    "MetricResult",
    "schema_discovery_metrics",
    "schema_drift_metrics",
    "semantic_mapping_metrics",
    "join_discovery_metrics",
    "entity_resolution_metrics",
    "data_quality_metrics",
    "lineage_completeness_metrics",
    "reliability_metrics",
    "compute_aggregate_score",
]
