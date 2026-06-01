"""Platform Testing Framework — AI capability evaluation and error injection.

This package evaluates how well the NEXUS platform discovers, understands,
and handles data across 8 AI capabilities:

  - Schema discovery & drift detection
  - Semantic mapping & join discovery
  - Entity resolution & data quality detection
  - Lineage completeness & reliability handling

Warning
-------
These metrics measure **platform intelligence**, NOT TPC-DI pipeline
performance. For TPC-DI benchmark results (DIU/hr, correctness audits,
resource consumption), use ``benchmark.tpcdi`` instead.

Usage (R&D only)::

    from benchmark.platform_testing.injection import InjectionEngine
    from benchmark.platform_testing.evaluation import EvaluationEngine

    engine = InjectionEngine(seed=42)
    engine.run_scenario(config)

    evaluator = EvaluationEngine(ground_truth_path=Path("ground_truth.json"))
    scorecard = evaluator.evaluate_scenario("l1_schema", platform_outputs)
"""

from benchmark.platform_testing.injection import (
    InjectionEngine,
    SchemaInjector,
    SemanticInjector,
    QualityInjector,
    HeterogeneityInjector,
    ReliabilityInjector,
)
from benchmark.platform_testing.evaluation import (
    EvaluationEngine,
    MetricResult,
)

__all__ = [
    "InjectionEngine",
    "SchemaInjector",
    "SemanticInjector",
    "QualityInjector",
    "HeterogeneityInjector",
    "ReliabilityInjector",
    "EvaluationEngine",
    "MetricResult",
]
