"""
NEXUS Multi-Source Data Ingestion & Semantic Discovery Benchmark Framework.

Ground-truth dataset: TPC-DS SF=1 (12 tables, ~127M rows).
Evaluates 10 platform capabilities across 4 difficulty levels.

Architecture:
    benchmark/
    ├── injection/      — Error Injection Framework (6 categories, 35+ error types)
    ├── ground_truth/    — Ground truth metadata extraction from clean TPC-DS
    ├── evaluation/      — Metric calculators for 8 capabilities
    ├── scenarios/       — YAML scenario configs (Level 1-4)
    ├── cli/             — CLI runner
    ├── reports/         — Generated scorecards and aggregate reports
    └── utils/           — Shared hashing, IO, validation
"""

__version__ = "1.0.0"
