"""
NEXUS Benchmark Framework — TPC-DI pipeline metrics and platform testing.

Primary path:
    benchmark/tpcdi/          — TPC-DI benchmark metrics (DIU/hr, correctness, resource)

Platform testing (R&D only):
    benchmark/platform_testing/ — Error injection + capability evaluation

Architecture:
    benchmark/
    ├── tpcdi/                  — TPC-DI DIU/hr, correctness audits, resource monitoring
    ├── platform_testing/
    │   ├── injection/          — Error Injection Framework (6 categories, 35+ types)
    │   ├── evaluation/         — Capability metric calculators (8 dimensions, F1 scores)
    │   └── scenarios/          — YAML scenario configs (Level 1-4)
    ├── ground_truth/           — Metadata extraction from clean data
    ├── cli/                    — CLI runner
    ├── reports/                — Generated scorecards and reports
    └── utils/                  — Shared hashing, IO, validation
"""

__version__ = "1.0.0"
