"""TPC-DI Benchmark Metrics — performance, correctness, and resource evaluation.

Implements the three canonical TPC-DI metric groups:

  1. Performance   — DIU/hr (Daily Ingestion Units per hour) from Phase 2 timing
  2. Correctness   — 6 audit categories that must ALL pass before results count
  3. Resource      — CPU, memory, and I/O consumption during pipeline execution

Usage::

    from benchmark.tpcdi.runner import TpcdiRunner
    runner = TpcdiRunner(scale_factor=3)
    result = runner.run()
    runner.save_report()
"""

from benchmark.tpcdi.performance import TpcdiPerformanceTimer, compute_diu_hr
from benchmark.tpcdi.correctness import TpcdiCorrectnessAuditor, AuditStatus
from benchmark.tpcdi.resource import ResourceMonitor, ResourceSnapshot
from benchmark.tpcdi.runner import TpcdiRunner, TpcdiResult

__all__ = [
    "TpcdiPerformanceTimer", "compute_diu_hr",
    "TpcdiCorrectnessAuditor", "AuditStatus",
    "ResourceMonitor", "ResourceSnapshot",
    "TpcdiRunner", "TpcdiResult",
]
