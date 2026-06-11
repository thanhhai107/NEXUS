"""
NEXUS TPC-DI Benchmark Framework.

Evaluates data pipeline performance against the TPC-DI standard across three
metric dimensions:

  1. Performance   — DIU/hr (Daily Ingestion Units per hour), Phase 2 timing
  2. Correctness   — SCD integrity, referential FK, business rules, dedup
  3. Resource      — CPU, memory, I/O consumption and price-per-DIU/hr

Usage::

    from benchmark.tpcdi.runner import TpcdiRunner

    runner = TpcdiRunner(scale_factor=3, hourly_infra_cost_usd=2.50)
    with runner.phase1():
        run_historical_load()
    with runner.phase2():
        run_incremental_load()
    result = runner.run()
    #   result.is_valid  == True  → all correctness audits passed
    #   result.diu_per_hour       → competitive metric
"""

__version__ = "1.0.0"
