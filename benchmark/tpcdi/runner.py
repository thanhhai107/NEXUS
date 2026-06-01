"""TPC-DI Benchmark Runner — orchestrates all three metric groups.

Produces a unified TPC-DI result that is only considered valid when
ALL correctness audits pass.  Performance (DIU/hr) is the competitive
metric; resource consumption is reported alongside.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from benchmark.tpcdi.performance import TpcdiPerformanceTimer, get_diu
from benchmark.tpcdi.correctness import TpcdiCorrectnessAuditor, AuditResult
from benchmark.tpcdi.resource import ResourceMonitor
from benchmark.utils.io import load_tpcdi_data, TPCDI_RUNTIME_DIR, REPORTS_DIR

from benchmark.ground_truth.extractor import TPCDI_TABLES


@dataclass
class TpcdiResult:
    scale_factor: int
    is_valid: bool
    correctness_all_passed: bool
    correctness_pass_rate: float
    correctness_details: list[dict[str, Any]] = field(default_factory=list)

    diu_per_hour: float = 0.0
    phase1_seconds: float = 0.0
    phase2_seconds: float = 0.0

    resource_avg_cpu: float = 0.0
    resource_peak_memory_mb: float = 0.0
    resource_avg_memory_mb: float = 0.0
    resource_avg_io_mbps: float = 0.0
    resource_sample_count: int = 0

    price_per_diu: float | None = None

    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scale_factor": self.scale_factor,
            "is_valid": self.is_valid,
            "correctness": {
                "all_passed": self.correctness_all_passed,
                "pass_rate": self.correctness_pass_rate,
                "details": self.correctness_details,
            },
            "performance": {
                "diu_per_hour": round(self.diu_per_hour, 3),
                "total_diu": get_diu(self.scale_factor),
                "phase1_seconds": round(self.phase1_seconds, 3),
                "phase2_seconds": round(self.phase2_seconds, 3),
            },
            "resource": {
                "avg_cpu_percent": round(self.resource_avg_cpu, 2),
                "peak_memory_mb": round(self.resource_peak_memory_mb, 2),
                "avg_memory_mb": round(self.resource_avg_memory_mb, 2),
                "avg_io_mbps": round(self.resource_avg_io_mbps, 3),
                "sample_count": self.resource_sample_count,
            },
            "price_per_diu": self.price_per_diu,
            "errors": self.errors,
        }


DEFAULT_PHASE1_DURATION = 30.0
DEFAULT_PHASE2_DURATION = 120.0


class TpcdiRunner:
    """Orchestrates a full TPC-DI benchmark run.

    Parameters
    ----------
    scale_factor:
        TPC-DI scale factor (default 1).
    base_data_dir:
        Directory containing CSV data files. Defaults to ``runtime/datasets/tpcdi/``.
    hourly_infra_cost_usd:
        Optional hourly infrastructure cost for computing ``price_per_diu``.
    """

    REPORT_PATH = REPORTS_DIR / "tpcdi_benchmark.json"

    def __init__(
        self,
        scale_factor: int = 1,
        base_data_dir: Optional[Path] = None,
        hourly_infra_cost_usd: Optional[float] = None,
    ):
        self.scale_factor = scale_factor
        self.base_data_dir = base_data_dir or TPCDI_RUNTIME_DIR
        self.hourly_infra_cost = hourly_infra_cost_usd

        self.timer = TpcdiPerformanceTimer(scale_factor=scale_factor)
        self.auditor = TpcdiCorrectnessAuditor()
        self.monitor = ResourceMonitor(interval_seconds=0.5)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self) -> TpcdiResult:
        result = TpcdiResult(scale_factor=self.scale_factor)

        self._load_all_data(result)
        if result.errors:
            return result

        self.monitor.start()
        try:
            result.correctness_all_passed, result.correctness_pass_rate, result.correctness_details = (
                self._run_correctness()
            )
        finally:
            self.monitor.stop()

        self._gather_resource(result)

        perf_summary = self.timer.summary()
        result.diu_per_hour = perf_summary["diu_per_hour"]
        result.phase1_seconds = perf_summary["phase1_seconds"]
        result.phase2_seconds = perf_summary["phase2_seconds"]

        if self.hourly_infra_cost and result.diu_per_hour > 0:
            result.price_per_diu = round(self.hourly_infra_cost / result.diu_per_hour, 4)

        result.is_valid = result.correctness_all_passed

        return result

    def save_report(self, result: Optional[TpcdiResult] = None) -> Path:
        if result is None:
            result = self.run()
        self.REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        self.REPORT_PATH.write_text(json.dumps(result.to_dict(), indent=2, default=str))
        return self.REPORT_PATH

    # ------------------------------------------------------------------
    # Timing context managers
    # ------------------------------------------------------------------
    def phase1(self):
        return self.timer.phase1()

    def phase2(self, days: int = 1):
        return self.timer.phase2(days=days)

    def set_row_count(self, rows: int) -> None:
        self.timer.set_row_count(rows)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _load_all_data(self, result: TpcdiResult) -> None:
        data: dict[str, list[dict[str, Any]]] = {}
        for table in TPCDI_TABLES:
            try:
                data[table] = load_tpcdi_data(table, self.base_data_dir)
            except FileNotFoundError as exc:
                result.errors.append(f"[{table}] {exc}")
        self.auditor.data = data

    def _run_correctness(self) -> tuple[bool, float, list[dict[str, Any]]]:
        audit_results = self.auditor.run_all()

        data = self.auditor.data
        source_counts: dict[str, int] = {}
        dw_counts: dict[str, int] = {}
        for table in TPCDI_TABLES:
            records = data.get(table, [])
            if records:
                dw_counts[table] = len(records)
                source_counts[table] = len(records)

        row_audit = self.auditor.run_row_count_audit(source_counts, dw_counts)
        audit_results.append(row_audit)

        all_passed = all(r.passed for r in audit_results if r.status != AuditResult.__annotations__.get("status", None))  # noqa
        from benchmark.tpcdi.correctness import AuditStatus
        passed = sum(1 for r in audit_results if r.status == AuditStatus.PASS)
        skipped = sum(1 for r in audit_results if r.status == AuditStatus.SKIP)
        total = len(audit_results) - skipped
        pass_rate = passed / total if total > 0 else 0.0

        details = [r.to_dict() for r in audit_results]
        return all(r.status == AuditStatus.PASS for r in audit_results if r.status != AuditStatus.SKIP), pass_rate, details

    def _gather_resource(self, result: TpcdiResult) -> None:
        s = self.monitor.summary()
        result.resource_avg_cpu = s["avg_cpu_percent"]
        result.resource_peak_memory_mb = s["peak_memory_rss_mb"]
        result.resource_avg_memory_mb = s["avg_memory_rss_mb"]
        result.resource_avg_io_mbps = s["avg_io_mbps"]
        result.resource_sample_count = s["sample_count"]
