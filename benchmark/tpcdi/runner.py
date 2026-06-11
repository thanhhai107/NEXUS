"""TPC-DI Benchmark Runner — orchestrates all three metric groups.

Produces a unified TPC-DI result that is only considered valid when
ALL correctness audits pass.  Performance (DIU/hr) is the competitive
metric; resource consumption is reported alongside.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from benchmark.tpcdi.performance import TpcdiPerformanceTimer, get_diu
from benchmark.tpcdi.correctness import TpcdiCorrectnessAuditor, AuditStatus
from benchmark.tpcdi.resource import ResourceMonitor
from benchmark.utils.io import load_tpcdi_data, REPORTS_DIR
from common.tpcdi_sources import resolve_scale_factor

from benchmark.ground_truth.extractor import TPCDI_TABLES

# M1 sources
M1_SOURCES = ["status_type", "trade_type", "tax_rate", "industry", "date", "time"]


@dataclass
class TpcdiResult:
    scale_factor: int
    is_valid: bool = False
    correctness_all_passed: bool = False
    correctness_pass_rate: float = 0.0
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
                "pass_rate": round(self.correctness_pass_rate, 4),
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


class TpcdiRunner:
    """Orchestrates a full TPC-DI benchmark run.

    Parameters
    ----------
    scale_factor:
        TPC-DI scale factor. Supported benchmark scales are 3, 10, and 50.
    base_data_dir:
        Optional override for the DIGen source root. By default the runner uses
        ``runtime/tpcdi/sf{scale_factor}/``.
    hourly_infra_cost_usd:
        Optional hourly infrastructure cost for computing ``price_per_diu``.
    """

    REPORT_PATH = REPORTS_DIR / "tpcdi_benchmark.json"

    def __init__(
        self,
        scale_factor: int = 3,
        base_data_dir: Optional[Path] = None,
        hourly_infra_cost_usd: Optional[float] = None,
    ):
        self.scale_factor = resolve_scale_factor(scale_factor)
        os.environ["TPCDI_SCALE_FACTOR"] = str(self.scale_factor)
        self.base_data_dir = base_data_dir
        self.hourly_infra_cost = hourly_infra_cost_usd

        self.timer = TpcdiPerformanceTimer(scale_factor=scale_factor)
        self.auditor = TpcdiCorrectnessAuditor()
        self.monitor = ResourceMonitor(interval_seconds=0.5)
        self._row_total = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self) -> TpcdiResult:
        result = TpcdiResult(scale_factor=self.scale_factor)

        self._load_all_data(result)
        self._count_rows()

        if result.errors:
            return result

        self.monitor.start()
        try:
            audit_results = self.auditor.run_all()

            source_counts: dict[str, int] = {}
            dw_counts: dict[str, int] = {}
            for table in TPCDI_TABLES:
                records = self.auditor.data.get(table, [])
                if records:
                    source_counts[table] = len(records)
                    dw_counts[table] = len(records)

            row_audit = self.auditor.run_row_count_audit(source_counts, dw_counts)
            audit_results.append(row_audit)

            passed = sum(1 for r in audit_results if r.status == AuditStatus.PASS)
            skipped = sum(1 for r in audit_results if r.status == AuditStatus.SKIP)
            total = len(audit_results) - skipped
            result.correctness_pass_rate = passed / total if total > 0 else 0.0
            result.correctness_all_passed = (
                total > 0 and
                all(r.status == AuditStatus.PASS for r in audit_results if r.status != AuditStatus.SKIP)
            )
            result.correctness_details = [r.to_dict() for r in audit_results]
        finally:
            self.monitor.stop()

        self._gather_resource(result)
        self.timer.set_row_count(self._row_total)

        perf_summary = self.timer.summary()
        result.diu_per_hour = perf_summary["diu_per_hour"]
        result.phase1_seconds = perf_summary["phase1_seconds"]
        result.phase2_seconds = perf_summary["phase2_seconds"]

        if self.hourly_infra_cost and result.diu_per_hour > 0:
            result.price_per_diu = round(self.hourly_infra_cost / result.diu_per_hour, 4)

        result.is_valid = result.correctness_all_passed

        return result

    # ------------------------------------------------------------------
    # Milestone 1 — Group 1 (reference + date/time)
    # ------------------------------------------------------------------
    M1_SOURCES = ["status_type", "trade_type", "tax_rate", "industry", "date", "time"]

    # ------------------------------------------------------------------
    # Milestone 2 — Group 2a (hr, prospect, daily_market)
    # ------------------------------------------------------------------
    M2_SOURCES = ["hr", "prospect", "daily_market", "trade", "cash_transaction", "holding_history", "watch_history"]

    M3_NON_VALIDATABLE = {"customer_mgmt", "finwire"}  # XML/fixed-width — not csv-validatable

    def _run_pipeline(self, source_names: list[str], result: TpcdiResult,
                       steps: tuple[str, ...] = ("validate", "bronze", "silver", "gold")) -> float:
        """Run specified pipeline steps for each source. Returns elapsed seconds."""
        import time
        from governance.quality.bronze_validator import validate_bronze_tpcdi_file
        from processing.bronze.tpcdi_raw_to_bronze import run as bronze_run
        from processing.silver.tpcdi_bronze_to_silver import run as silver_run
        from processing.gold.tpcdi_silver_to_gold import run as gold_run

        started = time.perf_counter()

        for source_name in source_names:
            if "validate" in steps and source_name not in self.M3_NON_VALIDATABLE:
                v = validate_bronze_tpcdi_file(source_name=source_name, batch_id="batch1")
                if v["status"] != "passed":
                    result.errors.append(f"[{source_name}] validation failed: {v['details']}")
                    continue

            if "bronze" in steps:
                br = bronze_run(source_name=source_name)
                if br.get("error"):
                    result.errors.append(f"[{source_name}] bronze error: {br['error']}")
                    continue

            if "silver" in steps:
                sr = silver_run(source_name=source_name)
                if sr.get("error"):
                    result.errors.append(f"[{source_name}] silver error: {sr['error']}")
                    continue

            if "gold" in steps:
                gr = gold_run(source_name=source_name)
                if gr.get("error"):
                    result.errors.append(f"[{source_name}] gold error: {gr['error']}")
                    continue

        return round(time.perf_counter() - started, 3)

    def run_milestone1(self, clean_outputs: bool = True) -> TpcdiResult:
        result = TpcdiResult(scale_factor=self.scale_factor)
        if clean_outputs:
            self._clean_m1_outputs()
        self.monitor.start()
        result.phase1_seconds = self._run_pipeline(self.M1_SOURCES, result)
        self.monitor.stop()
        self._gather_resource(result)
        audit_results = self.auditor.run_milestone1()
        self._set_audit_result(result, audit_results)
        return result

    M2A_SOURCES = ["hr", "prospect", "daily_market"]
    M2B_SOURCES = ["trade", "cash_transaction", "holding_history", "watch_history"]
    M3_SOURCES = ["customer_mgmt", "finwire", "customer_update", "account_update"]

    def run_milestone2a(self, clean_outputs: bool = True) -> TpcdiResult:
        """M2a: M1 full + M2a full (Silver+Gold) + M1+M2a audits."""
        self.monitor.start()
        result = TpcdiResult(scale_factor=self.scale_factor)
        if clean_outputs:
            self._clean_m1_outputs()
        m1_time = self._run_pipeline(self.M1_SOURCES, result)
        g2_time = self._run_pipeline(self.M2A_SOURCES, result)
        self.monitor.stop()
        result.phase1_seconds = round(m1_time + g2_time, 3)
        self._gather_resource(result)
        audit_results = self.auditor.run_milestone1() + self.auditor.run_milestone2a()
        self._set_audit_result(result, audit_results)
        return result

    def run_milestone2(self, clean_outputs: bool = True) -> TpcdiResult:
        """M2: M1 full + M2a full + M2b validate+bronze."""
        self.monitor.start()
        result = TpcdiResult(scale_factor=self.scale_factor)
        if clean_outputs:
            self._clean_m1_outputs()
        m1_time = self._run_pipeline(self.M1_SOURCES, result)
        g2a_time = self._run_pipeline(self.M2A_SOURCES, result)
        g2b_time = self._run_pipeline(self.M2B_SOURCES, result, steps=("validate", "bronze"))
        self.monitor.stop()
        result.phase1_seconds = round(m1_time + g2a_time + g2b_time, 3)
        self._gather_resource(result)
        audit_results = self.auditor.run_milestone1() + self.auditor.run_milestone2a()
        self._set_audit_result(result, audit_results)
        return result

    def run_milestone3(self, clean_outputs: bool = True) -> TpcdiResult:
        """M3: M1 full + M2a full + M2b full (Silver+Gold) + all audits."""
        self.monitor.start()
        result = TpcdiResult(scale_factor=self.scale_factor)
        if clean_outputs:
            self._clean_m1_outputs()
        m1_time = self._run_pipeline(self.M1_SOURCES, result)
        g2a_time = self._run_pipeline(self.M2A_SOURCES, result)
        g2b_time = self._run_pipeline(self.M2B_SOURCES, result)
        self.monitor.stop()
        result.phase1_seconds = round(m1_time + g2a_time + g2b_time, 3)
        self._gather_resource(result)
        audit_results = (
            self.auditor.run_milestone1()
            + self.auditor.run_milestone2a()
            + self.auditor.run_milestone3()
        )
        self._set_audit_result(result, audit_results)
        return result

    def run_milestone4(self, clean_outputs: bool = True) -> TpcdiResult:
        """M4: M1 + M2a + M2b + M3 full + SCD2 transforms + M1-4 audits."""
        self.monitor.start()
        result = TpcdiResult(scale_factor=self.scale_factor)
        if clean_outputs:
            self._clean_m1_outputs()
        from processing.silver.tpcdi_transform import run_hr_split, run_scd2, run_trade_merge

        # M3 has mixed batch_ids — split into batch1 and batch2 groups
        m3_batch1 = ["customer_mgmt", "finwire"]
        m3_batch2 = ["customer_update", "account_update"]

        t1 = self._run_pipeline(self.M1_SOURCES, result)
        t2 = self._run_pipeline(self.M2A_SOURCES, result)
        t3 = self._run_pipeline(self.M2B_SOURCES, result)
        t4 = self._run_pipeline(m3_batch1, result)  # batch1 → customer_mgmt, finwire
        # customer_update and account_update exist in batch2 only
        from processing.bronze.tpcdi_raw_to_bronze import run as bronze_run
        for src in m3_batch2:
            br = bronze_run(source_name=src, batch_id="batch2")
            if br.get("error"):
                result.errors.append(f"[{src}] bronze error: {br['error']}")
        result.phase1_seconds = round(t1 + t2 + t3 + t4, 3)

        # Transforms
        hr_result = run_hr_split()
        if hr_result.get("error"):
            result.errors.append(f"[hr_split] {hr_result['error']}")
        scd2_result = run_scd2("customer_update", "batch2")
        if scd2_result.get("error"):
            result.errors.append(f"[scd2] {scd2_result['error']}")
        trade_result = run_trade_merge()
        if trade_result.get("error"):
            result.errors.append(f"[trade_merge] {trade_result['error']}")

        self.monitor.stop()
        self._gather_resource(result)

        audit_results = (
            self.auditor.run_milestone1()
            + self.auditor.run_milestone2a()
            + self.auditor.run_milestone3()
            + self.auditor.run_milestone4()
        )
        self._set_audit_result(result, audit_results)
        return result

    def _set_audit_result(self, result: TpcdiResult, audit_results: list) -> None:
        passed = sum(1 for r in audit_results if r.status == AuditStatus.PASS)
        skipped = sum(1 for r in audit_results if r.status == AuditStatus.SKIP)
        total = len(audit_results) - skipped
        result.correctness_pass_rate = passed / total if total > 0 else 0.0
        result.correctness_all_passed = (
            total > 0
            and all(r.status == AuditStatus.PASS for r in audit_results if r.status != AuditStatus.SKIP)
        )
        result.correctness_details = [r.to_dict() for r in audit_results]
        result.is_valid = result.correctness_all_passed and not result.errors

    def _clean_m1_outputs(self) -> None:
        """Remove Bronze/Silver/Gold output for all milestone sources."""
        import shutil
        project_root = Path(__file__).resolve().parents[2]
        all_sources = self.M1_SOURCES + self.M2A_SOURCES + self.M2B_SOURCES + self.M3_SOURCES
        for layer in ["bronze", "silver", "gold"]:
            base = project_root / "runtime" / "lake" / layer / "tpcdi"
            for source in all_sources:
                path = base / source
                if path.exists():
                    shutil.rmtree(path)

    def save_report(self, result: Optional[TpcdiResult] = None) -> Path:
        if result is None:
            result = self.run_milestone1()
        self.REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        self.REPORT_PATH.write_text(json.dumps(result.to_dict(), indent=2, default=str))
        return self.REPORT_PATH

    # ------------------------------------------------------------------
    # Timing context managers (for pipeline integration)
    # ------------------------------------------------------------------
    def phase1(self):
        return self.timer.phase1()

    def phase2(self, days: int = 1):
        return self.timer.phase2(days=days)

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

    def _count_rows(self) -> None:
        self._row_total = sum(len(v) for v in self.auditor.data.values())

    def _gather_resource(self, result: TpcdiResult) -> None:
        s = self.monitor.summary()
        result.resource_avg_cpu = s["avg_cpu_percent"]
        result.resource_peak_memory_mb = s["peak_memory_rss_mb"]
        result.resource_avg_memory_mb = s["avg_memory_rss_mb"]
        result.resource_avg_io_mbps = s["avg_io_mbps"]
        result.resource_sample_count = s["sample_count"]
