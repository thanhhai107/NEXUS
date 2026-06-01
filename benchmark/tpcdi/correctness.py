"""TPC-DI Correctness Audits.

The TPC-DI specification mandates that ALL audit queries must pass (100%)
before a benchmark result is considered valid.  No exceptions.

Six audit categories:

1. Row Count         — DW record counts match source record counts
2. SCD Correctness   — SCD Type 2 integrity (one IsCurrent=TRUE per business key)
3. Referential       — No orphan FK values in fact / dim tables
4. Trade Status      — Trade lifecycle logic is correct
5. Deduplication     — No duplicate records introduced by multi-source merge
6. Prospect Match    — Existing customers not inserted as new prospects
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any

from benchmark.ground_truth.extractor import TPCDI_FOREIGN_KEYS, TPCDI_TABLES


class AuditStatus(enum.Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass
class AuditResult:
    audit_name: str
    status: AuditStatus
    detail: str = ""
    violations: list[dict[str, Any]] = field(default_factory=list)
    expected: int = 0
    actual: int = 0

    @property
    def passed(self) -> bool:
        return self.status == AuditStatus.PASS

    def to_dict(self) -> dict[str, Any]:
        return {
            "audit": self.audit_name,
            "status": self.status.value,
            "detail": self.detail,
            "violation_count": len(self.violations),
            "expected": self.expected,
            "actual": self.actual,
            "violations": self.violations[:20],
        }


SCD_TABLES = {
    "tpcdi_dim_account":   "accountid",
    "tpcdi_dim_customer":  "customerid",
    "tpcdi_dim_broker":    "brokerid",
    "tpcdi_dim_security":  "symbol",
    "tpcdi_dim_company":   "companyid",
    "tpcdi_dim_trade":     "tradeid",
}


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "t")
    if isinstance(value, (int, float)):
        return bool(value) and value != 0
    return False


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


class TpcdiCorrectnessAuditor:
    """Run the full TPC-DI correctness audit suite against loaded data."""

    def __init__(self, data: dict[str, list[dict[str, Any]]] | None = None):
        self.data: dict[str, list[dict[str, Any]]] = data or {}
        self.results: list[AuditResult] = []

    # ==================================================================
    # 1. Row Count Audit
    # ==================================================================
    def run_row_count_audit(
        self,
        source_counts: dict[str, int],
        dw_counts: dict[str, int],
    ) -> AuditResult:
        violations: list[dict[str, Any]] = []
        for table in sorted(set(source_counts) | set(dw_counts)):
            src = source_counts.get(table, 0)
            dw = dw_counts.get(table, 0)
            if src != dw:
                violations.append({
                    "table": table,
                    "source_count": src,
                    "dw_count": dw,
                    "delta": dw - src,
                })

        status = AuditStatus.PASS if not violations else AuditStatus.FAIL
        return AuditResult(
            audit_name="row_count",
            status=status,
            detail=f"{len(set(source_counts) | set(dw_counts)) - len(violations)}/{len(set(source_counts) | set(dw_counts))} tables match" if violations else f"All {len(source_counts)} tables match",
            violations=violations,
            expected=sum(source_counts.values()),
            actual=sum(dw_counts.values()),
        )

    # ==================================================================
    # 2. SCD Type 2 Correctness Audit
    # ==================================================================
    def run_scd_audit(self) -> AuditResult:
        violations: list[dict[str, Any]] = []
        checked = 0

        for table, business_key in SCD_TABLES.items():
            records = self.data.get(table, [])
            if not records:
                continue
            checked += 1

            by_bk: dict[str, list[dict[str, Any]]] = {}
            for r in records:
                bk_val = _safe_str(r.get(business_key))
                by_bk.setdefault(bk_val, []).append(r)

            for bk_val, versions in by_bk.items():
                current_count = sum(1 for r in versions if _to_bool(r.get("iscurrent")))
                if current_count != 1:
                    violations.append({
                        "table": table,
                        "business_key": bk_val,
                        "issue": "iscurrent_count",
                        "expected": 1,
                        "actual": current_count,
                        "version_count": len(versions),
                    })

                for r in versions:
                    is_current = _to_bool(r.get("iscurrent"))
                    enddate = r.get("enddate")
                    if not is_current and (enddate is None or str(enddate).strip() == "" or str(enddate).strip() == "9999-12-31"):
                        violations.append({
                            "table": table,
                            "business_key": bk_val,
                            "sk": r.get(f"sk_{business_key.replace('id', '')}id") if "sk_" in str(list(r.keys())) else r,
                            "issue": "missing_enddate_on_noncurrent",
                            "iscurrent": is_current,
                            "enddate": enddate,
                        })

                    effectivedate = r.get("effectivedate")
                    if effectivedate is None or str(effectivedate).strip() == "":
                        violations.append({
                            "table": table,
                            "business_key": bk_val,
                            "issue": "missing_effectivedate",
                        })

        status = AuditStatus.PASS if not violations else AuditStatus.FAIL
        return AuditResult(
            audit_name="scd_correctness",
            status=status,
            detail=f"Checked {checked} SCD tables" if not violations else f"{len(violations)} policy violations in {checked} SCD tables",
            violations=violations,
            expected=0,
            actual=len(violations),
        )

    # ==================================================================
    # 3. Referential Integrity Audit
    # ==================================================================
    def run_referential_integrity_audit(self) -> AuditResult:
        violations: list[dict[str, Any]] = []

        for table, fks in TPCDI_FOREIGN_KEYS.items():
            records = self.data.get(table, [])
            if not records:
                continue
            for r in records:
                for fk_col, parent_ref in fks.items():
                    fk_val = _safe_str(r.get(fk_col))
                    if not fk_val or fk_val == "0":
                        continue
                    parent_table, parent_col = parent_ref.split(".", 1)
                    records_p = self.data.get(parent_table, [])
                    parent_vals = {_safe_str(pr.get(parent_col)) for pr in records_p}
                    if fk_val not in parent_vals:
                        violations.append({
                            "table": table,
                            "fk_column": fk_col,
                            "fk_value": fk_val,
                            "referenced_table": parent_table,
                            "referenced_column": parent_col,
                        })

        status = AuditStatus.PASS if not violations else AuditStatus.FAIL
        return AuditResult(
            audit_name="referential_integrity",
            status=status,
            detail=f"All FKs valid" if not violations else f"{len(violations)} orphan FK values found",
            violations=violations,
            expected=0,
            actual=len(violations),
        )

    # ==================================================================
    # 4. Trade Status Business Rule Audit
    # ==================================================================
    def run_trade_status_audit(self) -> AuditResult:
        violations: list[dict[str, Any]] = []
        trades = self.data.get("tpcdi_dim_trade", [])
        holdings = self.data.get("tpcdi_fact_holdings", [])

        if not trades or not holdings:
            return AuditResult("trade_status", AuditStatus.SKIP, "No trade or holdings data")

        trade_statuses: dict[str, str] = {}
        trade_ids_in_holdings: set[str] = set()
        for h in holdings:
            tid = _safe_str(h.get("sk_tradeid"))
            if tid:
                trade_ids_in_holdings.add(tid)

        for t in trades:
            tid = _safe_str(t.get("sk_tradeid"))
            status = _safe_str(t.get("trade_status"))
            if tid:
                trade_statuses[tid] = status

        for t in trades:
            tid = _safe_str(t.get("sk_tradeid"))
            status_str = _safe_str(t.get("trade_status"))
            is_current = _to_bool(t.get("iscurrent"))

            if not is_current:
                continue

            if "cancel" in status_str:
                if tid in trade_ids_in_holdings:
                    violations.append({
                        "table": "tpcdi_dim_trade",
                        "sk_tradeid": tid,
                        "issue": "cancelled_trade_has_holdings",
                        "trade_status": status_str,
                    })

            elif "complet" in status_str:
                if tid not in trade_ids_in_holdings:
                    violations.append({
                        "table": "tpcdi_dim_trade",
                        "sk_tradeid": tid,
                        "issue": "completed_trade_missing_holdings",
                        "trade_status": status_str,
                    })

            elif "pending" in status_str:
                pass

        status = AuditStatus.PASS if not violations else AuditStatus.FAIL
        return AuditResult(
            audit_name="trade_status",
            status=status,
            detail=f"{len(trades)} trades checked" if not violations else f"{len(violations)} trade status violations",
            violations=violations,
            expected=0,
            actual=len(violations),
        )

    # ==================================================================
    # 5. Deduplication Audit
    # ==================================================================
    def run_dedup_audit(self) -> AuditResult:
        violations: list[dict[str, Any]] = []

        for table in TPCDI_TABLES:
            records = self.data.get(table, [])
            if not records or len(records) < 2:
                continue

            if table in SCD_TABLES:
                continue

            pk_candidates = self._get_pk(table)
            if not pk_candidates:
                continue

            seen: set[tuple] = set()
            for r in records:
                key = tuple(_safe_str(r.get(col, "")) for col in pk_candidates)
                if key in seen:
                    violations.append({
                        "table": table,
                        "duplicate_key": {col: r.get(col) for col in pk_candidates},
                        "issue": "duplicate_primary_key",
                    })
                seen.add(key)

        status = AuditStatus.PASS if not violations else AuditStatus.FAIL
        return AuditResult(
            audit_name="deduplication",
            status=status,
            detail="No duplicates found" if not violations else f"{len(violations)} duplicate PK violations",
            violations=violations,
            expected=0,
            actual=len(violations),
        )

    def _get_pk(self, table: str) -> list[str]:
        from benchmark.ground_truth.extractor import GroundTruthExtractor
        return GroundTruthExtractor._get_primary_keys(GroundTruthExtractor, table)

    # ==================================================================
    # 6. Prospect Matching Audit
    # ==================================================================
    def run_prospect_match_audit(self) -> AuditResult:
        violations: list[dict[str, Any]] = []
        prospects = self.data.get("tpcdi_prospect", [])
        customers = self.data.get("tpcdi_dim_customer", [])

        if not prospects:
            return AuditResult("prospect_match", AuditStatus.SKIP, "No prospect data")

        customer_set: set[tuple[str, str, str]] = set()
        for c in customers:
            if not _to_bool(c.get("iscurrent", True)):
                continue
            fname = _safe_str(c.get("firstname", ""))
            lname = _safe_str(c.get("lastname", ""))
            email = _safe_str(c.get("email", ""))
            customer_set.add((fname, lname, email))

        for p in prospects:
            p_fname = _safe_str(p.get("firstname", ""))
            p_lname = _safe_str(p.get("lastname", ""))
            p_email = _safe_str(p.get("email", ""))
            is_customer = _to_bool(p.get("iscustomer"))

            key = (p_fname, p_lname, p_email)
            is_known = key in customer_set

            if is_known and not is_customer:
                violations.append({
                    "agencyid": p.get("agencyid"),
                    "issue": "known_customer_marked_as_not_customer",
                    "firstname": p_fname,
                    "lastname": p_lname,
                    "email": p_email,
                })

            if is_customer and not is_known:
                violations.append({
                    "agencyid": p.get("agencyid"),
                    "issue": "iscustomer_true_but_not_in_customer_dim",
                    "firstname": p_fname,
                    "lastname": p_lname,
                    "email": p_email,
                    "iscustomer": is_customer,
                })

        status = AuditStatus.PASS if not violations else AuditStatus.FAIL
        return AuditResult(
            audit_name="prospect_match",
            status=status,
            detail=f"{len(prospects)} prospects checked" if not violations else f"{len(violations)} prospect-customer mismatches",
            violations=violations,
            expected=sum(1 for p in prospects if _to_bool(p.get("iscustomer"))),
            actual=len(violations),
        )

    # ==================================================================
    # Run all
    # ==================================================================
    def run_all(
        self,
        source_counts: dict[str, int] | None = None,
        dw_counts: dict[str, int] | None = None,
    ) -> list[AuditResult]:
        self.results = []

        self.results.append(self.run_dedup_audit())

        self.results.append(self.run_scd_audit())

        self.results.append(self.run_referential_integrity_audit())

        self.results.append(self.run_trade_status_audit())

        self.results.append(self.run_prospect_match_audit())

        if source_counts or dw_counts:
            self.results.append(self.run_row_count_audit(source_counts or {}, dw_counts or {}))

        return self.results

    @property
    def all_passed(self) -> bool:
        return all(r.status == AuditStatus.PASS for r in self.results if r.status != AuditStatus.SKIP)

    @property
    def failed_audits(self) -> list[AuditResult]:
        return [r for r in self.results if r.status == AuditStatus.FAIL]

    @property
    def pass_rate(self) -> float:
        evaluated = [r for r in self.results if r.status != AuditStatus.SKIP]
        if not evaluated:
            return 0.0
        return sum(1 for r in evaluated if r.passed) / len(evaluated)

    def summary(self) -> dict[str, Any]:
        return {
            "all_passed": self.all_passed,
            "pass_rate": round(self.pass_rate, 4),
            "total_audits": len(self.results),
            "passed": sum(1 for r in self.results if r.passed),
            "failed": len(self.failed_audits),
            "skipped": sum(1 for r in self.results if r.status == AuditStatus.SKIP),
            "details": [r.to_dict() for r in self.results],
        }
