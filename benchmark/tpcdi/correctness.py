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
    # M1 — Row Count Audit (Gold vs digen_report.txt)
    # ==================================================================
    M1_TABLES = {
        "status_type": "st_id",
        "trade_type": "tt_id",
        "tax_rate": "tx_id",
        "industry": "in_id",
        "date": "sk_date",
        "time": "sk_time",
    }

    M2A_TABLES = {
        "hr": "employee_id",
        "prospect": "agency_id",
    }

    M2B_TABLES = {
        "daily_market": ["dm_date", "dm_s_symb"],
    }

    M2C_TABLES = {}  # all fact tables — row_count only, no natural PK at source level
    M2C_TABLES_SINGLE_PK = {
        "trade": "trade_id",
    }
    M2C_TABLES_ALL = {
        "trade": "trade_id",
        "cash_transaction": None,  # fact — row count only
        "holding_history": None,   # fact — row count only
        "watch_history": None,     # fact — row count only
    }

    def run_row_count_audit_m1(self) -> AuditResult:
        """Compare M1 Gold record counts against DIGen source record counts.

        Reads Gold JSONL via streaming — does not load full table.
        """
        from common.tpcdi_io import count_tpcdi_records
        from pathlib import Path

        gold_root = Path(__file__).resolve().parents[2] / "runtime" / "lake" / "gold" / "tpcdi"

        violations: list[dict[str, Any]] = []
        checked = 0
        expected_total = 0
        actual_total = 0

        for source_name in self.M1_TABLES:
            gold_file = gold_root / source_name / "data.jsonl"

            expected_count = count_tpcdi_records(source_name, "batch1")
            expected_total += expected_count

            if not gold_file.exists():
                violations.append({
                    "table": source_name,
                    "issue": "gold_data_not_found",
                    "path": str(gold_file),
                    "expected": expected_count,
                    "actual": 0,
                })
                continue

            actual_count = 0
            with gold_file.open("r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        actual_count += 1

            actual_total += actual_count
            checked += 1

            if actual_count != expected_count:
                violations.append({
                    "table": source_name,
                    "issue": "row_count_mismatch",
                    "expected": expected_count,
                    "actual": actual_count,
                    "delta": actual_count - expected_count,
                })

        status = AuditStatus.PASS if not violations else AuditStatus.FAIL
        return AuditResult(
            audit_name="row_count_m1",
            status=status,
            detail=(
                f"{checked}/{len(self.M1_TABLES)} tables match"
                if not violations
                else f"{len(violations)} row count issues"
            ),
            violations=violations,
            expected=expected_total,
            actual=actual_total,
        )

    # ==================================================================
    # M1 — PK Duplicate Audit (Gold JSONL, streaming)
    # ==================================================================
    def run_pk_duplicate_audit_m1(self) -> AuditResult:
        """Check for duplicate or missing primary keys in M1 Gold tables.

        Uses a streaming Python set per table — memory O(distinct PKs), not O(rows).
        """
        import json
        from pathlib import Path

        gold_root = Path(__file__).resolve().parents[2] / "runtime" / "lake" / "gold" / "tpcdi"

        violations: list[dict[str, Any]] = []
        checked = 0

        for table, pk_col in self.M1_TABLES.items():
            gold_file = gold_root / table / "data.jsonl"
            if not gold_file.exists():
                violations.append({
                    "table": table,
                    "issue": "gold_data_not_found",
                    "path": str(gold_file),
                })
                continue

            checked += 1
            seen: set[str] = set()

            with gold_file.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    payload = record.get("payload", {})
                    pk_raw = payload.get(pk_col)
                    pk_val = str(pk_raw).strip() if pk_raw is not None else ""

                    if not pk_val:
                        violations.append({
                            "table": table,
                            "pk_column": pk_col,
                            "issue": "missing_primary_key",
                        })
                        continue

                    if pk_val in seen:
                        violations.append({
                            "table": table,
                            "pk_column": pk_col,
                            "pk_value": pk_val,
                            "issue": "duplicate_primary_key",
                        })
                    else:
                        seen.add(pk_val)

        status = AuditStatus.PASS if not violations else AuditStatus.FAIL
        return AuditResult(
            audit_name="pk_duplicate_m1",
            status=status,
            detail=f"No issues in {checked} tables" if not violations else f"{len(violations)} PK issues",
            violations=violations,
            expected=0,
            actual=len(violations),
        )

    # ==================================================================
    # M2a — Row Count Audit (hr, prospect, daily_market)
    # ==================================================================
    def run_row_count_audit_m2a(self) -> AuditResult:
        from common.tpcdi_io import count_tpcdi_records
        from pathlib import Path

        gold_root = Path(__file__).resolve().parents[2] / "runtime" / "lake" / "gold" / "tpcdi"
        sources = list(self.M2A_TABLES) + list(self.M2B_TABLES)
        violations: list[dict[str, Any]] = []
        checked = expected_total = actual_total = 0

        for source_name in sources:
            expected_count = count_tpcdi_records(source_name, "batch1")
            expected_total += expected_count
            gold_file = gold_root / source_name / "data.jsonl"
            if not gold_file.exists():
                violations.append({"table": source_name, "issue": "gold_data_not_found", "expected": expected_count, "actual": 0})
                continue
            actual_count = sum(1 for line in gold_file.open() if line.strip())
            actual_total += actual_count
            checked += 1
            if actual_count != expected_count:
                violations.append({"table": source_name, "issue": "row_count_mismatch", "expected": expected_count, "actual": actual_count, "delta": actual_count - expected_count})

        status = AuditStatus.PASS if not violations else AuditStatus.FAIL
        return AuditResult(
            audit_name="row_count_m2a",
            status=status,
            detail=f"{checked}/{len(sources)} tables match" if not violations else f"{len(violations)} row count issues",
            violations=violations, expected=expected_total, actual=actual_total,
        )

    # ==================================================================
    # M2a — PK Duplicate Audit (hr, prospect, daily_market)
    # ==================================================================
    def run_pk_duplicate_audit_m2a(self) -> AuditResult:
        import json
        from pathlib import Path

        gold_root = Path(__file__).resolve().parents[2] / "runtime" / "lake" / "gold" / "tpcdi"
        violations: list[dict[str, Any]] = []
        checked = 0

        # Single-field PK tables
        for table, pk_col in self.M2A_TABLES.items():
            gold_file = gold_root / table / "data.jsonl"
            if not gold_file.exists():
                violations.append({"table": table, "issue": "gold_data_not_found"})
                continue
            checked += 1
            seen: set[str] = set()
            with gold_file.open() as f:
                for line in f:
                    if not line.strip(): continue
                    pk_raw = json.loads(line).get("payload", {}).get(pk_col)
                    pk_val = str(pk_raw).strip() if pk_raw is not None else ""
                    if not pk_val:
                        violations.append({"table": table, "pk_column": pk_col, "issue": "missing_primary_key"})
                    elif pk_val in seen:
                        violations.append({"table": table, "pk_column": pk_col, "pk_value": pk_val, "issue": "duplicate_primary_key"})
                    else:
                        seen.add(pk_val)

        # Composite PK tables (daily_market)
        for table, pk_cols in self.M2B_TABLES.items():
            gold_file = gold_root / table / "data.jsonl"
            if not gold_file.exists():
                violations.append({"table": table, "issue": "gold_data_not_found"})
                continue
            checked += 1
            seen: set[tuple] = set()
            with gold_file.open() as f:
                for line in f:
                    if not line.strip(): continue
                    payload = json.loads(line).get("payload", {})
                    key = tuple(str(payload.get(c, "") or "").strip() for c in pk_cols)
                    if not all(key):
                        violations.append({"table": table, "pk_columns": pk_cols, "issue": "missing_primary_key"})
                    elif key in seen:
                        violations.append({"table": table, "pk_columns": pk_cols, "pk_value": str(key), "issue": "duplicate_primary_key"})
                    else:
                        seen.add(key)

        status = AuditStatus.PASS if not violations else AuditStatus.FAIL
        return AuditResult(
            audit_name="pk_duplicate_m2a",
            status=status,
            detail=f"No issues in {checked} tables" if not violations else f"{len(violations)} PK issues",
            violations=violations, expected=0, actual=len(violations),
        )

    # ==================================================================
    # M2c — Row Count Audit (trade, cash_transaction, holding_history, watch_history)
    # ==================================================================
    def run_row_count_audit_m2c(self) -> AuditResult:
        from common.tpcdi_io import count_tpcdi_records
        from pathlib import Path
        gold_root = Path(__file__).resolve().parents[2] / "runtime" / "lake" / "gold" / "tpcdi"
        sources = list(self.M2C_TABLES_ALL)
        violations: list[dict[str, Any]] = []
        checked = expected_total = actual_total = 0
        for source_name in sources:
            expected_count = count_tpcdi_records(source_name, "batch1")
            expected_total += expected_count
            gold_file = gold_root / source_name / "data.jsonl"
            if not gold_file.exists():
                violations.append({"table": source_name, "issue": "gold_data_not_found", "expected": expected_count, "actual": 0}); continue
            actual_count = sum(1 for line in gold_file.open() if line.strip())
            actual_total += actual_count; checked += 1
            if actual_count != expected_count:
                violations.append({"table": source_name, "issue": "row_count_mismatch", "expected": expected_count, "actual": actual_count, "delta": actual_count - expected_count})
        status = AuditStatus.PASS if not violations else AuditStatus.FAIL
        return AuditResult(audit_name="row_count_m2c", status=status, detail=f"{checked}/{len(sources)} tables match" if not violations else f"{len(violations)} row count issues", violations=violations, expected=expected_total, actual=actual_total)

    # ==================================================================
    # M2c — PK Duplicate Audit (trade only — has natural PK at source level)
    # ==================================================================
    def run_pk_duplicate_audit_m2c(self) -> AuditResult:
        import json; from pathlib import Path
        gold_root = Path(__file__).resolve().parents[2] / "runtime" / "lake" / "gold" / "tpcdi"
        violations: list[dict[str, Any]] = []; checked = 0
        for table, pk_col in self.M2C_TABLES_SINGLE_PK.items():
            gold_file = gold_root / table / "data.jsonl"
            if not gold_file.exists():
                violations.append({"table": table, "issue": "gold_data_not_found"}); continue
            checked += 1; seen: set[str] = set()
            with gold_file.open() as f:
                for line in f:
                    if not line.strip(): continue
                    pk_raw = json.loads(line).get("payload", {}).get(pk_col)
                    pk_val = str(pk_raw).strip() if pk_raw is not None else ""
                    if not pk_val:
                        violations.append({"table": table, "pk_column": pk_col, "issue": "missing_primary_key"})
                    elif pk_val in seen:
                        violations.append({"table": table, "pk_column": pk_col, "pk_value": pk_val, "issue": "duplicate_primary_key"})
                    else:
                        seen.add(pk_val)
        status = AuditStatus.PASS if not violations else AuditStatus.FAIL
        detail = f"No issues in {checked} tables" if not violations else f"{len(violations)} PK issues in {checked} tables"
        return AuditResult(audit_name="pk_duplicate_m2c", status=status, detail=detail, violations=violations, expected=0, actual=len(violations))

    def run_milestone3(self) -> list[AuditResult]:
        self.results = [self.run_row_count_audit_m2c(), self.run_pk_duplicate_audit_m2c()]
        return self.results

    def run_milestone2a(self) -> list[AuditResult]:
        self.results = [self.run_row_count_audit_m2a(), self.run_pk_duplicate_audit_m2a()]
        return self.results

    def run_milestone1(self) -> list[AuditResult]:
        """Run M1 audit suite (row_count + pk_duplicate)."""
        self.results = [self.run_row_count_audit_m1(), self.run_pk_duplicate_audit_m1()]
        return self.results

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
