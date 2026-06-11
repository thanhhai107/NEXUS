"""Ground Truth Extractor for TPC-DI benchmark data.

Extracts authoritative metadata from clean generated TPC-DI data:
  - Schemas (field names, types, nullability)
  - Semantic annotations (business names, types, PII)
  - Entity crosswalks (canonical IDs)
  - FK relationships (join graph)
  - Record count baselines
  - Data checksums (per-table SHA-256)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from benchmark.utils.hashing import hash_records
from benchmark.utils.io import load_tpcdi_data


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TPC_SCHEMAS_DIR = PROJECT_ROOT / "domains" / "tpc" / "schemas"
TPC_SEMANTIC_PATH = PROJECT_ROOT / "domains" / "tpc" / "semantic_rules.yml"
TPC_QUALITY_PATH = PROJECT_ROOT / "domains" / "tpc" / "quality_rules.yml"

TPCDI_TABLES = [
    "tpcdi_dim_date", "tpcdi_dim_time", "tpcdi_dim_account",
    "tpcdi_dim_customer", "tpcdi_dim_broker", "tpcdi_dim_security",
    "tpcdi_dim_company", "tpcdi_dim_trade",
    "tpcdi_fact_cash_balances", "tpcdi_fact_holdings",
    "tpcdi_fact_market_history", "tpcdi_fact_watches",
    "tpcdi_industry", "tpcdi_status_type", "tpcdi_tax_rate",
    "tpcdi_trade_type", "tpcdi_prospect",
]

TPCDI_FOREIGN_KEYS = {
    "tpcdi_dim_account": {
        "sk_customerid": "tpcdi_dim_customer.sk_customerid",
        "sk_brokerid": "tpcdi_dim_broker.sk_brokerid",
    },
    "tpcdi_dim_security": {
        "sk_companyid": "tpcdi_dim_company.sk_companyid",
    },
    "tpcdi_dim_company": {
        "industryid": "tpcdi_industry.in_id",
    },
    "tpcdi_dim_trade": {
        "sk_accountid": "tpcdi_dim_account.sk_accountid",
        "sk_securityid": "tpcdi_dim_security.sk_securityid",
        "sk_companyid": "tpcdi_dim_company.sk_companyid",
        "sk_closed_customerid": "tpcdi_dim_customer.sk_customerid",
        "sk_traded_customerid": "tpcdi_dim_customer.sk_customerid",
    },
    "tpcdi_fact_cash_balances": {
        "sk_customerid": "tpcdi_dim_customer.sk_customerid",
        "sk_accountid": "tpcdi_dim_account.sk_accountid",
        "sk_date": "tpcdi_dim_date.sk_date",
    },
    "tpcdi_fact_holdings": {
        "sk_tradeid": "tpcdi_dim_trade.sk_tradeid",
        "sk_customerid": "tpcdi_dim_customer.sk_customerid",
        "sk_accountid": "tpcdi_dim_account.sk_accountid",
        "sk_securityid": "tpcdi_dim_security.sk_securityid",
        "sk_companyid": "tpcdi_dim_company.sk_companyid",
        "sk_closedate": "tpcdi_dim_date.sk_date",
        "sk_closetime": "tpcdi_dim_time.sk_time",
    },
    "tpcdi_fact_market_history": {
        "sk_securityid": "tpcdi_dim_security.sk_securityid",
        "sk_companyid": "tpcdi_dim_company.sk_companyid",
        "sk_date": "tpcdi_dim_date.sk_date",
        "sk_fifty_two_week_high_date": "tpcdi_dim_date.sk_date",
        "sk_fifty_two_week_low_date": "tpcdi_dim_date.sk_date",
    },
    "tpcdi_fact_watches": {
        "sk_customerid": "tpcdi_dim_customer.sk_customerid",
        "sk_securityid": "tpcdi_dim_security.sk_securityid",
        "sk_closedate": "tpcdi_dim_date.sk_date",
    },
    "tpcdi_prospect": {
        "sk_recorddateid": "tpcdi_dim_date.sk_date",
        "sk_updatedateid": "tpcdi_dim_date.sk_date",
    },
}


class GroundTruthExtractor:
    """Extract ground-truth metadata from clean TPC-DI benchmark data."""

    def __init__(self, base_data_dir: Path | None = None):
        self.base_data_dir = base_data_dir

    def extract_all(self) -> dict[str, Any]:
        return {
            "schemas": self.extract_schemas(),
            "semantics": self.extract_semantics(),
            "entities": self.extract_entities(),
            "relationships": self.extract_relationships(),
            "baselines": self.extract_baselines(),
            "checksums": self.extract_checksums(),
        }

    def extract_schemas(self) -> dict[str, Any]:
        schemas: dict[str, Any] = {}
        for schema_file in sorted(TPC_SCHEMAS_DIR.glob("*.schema.json")):
            table_name = schema_file.stem.replace(".schema", "")
            schema = json.loads(schema_file.read_text(encoding="utf-8"))
            schemas[table_name] = {
                "fields": {
                    name: {
                        "type": prop.get("type", "unknown"),
                        "nullable": name not in schema.get("required", []),
                    }
                    for name, prop in schema.get("properties", {}).items()
                },
                "primary_keys": self._get_primary_keys(table_name),
                "foreign_keys": TPCDI_FOREIGN_KEYS.get(table_name, {}),
                "additionalProperties": schema.get("additionalProperties", True),
            }
        return schemas

    def extract_semantics(self) -> dict[str, Any]:
        semantics: dict[str, Any] = {}
        for table in TPCDI_TABLES:
            semantics[table] = {
                "business_name": table,
                "field_mappings": self._get_field_semantics(table),
                "grain": self._get_grain(table),
            }
        return semantics

    def extract_entities(self) -> dict[str, Any]:
        return {
            "accounts": {"table": "tpcdi_dim_account", "key": "sk_accountid"},
            "customers": {"table": "tpcdi_dim_customer", "key": "sk_customerid"},
            "brokers": {"table": "tpcdi_dim_broker", "key": "sk_brokerid"},
            "securities": {"table": "tpcdi_dim_security", "key": "sk_securityid"},
            "companies": {"table": "tpcdi_dim_company", "key": "sk_companyid"},
            "trades": {"table": "tpcdi_dim_trade", "key": "sk_tradeid"},
            "dates": {"table": "tpcdi_dim_date", "key": "sk_date"},
        }

    def extract_relationships(self) -> dict[str, Any]:
        return {"foreign_keys": TPCDI_FOREIGN_KEYS}

    def extract_baselines(self) -> dict[str, Any]:
        baselines: dict[str, Any] = {}
        for table in TPCDI_TABLES:
            try:
                records = load_tpcdi_data(table, self.base_data_dir)
                baselines[table] = {
                    "record_count": len(records),
                    "field_count": len(records[0]) if records else 0,
                    "fields": list(records[0].keys()) if records else [],
                }
            except FileNotFoundError:
                baselines[table] = {"record_count": 0, "field_count": 0, "fields": [], "error": "data not found"}
        return baselines

    def extract_checksums(self) -> dict[str, str]:
        checksums: dict[str, str] = {}
        for table in TPCDI_TABLES:
            try:
                records = load_tpcdi_data(table, self.base_data_dir)
                import hashlib
                import json
                sorted_json = json.dumps(
                    sorted(hash_records(records)), sort_keys=True
                ).encode("utf-8")
                checksums[table] = hashlib.sha256(sorted_json).hexdigest()
            except FileNotFoundError:
                checksums[table] = "N/A"
        return checksums

    def _get_primary_keys(self, table: str) -> list[str]:
        pk_map = {
            "tpcdi_dim_date": ["sk_date"],
            "tpcdi_dim_time": ["sk_time"],
            "tpcdi_dim_account": ["sk_accountid"],
            "tpcdi_dim_customer": ["sk_customerid"],
            "tpcdi_dim_broker": ["sk_brokerid"],
            "tpcdi_dim_security": ["sk_securityid"],
            "tpcdi_dim_company": ["sk_companyid"],
            "tpcdi_dim_trade": ["sk_tradeid"],
            "tpcdi_fact_cash_balances": ["sk_customerid", "sk_accountid", "sk_date"],
            "tpcdi_fact_holdings": ["sk_tradeid", "sk_customerid", "sk_securityid", "sk_closedate"],
            "tpcdi_fact_market_history": ["sk_securityid", "sk_date"],
            "tpcdi_fact_watches": ["sk_customerid", "sk_securityid"],
            "tpcdi_industry": ["in_id"],
            "tpcdi_status_type": ["st_id"],
            "tpcdi_tax_rate": ["tx_id"],
            "tpcdi_trade_type": ["tt_id"],
            "tpcdi_prospect": ["agencyid"],
        }
        return pk_map.get(table, [])

    def _get_field_semantics(self, table: str) -> dict[str, Any]:
        field_semantics: dict[str, Any] = {}
        schema_file = TPC_SCHEMAS_DIR / f"{table}.schema.json"
        if not schema_file.exists():
            return field_semantics
        schema = json.loads(schema_file.read_text(encoding="utf-8"))
        for name in schema.get("properties", {}):
            stype = "unknown"
            if name.endswith("_sk"):
                stype = "foreign_key"
            elif name.endswith("_id"):
                stype = "identifier"
            elif any(t in name.lower() for t in ["name", "first", "last"]):
                stype = "person_name"
            elif "email" in name.lower():
                stype = "email"
            elif any(t in name.lower() for t in ["date", "time"]):
                stype = "temporal"
            elif any(t in name.lower() for t in ["price", "cost", "amount", "profit", "tax", "revenue", "coupon"]):
                stype = "monetary_amount"
            elif any(t in name.lower() for t in ["quantity", "count", "number"]):
                stype = "quantity"
            elif any(t in name.lower() for t in ["flag"]):
                stype = "categorical"
            field_semantics[name] = {"semantic_type": stype}
        return field_semantics

    def _get_grain(self, table: str) -> str:
        grain_map = {
            "tpcdi_dim_date": "one row per calendar date",
            "tpcdi_dim_time": "one row per second of day",
            "tpcdi_dim_account": "one row per brokerage account",
            "tpcdi_dim_customer": "one row per customer (SCD Type 2)",
            "tpcdi_dim_broker": "one row per broker",
            "tpcdi_dim_security": "one row per security (SCD Type 2)",
            "tpcdi_dim_company": "one row per company (SCD Type 2)",
            "tpcdi_dim_trade": "one row per trade execution (SCD Type 2)",
            "tpcdi_fact_cash_balances": "one row per account per day",
            "tpcdi_fact_holdings": "one row per holding per day",
            "tpcdi_fact_market_history": "one row per security per trading day",
            "tpcdi_fact_watches": "one row per customer security watch",
            "tpcdi_industry": "one row per industry classification",
            "tpcdi_status_type": "one row per status value",
            "tpcdi_tax_rate": "one row per tax rate type",
            "tpcdi_trade_type": "one row per trade type",
            "tpcdi_prospect": "one row per prospect",
        }
        return grain_map.get(table, "unknown")

    def save_ground_truth(self, output_dir: Path | None = None) -> Path:
        output_dir = output_dir or (PROJECT_ROOT / "benchmark" / "ground_truth" / "data")
        output_dir.mkdir(parents=True, exist_ok=True)
        truth = self.extract_all()
        filepath = output_dir / "ground_truth.json"
        with filepath.open("w", encoding="utf-8") as f:
            json.dump(truth, f, indent=2, default=str)
        return filepath
