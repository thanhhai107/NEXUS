"""Error Injection Framework — core engine.

Orchestrates deterministic error injection across 6 categories and 35+ error types,
producing derived data sources from clean TPC-DS SF=1 ground truth.
"""

from __future__ import annotations

import copy
import random
from pathlib import Path
from typing import Any

from benchmark.utils.hashing import hash_records, InjectionLog
from benchmark.utils.io import load_tpcdi_data, save_derived_source, save_injection_log
from benchmark.platform_testing.injection.schema_injector import SchemaInjector
from benchmark.platform_testing.injection.semantic_injector import SemanticInjector
from benchmark.platform_testing.injection.quality_injector import QualityInjector
from benchmark.platform_testing.injection.heterogeneity_injector import HeterogeneityInjector
from benchmark.platform_testing.injection.reliability_injector import ReliabilityInjector


INJECTOR_REGISTRY: dict[str, Any] = {
    "schema_issues": SchemaInjector,
    "semantic_issues": SemanticInjector,
    "data_quality": QualityInjector,
    "heterogeneity": HeterogeneityInjector,
    "reliability": ReliabilityInjector,
}


class InjectionEngine:
    """Orchestrates error injection across all categories and derived sources."""

    def __init__(self, seed: int = 42, base_data_dir: Path | None = None):
        self.seed = seed
        self.base_data_dir = base_data_dir
        self.injection_logs: list[InjectionLog] = []
        self._loaded_data: dict[str, list[dict[str, Any]]] = {}

    def run_scenario(self, config: dict[str, Any]) -> dict[str, Any]:
        self.seed = config.get("seed", self.seed)
        random.seed(self.seed)
        self.injection_logs = []

        scenario_id = config["id"]
        scenario_level = config.get("level", 1)
        errors = config.get("errors", [])
        source_count = config.get("source_count", 1)

        results: dict[str, Any] = {
            "scenario_id": scenario_id,
            "level": scenario_level,
            "seed": self.seed,
            "sources": [],
            "total_errors_injected": 0,
        }

        for source_idx in range(source_count):
            source_errors = (
                [e for e in errors if e.get("source_index", 0) == source_idx]
                if any(e.get("source_index") is not None for e in errors)
                else errors
            )

            source_result = self._inject_source(
                scenario_id=scenario_id,
                source_index=source_idx,
                errors=source_errors,
                source_config=config.get("source_configs", [{}])[source_idx] if source_idx < len(config.get("source_configs", [{}])) else {},
            )
            results["sources"].append(source_result)
            results["total_errors_injected"] += source_result["errors_injected"]

        results["injection_log_count"] = len(self.injection_logs)

        log_path = save_injection_log(
            scenario_id,
            [log.to_dict() for log in self.injection_logs],
        )
        results["injection_log_path"] = str(log_path)

        return results

    def _inject_source(
        self,
        scenario_id: str,
        source_index: int,
        errors: list[dict[str, Any]],
        source_config: dict[str, Any],
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "source_index": source_index,
            "tables": {},
            "errors_injected": 0,
        }

        tables = source_config.get("tables", []) or list(
            {e.get("table", "") for e in errors if e.get("table")}
        )

        if not tables:
            tables = [
                "tpcdi_dim_customer", "tpcdi_dim_account", "tpcdi_dim_broker",
                "tpcdi_dim_security", "tpcdi_dim_company", "tpcdi_dim_trade",
                "tpcdi_fact_cash_balances", "tpcdi_fact_holdings",
                "tpcdi_fact_market_history", "tpcdi_fact_watches",
                "tpcdi_dim_date", "tpcdi_dim_time", "tpcdi_industry",
                "tpcdi_status_type", "tpcdi_tax_rate", "tpcdi_trade_type",
                "tpcdi_prospect",
            ]

        for table in tables:
            records = self._load_table(table)
            modified_records = list(records)
            table_logs: list[InjectionLog] = []

            for error in errors:
                if error.get("table") and error.get("table") != table:
                    continue
                if not error.get("table"):
                    error_table = self._resolve_table(error, table)
                    if error_table != table:
                        continue

                result_records, error_log = self._apply_error(
                    scenario_id, table, modified_records, error
                )
                modified_records = result_records
                if error_log:
                    table_logs.append(error_log)
                    self.injection_logs.append(error_log)

            output_format = source_config.get("format", "csv")
            output_path = save_derived_source(
                modified_records,
                table,
                scenario_id=f"{scenario_id}_source{source_index}",
                format=output_format,
            )

            result["tables"][table] = {
                "original_count": len(records),
                "modified_count": len(modified_records),
                "errors_applied": len(table_logs),
                "output_path": str(output_path),
                "output_format": output_format,
            }
            result["errors_injected"] += len(table_logs)

        return result

    def _load_table(self, table: str) -> list[dict[str, Any]]:
        if table not in self._loaded_data:
            self._loaded_data[table] = load_tpcdi_data(table, self.base_data_dir)
        return copy.deepcopy(self._loaded_data[table])

    def _apply_error(
        self,
        scenario_id: str,
        table: str,
        records: list[dict[str, Any]],
        error: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], InjectionLog | None]:
        category = error.get("category", "")
        error_type = error.get("type", "")
        params = error.get("params", {})

        injector_cls = INJECTOR_REGISTRY.get(category)
        if injector_cls is None:
            return records, None

        injector = injector_cls(seed=self.seed)
        method_name = f"inject_{error_type}"
        method = getattr(injector, method_name, None)

        if method is None:
            return records, None

        original_hashes = set(hash_records(records))
        modified_records = method(records, **params)
        modified_hashes = set(hash_records(modified_records))
        injected_hashes = list(modified_hashes - original_hashes)

        log = InjectionLog(
            scenario_id=scenario_id,
            table=table,
            error_type=error_type,
            error_category=category,
            target_field=params.get("field") or (params.get("fields", [None])[0] if isinstance(params.get("fields"), list) else None),
            injected_record_hashes=injected_hashes,
            parameters=params,
            expected_detection=error.get("expected_detection", ""),
        )

        return modified_records, log

    @staticmethod
    def _resolve_table(error: dict[str, Any], current_table: str) -> str:
        tables = error.get("tables", [])
        if not tables:
            return current_table
        if isinstance(tables, str):
            return tables
        return current_table
