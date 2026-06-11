"""
TPC-DI Config/Infrastructure Error Injector.

Simulates infrastructure-level Source Heterogeneity issues: API failures,
batch frequency mismatches, and REST adapter mocking.

Supported mutation_types:
  simulate_api_failure         — Write an error response file simulating API downtime.
  batch_frequency_mismatch     — Duplicate or skip batch directories to simulate
                                  out-of-order or double-delivered batches.
  mock_rest_adapter            — Write a mock REST response JSON file that replaces
                                  a source file (simulates a REST-polled source).

Usage::

    from ingestion.tpcdi.error_injection.config_injector import ConfigInjector

    ci = ConfigInjector(seed=42)
    scenario_root = ci.create_scenario(
        "cfg_api_fail_001",
        mutation_type="simulate_api_failure",
        target_source="trade",
        batch_id="batch1",
    )
"""

from __future__ import annotations

import json
import random
import shutil
from pathlib import Path
from typing import Any

from common.tpcdi_sources import source_root

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCENARIO_BASE = Path("runtime/tpcdi/scenarios")

_BATCH_MAP = {"batch1": "Batch1", "batch2": "Batch2", "batch3": "Batch3"}


class ConfigInjector:
    """Infrastructure/config mutation injector for TPC-DI scenarios."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        self.rng = random.Random(seed)

    def create_scenario(
        self,
        scenario_id: str,
        *,
        mutation_type: str,
        target_source: str = "trade",
        batch_id: str = "batch1",
        **kwargs: Any,
    ) -> Path:
        """Create scenario with one config/infra mutation."""
        clean_root = source_root()
        scenario_root_dir = PROJECT_ROOT / SCENARIO_BASE / scenario_id
        src_dir = scenario_root_dir / "source"

        if src_dir.exists():
            shutil.rmtree(src_dir)
        shutil.copytree(clean_root, src_dir)

        method = getattr(self, f"_inject_{mutation_type}", None)
        if method is None:
            raise ValueError(f"Unknown config mutation_type: {mutation_type!r}")

        mutations = method(scenario_root_dir, src_dir, target_source, batch_id, **kwargs)

        manifest = {
            "scenario_id": scenario_id,
            "seed": self.seed,
            "base_source_root": str(clean_root),
            "scenario_root": str(scenario_root_dir),
            "scenario_source_root": str(src_dir),
            "target_source": target_source,
            "batch": batch_id,
            "mutation_type": mutation_type,
            "mutations": mutations,
        }
        (scenario_root_dir / "injection_manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
        return scenario_root_dir

    # ── Mutation implementations ─────────────────────────────────────────────

    def _inject_simulate_api_failure(
        self,
        scenario_root_dir: Path,
        src_dir: Path,
        target_source: str,
        batch_id: str,
        *,
        http_status: int = 503,
        error_message: str = "Service Unavailable",
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Write an HTTP error response file to simulate API downtime."""
        api_dir = scenario_root_dir / "api_responses"
        api_dir.mkdir(exist_ok=True)

        error_response = {
            "status": http_status,
            "error": error_message,
            "source": target_source,
            "batch": batch_id,
            "retryable": http_status in (429, 503, 504),
            "retry_after_seconds": 60 if http_status == 429 else None,
        }
        response_file = api_dir / f"{target_source}_api_error.json"
        response_file.write_text(json.dumps(error_response, indent=2), encoding="utf-8")

        # Also create a sentinel file in source dir to indicate the source is "unavailable"
        batch_name = _BATCH_MAP.get(batch_id, batch_id.capitalize())
        batch_dir = src_dir / batch_name
        if batch_dir.exists():
            sentinel = batch_dir / f"{target_source}_UNAVAILABLE.flag"
            sentinel.write_text(
                json.dumps({"reason": error_message, "http_status": http_status}),
                encoding="utf-8",
            )

        return [{
            "mutation_type": "simulate_api_failure",
            "source_name": target_source,
            "target_source": target_source,
            "http_status": http_status,
            "error_message": error_message,
            "response_file": str(response_file.relative_to(scenario_root_dir)),
            "expected_detection": "source_unavailable",
            "expected_stage": "ingestion",
            "recoverable": http_status in (429, 503, 504),
            "recovery_hint": "retry_with_backoff",
        }]

    def _inject_batch_frequency_mismatch(
        self,
        scenario_root_dir: Path,
        src_dir: Path,
        target_source: str,
        batch_id: str,
        *,
        mismatch_type: str = "double_delivery",
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Simulate out-of-order or double-delivered batches.

        mismatch_type:
          double_delivery — duplicate Batch1 as Batch1_duplicate
          skip_batch      — rename Batch1 to Batch3 (gap in sequence)
          out_of_order    — swap Batch1 and Batch2 directories
        """
        batch_name = _BATCH_MAP.get(batch_id, batch_id.capitalize())
        batch_dir = src_dir / batch_name

        if not batch_dir.exists():
            return [{"mutation_type": "batch_frequency_mismatch",
                "source_name": target_source, "skipped": f"{batch_name}_not_found"}]

        mutations: list[dict[str, Any]] = []

        if mismatch_type == "double_delivery":
            dup_dir = src_dir / f"{batch_name}_duplicate"
            shutil.copytree(batch_dir, dup_dir)
            mutations.append({
                "mutation_type": "batch_frequency_mismatch",
                "source_name": target_source,
                "mismatch_type": "double_delivery",
                "original_batch": batch_name,
                "duplicate_batch": f"{batch_name}_duplicate",
                "expected_detection": "duplicate_batch",
                "expected_stage": "ingestion",
                "recoverable": True,
                "recovery_hint": "idempotent_ingestion_check",
            })

        elif mismatch_type == "skip_batch":
            # Move Batch1 → Batch3 to simulate a gap
            skip_target = src_dir / "Batch3"
            if skip_target.exists():
                shutil.rmtree(skip_target)
            shutil.move(str(batch_dir), str(skip_target))
            mutations.append({
                "mutation_type": "batch_frequency_mismatch",
                "source_name": target_source,
                "mismatch_type": "skip_batch",
                "original_path": batch_name,
                "new_path": "Batch3",
                "expected_detection": "batch_sequence_gap",
                "expected_stage": "ingestion",
                "recoverable": False,
            })

        elif mismatch_type == "out_of_order":
            batch2_dir = src_dir / "Batch2"
            if batch_dir.exists() and batch2_dir.exists():
                tmp = src_dir / "_tmp_swap"
                shutil.copytree(batch_dir, tmp)
                shutil.rmtree(batch_dir)
                shutil.copytree(batch2_dir, batch_dir)
                shutil.rmtree(batch2_dir)
                shutil.copytree(tmp, batch2_dir)
                shutil.rmtree(tmp)
                mutations.append({
                    "mutation_type": "batch_frequency_mismatch",
                "source_name": target_source,
                    "mismatch_type": "out_of_order",
                    "swapped": ["Batch1", "Batch2"],
                    "expected_detection": "batch_sequence_gap",
                    "expected_stage": "ingestion",
                    "recoverable": True,
                    "recovery_hint": "sort_by_batch_timestamp",
                })
            else:
                mutations.append({
                    "mutation_type": "batch_frequency_mismatch",
                "source_name": target_source,
                    "mismatch_type": "out_of_order",
                    "skipped": "batch2_not_found",
                })
        else:
            raise ValueError(f"Unknown mismatch_type: {mismatch_type!r}")

        return mutations

    def _inject_mock_rest_adapter(
        self,
        scenario_root_dir: Path,
        src_dir: Path,
        target_source: str,
        batch_id: str,
        *,
        n_records: int = 50,
        **_: Any,
    ) -> list[dict[str, Any]]:
        """Write a mock REST API response JSON that replaces the flat source file.

        Simulates a source that serves data via REST (paginated JSON) rather
        than flat file, creating heterogeneity that requires an adapter.
        """
        batch_name = _BATCH_MAP.get(batch_id, batch_id.capitalize())
        batch_dir = src_dir / batch_name

        # Build mock response payload
        records = [
            {
                "id": i,
                "source": target_source,
                "batch": batch_id,
                "value": self.rng.uniform(100.0, 10000.0),
                "timestamp": f"2024-01-{(i % 28) + 1:02d}T00:00:00Z",
            }
            for i in range(n_records)
        ]
        rest_response = {
            "page": 1,
            "page_size": n_records,
            "total_records": n_records,
            "records": records,
        }

        # Write REST response alongside original file
        rest_file = batch_dir / f"{target_source}_rest_response.json"
        rest_file.mkdir() if False else None  # type guard
        rest_file.write_text(json.dumps(rest_response, indent=2), encoding="utf-8")

        # Write adapter config indicating this source needs JSON/REST handling
        adapter_config = {
            "source": target_source,
            "batch": batch_id,
            "adapter_type": "rest_json",
            "response_file": str(rest_file.relative_to(src_dir)),
            "pagination_key": "page",
            "records_key": "records",
        }
        adapter_cfg_file = scenario_root_dir / "adapter_config.json"
        adapter_cfg_file.write_text(json.dumps(adapter_config, indent=2), encoding="utf-8")

        return [{
            "mutation_type": "mock_rest_adapter",
            "source_name": target_source,
            "target_source": target_source,
            "rest_response_file": str(rest_file.relative_to(scenario_root_dir)),
            "adapter_config_file": str(adapter_cfg_file.relative_to(scenario_root_dir)),
            "n_records": n_records,
            "expected_detection": "format_mismatch",
            "expected_stage": "ingestion",
            "recoverable": True,
            "recovery_hint": "use_rest_adapter",
        }]


__all__ = ["ConfigInjector"]
