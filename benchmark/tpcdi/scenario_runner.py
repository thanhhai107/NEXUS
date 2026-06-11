"""
TPC-DI Scenario Runner — end-to-end: inject → detect → recover → score.

Usage::

    from benchmark.tpcdi.scenario_runner import TpcdiScenarioRunner

    runner = TpcdiScenarioRunner(scale_factor=3)
    result = runner.run_scenario(
        scenario_id="extra_field_trade_001",
        mutation_type="extra_field",
        target_source="trade",
        line_numbers=[100, 200, 300],
        seed=42,
        recover=True,
    )
    print(result["scoring_report"])
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from benchmark.tpcdi.runner import TpcdiRunner
from benchmark.tpcdi.scoring import TpcdiScoringEngine
from governance.quality.bronze_validator import validate_bronze_tpcdi_file
from governance.recovery.engine import RecoveryEngine
from ingestion.tpcdi.error_injection.error_collector import collect_detected_errors, write_detected_errors
from ingestion.tpcdi.error_injection.source_injector import TpcdiSourceInjector
from ingestion.tpcdi.error_injection.semantic_injector import SemanticInjector
from ingestion.tpcdi.error_injection.format_injector import FormatInjector
from ingestion.tpcdi.error_injection.lineage_injector import LineageInjector
from ingestion.tpcdi.error_injection.reliability_injector import ReliabilityInjector
from ingestion.tpcdi.error_injection.config_injector import ConfigInjector
from ingestion.tpcdi.error_injection.schema_injector import SchemaInjector


class TpcdiScenarioRunner:
    """Orchestrate inject → detect → recover → score for one scenario."""

    def __init__(self, scale_factor: int = 3):
        self.scale_factor = scale_factor
        self.scorer = TpcdiScoringEngine()
        self.recovery = RecoveryEngine()

    def run_scenario(
        self,
        scenario_id: str,
        mutation_type: str,
        *,
        target_source: str = "trade",
        batch_id: str = "batch1",
        injector_type: str = "source",
        line_numbers: list[int] | None = None,
        seed: int = 42,
        recover: bool = True,
        extra_kwargs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run one scenario end-to-end with automatic injector dispatch.

        Parameters
        ----------
        injector_type:
            One of: source, semantic, format, lineage, reliability, config, schema.
        extra_kwargs:
            Additional keyword arguments forwarded to the injector create_scenario().
        """
        original_source_root = os.environ.get("TPCDI_SOURCE_ROOT")
        os.environ.pop("TPCDI_SOURCE_ROOT", None)

        is_dup = mutation_type == "duplicate_pk"
        bronze: dict[str, Any] | None = None
        before_result: Any = None
        detected: list[dict[str, Any]] = []
        before_status = "unknown"
        after_status = "unknown"

        try:
            # 1. Inject via dispatch to appropriate injector
            scenario_root = self._create_scenario(
                scenario_id=scenario_id,
                injector_type=injector_type,
                mutation_type=mutation_type,
                target_source=target_source,
                batch_id=batch_id,
                seed=seed,
                line_numbers=line_numbers,
                extra_kwargs=extra_kwargs or {},
            )
            source_dir = scenario_root / "source"
            scenario_root = Path(scenario_root)

            # 2. Detect (bronze validation only for source injector)
            os.environ["TPCDI_SOURCE_ROOT"] = str(source_dir)
            bronze: dict[str, Any] | None = None
            before_result: Any = None
            if is_dup:
                before_result = TpcdiRunner(scale_factor=self.scale_factor).run_milestone4(clean_outputs=True)
                before_status = "valid" if before_result.is_valid else "invalid"
                detected = collect_detected_errors(
                    scenario_id=scenario_id,
                    run_id=self.scorer.run_id,
                    audit_results=before_result.correctness_details,
                )
            elif injector_type == "source":
                bronze = validate_bronze_tpcdi_file(
                    source_name=target_source,
                    batch_id=batch_id,
                    chunk_size=50000,
                )
                before_status = bronze.get("status", "unknown")
                detected = collect_detected_errors(
                    scenario_id=scenario_id,
                    run_id=self.scorer.run_id,
                    bronze_validation=bronze,
                )
            else:
                # Non-source injectors: detection via scenario inspection only
                before_status = "scenario_inspected"
                detected = collect_detected_errors(
                    scenario_id=scenario_id,
                    run_id=self.scorer.run_id,
                )

            # 3. Write detected_errors.json (always, before recovery)
            write_detected_errors(detected, scenario_root / "detected_errors.json")

            # 4. Run early scoring (detect-only)
            detect_report = self.scorer.score_detection(
                scenario_root / "injection_manifest.json",
                detected_errors=detected,
            )

            # 5. Recover
            recovery_report = None
            after_detected: list[dict[str, Any]] = []
            if recover:
                recovery_report = self.recovery.run(
                    scenario_root,
                    detected_errors=detected,
                )

                # 6. Rerun pipeline on recovered source
                recovered_dir = scenario_root / "recovered_source"
                if recovery_report.repaired > 0 and recovered_dir.exists():
                    os.environ["TPCDI_SOURCE_ROOT"] = str(recovered_dir)
                    if is_dup:
                        rerun = TpcdiRunner(scale_factor=self.scale_factor).run_milestone4(clean_outputs=True)
                        after_status = "valid" if rerun.is_valid else "invalid"
                        after_detected = collect_detected_errors(
                            scenario_id=scenario_id,
                            run_id=self.scorer.run_id,
                            audit_results=rerun.correctness_details,
                        )
                    elif injector_type == "source":
                        after_bronze = validate_bronze_tpcdi_file(
                            source_name=target_source,
                            batch_id=batch_id,
                            chunk_size=50000,
                        )
                        after_status = after_bronze.get("status", "unknown")
                        after_detected = collect_detected_errors(
                            scenario_id=scenario_id,
                            run_id=self.scorer.run_id,
                            bronze_validation=after_bronze,
                        )
                    else:
                        after_status = "scenario_inspected"
                        after_detected = collect_detected_errors(
                            scenario_id=scenario_id,
                            run_id=self.scorer.run_id,
                        )
                    write_detected_errors(
                        after_detected, scenario_root / "detected_errors_after_recovery.json"
                    )

            # 7. Full scoring
            full_report = self.scorer.score_full(
                scenario_root,
                bronze_validation=bronze,
                audit_results=before_result.correctness_details if is_dup else None,
                detected_errors=detected,
                recovery_log_path=scenario_root / "recovery_log.json" if recover else None,
            )

            # 8. Write scoring report
            self.scorer.write_report(full_report, scenario_root)

            return {
                "scenario_id": scenario_id,
                "scenario_root": str(scenario_root),
                "before_status": before_status,
                "after_status": after_status,
                "detect_report": detect_report.to_dict(),
                "scoring_report": full_report.to_dict(),
                "recovery_report": recovery_report.to_dict() if recovery_report else None,
                "after_detected_count": len(after_detected),
                "artifacts": {
                    "manifest": str(scenario_root / "injection_manifest.json"),
                    "detected": str(scenario_root / "detected_errors.json"),
                    "recovery_log": str(scenario_root / "recovery_log.json"),
                    "scoring_report": str(scenario_root / "scoring_report.json"),
                },
            }

        finally:
            # Restore TPCDI_SOURCE_ROOT
            if original_source_root is not None:
                os.environ["TPCDI_SOURCE_ROOT"] = original_source_root
            elif "TPCDI_SOURCE_ROOT" in os.environ:
                os.environ.pop("TPCDI_SOURCE_ROOT", None)

    def _create_scenario(
        self,
        scenario_id: str,
        injector_type: str,
        mutation_type: str,
        target_source: str,
        batch_id: str,
        seed: int,
        line_numbers: list[int] | None,
        extra_kwargs: dict[str, Any],
    ) -> Path:
        """Dispatch to the correct injector class and call create_scenario()."""
        injector = self._get_injector(injector_type, seed)
        kwargs: dict[str, Any] = dict(extra_kwargs)

        if injector_type == "source":
            kwargs.setdefault("line_numbers", line_numbers or [100, 200, 300])
        if injector_type == "schema":
            kwargs.setdefault("schema_mutation", kwargs.get("schema_mutation", mutation_type))

        if injector_type in ("lineage", "config"):
            return injector.create_scenario(
                scenario_id,
                mutation_type=mutation_type,
                target_source=target_source,
                batch_id=batch_id,
                **kwargs,
            )

        return injector.create_scenario(
            scenario_id,
            target_source=target_source,
            batch_id=batch_id,
            mutation_type=mutation_type,
            **kwargs,
        )

    @staticmethod
    def _get_injector(injector_type: str, seed: int) -> Any:
        """Return an injector instance for the given type."""
        injectors = {
            "source": TpcdiSourceInjector,
            "semantic": SemanticInjector,
            "format": FormatInjector,
            "lineage": LineageInjector,
            "reliability": ReliabilityInjector,
            "config": ConfigInjector,
            "schema": SchemaInjector,
        }
        cls = injectors.get(injector_type)
        if cls is None:
            raise ValueError(f"Unknown injector_type: {injector_type!r}")
        return cls(seed=seed)