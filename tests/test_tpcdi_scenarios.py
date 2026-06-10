"""E2E scenario tests for TPC-DI pipeline: inject → detect → recover → score."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from benchmark.tpcdi.scenario_runner import TpcdiScenarioRunner
from common.tpcdi_sources import source_root


SCENARIO_BASE = Path("runtime/tpcdi/scenarios")
SCENARIOS = [
    ("extra_field_trade_pytest", "extra_field"),
    ("type_error_trade_pytest", "type_error"),
    # duplicate_pk runs full M4 (~4 min) — uncomment for full regression
    # ("duplicate_pk_trade_pytest", "duplicate_pk"),
]


def require_tpcdi_source_root() -> None:
    root = source_root()
    if not root.exists():
        pytest.skip(f"TPC-DI DIGen source root is missing: {root}")


@pytest.fixture(scope="module")
def runner():
    return TpcdiScenarioRunner(scale_factor=3)


@pytest.fixture(autouse=True)
def clean_env():
    """Ensure TPCDI_SOURCE_ROOT is clean before each test."""
    os.environ.pop("TPCDI_SOURCE_ROOT", None)
    yield
    os.environ.pop("TPCDI_SOURCE_ROOT", None)


class TestTpcdiScenarios:
    """End-to-end scenario tests."""

    @pytest.mark.parametrize("scenario_id, mutation_type", SCENARIOS)
    def test_scenario_full(self, runner, scenario_id, mutation_type):
        require_tpcdi_source_root()
        result = runner.run_scenario(
            scenario_id=scenario_id,
            mutation_type=mutation_type,
            target_source="trade",
            batch_id="batch1",
            line_numbers=[100, 200, 300],
            seed=42,
            recover=True,
        )

        report = result["scoring_report"]

        # Detection
        assert report["total_injected"] == 3, f"expected 3, got {report['total_injected']}"
        assert report["detection"]["true_positives"] == 3, \
            f"TP expected 3, got {report['detection']['true_positives']}"
        assert report["detection"]["false_negatives"] == 0
        assert report["detection"]["detection_rate"] == 1.0

        # Recovery
        assert report["recovery"]["repaired"] == 3, \
            f"repaired expected 3, got {report['recovery']['repaired']}"
        assert report["recovery"]["end_to_end_recovery_rate"] == 1.0

        # Artifacts
        for art_path in result["artifacts"].values():
            assert Path(art_path).exists(), f"Missing artifact: {art_path}"

        # Status
        assert result["before_status"] in ("failed", "invalid")
        assert result["after_status"] in ("passed", "valid")

        # Clean scenario directory
        scenario_dir = SCENARIO_BASE / scenario_id
        if scenario_dir.exists():
            shutil.rmtree(scenario_dir)

    def test_clean_m4_baseline(self):
        """Verify clean baseline still passes after scenario runs."""
        from benchmark.tpcdi.runner import TpcdiRunner
        require_tpcdi_source_root()
        os.environ.pop("TPCDI_SOURCE_ROOT", None)
        result = TpcdiRunner(scale_factor=3).run_milestone4(clean_outputs=True)
        assert result.is_valid, f"Clean M4 should be valid, got errors: {result.errors}"
        assert result.correctness_all_passed
