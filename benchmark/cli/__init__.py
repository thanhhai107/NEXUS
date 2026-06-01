"""Benchmark CLI — run injection, evaluation, and reporting."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


def cmd_extract_ground_truth(args: argparse.Namespace) -> None:
    from benchmark.ground_truth.extractor import GroundTruthExtractor
    extractor = GroundTruthExtractor(args.data_dir or None)
    path = extractor.save_ground_truth()
    print(json.dumps({"status": "ok", "ground_truth_path": str(path)}, indent=2))


def cmd_inject(args: argparse.Namespace) -> None:
    from benchmark.platform_testing.injection.engine import InjectionEngine
    from benchmark.utils.io import load_scenario_config

    config = load_scenario_config(args.scenario)
    engine = InjectionEngine(seed=config.get("seed", 42), base_data_dir=args.data_dir or None)
    result = engine.run_scenario(config)
    print(json.dumps(result, indent=2, default=str))


def cmd_inject_all(args: argparse.Namespace) -> None:
    from benchmark.platform_testing.injection.engine import InjectionEngine
    from benchmark.utils.io import SCENARIOS_DIR, load_scenario_config

    scenarios = sorted(SCENARIOS_DIR.glob("*.yml"))
    if args.level:
        scenarios = [s for s in scenarios if s.stem.startswith(f"l{args.level}")]

    results = {}
    for sc_path in scenarios:
        config = load_scenario_config(sc_path.stem)
        engine = InjectionEngine(seed=config.get("seed", 42), base_data_dir=args.data_dir or None)
        results[sc_path.stem] = engine.run_scenario(config)
        print(f"  [{sc_path.stem}] injected {results[sc_path.stem].get('total_errors_injected', 0)} errors")

    print(json.dumps({"status": "ok", "scenarios_run": len(results)}, indent=2))


def cmd_evaluate(args: argparse.Namespace) -> None:
    from benchmark.platform_testing.evaluation.engine import EvaluationEngine

    gt_path = Path(args.ground_truth) if args.ground_truth else None
    engine = EvaluationEngine(ground_truth_path=gt_path)

    platform_outputs = {}
    if args.platform_outputs:
        platform_outputs = json.loads(Path(args.platform_outputs).read_text(encoding="utf-8"))

    scorecard = engine.evaluate_scenario(args.scenario, platform_outputs)
    print(json.dumps(scorecard, indent=2, default=str))


def cmd_evaluate_all(args: argparse.Namespace) -> None:
    from benchmark.platform_testing.evaluation.engine import EvaluationEngine
    from benchmark.utils.io import SCENARIOS_DIR

    gt_path = Path(args.ground_truth) if args.ground_truth else None
    engine = EvaluationEngine(ground_truth_path=gt_path)

    platform_outputs = {}
    if args.platform_outputs:
        platform_outputs = json.loads(Path(args.platform_outputs).read_text(encoding="utf-8"))

    scenarios = sorted(SCENARIOS_DIR.glob("*.yml"))
    if args.level:
        scenarios = [s for s in scenarios if s.stem.startswith(f"l{args.level}")]

    report = engine.evaluate_all(
        [s.stem for s in scenarios],
        platform_outputs,
    )
    print(json.dumps(report["summary"], indent=2, default=str))


def cmd_list_scenarios(_args: argparse.Namespace) -> None:
    from benchmark.utils.io import SCENARIOS_DIR
    scenarios = sorted(SCENARIOS_DIR.glob("*.yml"))
    print(json.dumps({
        "scenarios": [s.stem for s in scenarios],
        "count": len(scenarios),
    }, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NEXUS Benchmark Framework CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    extract = sub.add_parser("extract-ground-truth", help="Extract ground truth from clean TPC-DS SF=1")
    extract.add_argument("--data-dir", type=Path, help="Path to TPC-DS data directory")
    extract.set_defaults(func=cmd_extract_ground_truth)

    inject = sub.add_parser("inject", help="Run error injection for one scenario")
    inject.add_argument("--scenario", required=True, help="Scenario ID (e.g. l1_schema)")
    inject.add_argument("--data-dir", type=Path, help="Path to clean TPC-DS data")
    inject.set_defaults(func=cmd_inject)

    inject_all = sub.add_parser("inject-all", help="Run error injection for all scenarios")
    inject_all.add_argument("--level", type=int, choices=[1, 2, 3, 4], help="Filter by difficulty level")
    inject_all.add_argument("--data-dir", type=Path, help="Path to clean TPC-DS data")
    inject_all.set_defaults(func=cmd_inject_all)

    evaluate = sub.add_parser("evaluate", help="Evaluate platform outputs against ground truth")
    evaluate.add_argument("--scenario", required=True, help="Scenario ID")
    evaluate.add_argument("--ground-truth", help="Path to ground_truth.json")
    evaluate.add_argument("--platform-outputs", help="Path to platform outputs JSON")
    evaluate.set_defaults(func=cmd_evaluate)

    evaluate_all = sub.add_parser("evaluate-all", help="Evaluate all scenarios")
    evaluate_all.add_argument("--level", type=int, choices=[1, 2, 3, 4])
    evaluate_all.add_argument("--ground-truth", help="Path to ground_truth.json")
    evaluate_all.add_argument("--platform-outputs", help="Path to aggregated platform outputs JSON")
    evaluate_all.set_defaults(func=cmd_evaluate_all)

    list_cmd = sub.add_parser("list-scenarios", help="List available benchmark scenarios")
    list_cmd.set_defaults(func=cmd_list_scenarios)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
