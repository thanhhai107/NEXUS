"""I/O utilities for loading TPC-DI data, saving derived sources, and managing configs."""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TPCDI_RUNTIME_DIR = PROJECT_ROOT / "runtime" / "datasets" / "tpcdi"
BENCHMARK_DIR = PROJECT_ROOT / "benchmark"
SCENARIOS_DIR = BENCHMARK_DIR / "scenarios"
REPORTS_DIR = BENCHMARK_DIR / "reports"
DERIVED_DATA_DIR = BENCHMARK_DIR / "derived_data"


def _detect_delimiter(filepath: Path) -> str:
    with filepath.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.readline()
    if "|" in sample:
        return "|"
    if "\t" in sample:
        return "\t"
    return ","


def _find_csv_files(directory: Path) -> list[Path]:
    return sorted(directory.glob("*.csv"))


def load_tpcdi_data(table: str, base_dir: Path | None = None) -> list[dict[str, Any]]:
    base_dir = base_dir or TPCDI_RUNTIME_DIR
    possible_paths = [
        base_dir / f"{table}.csv",
        base_dir / "csv" / f"{table}.csv",
        base_dir / table / "*.csv",
    ]

    csv_files: list[Path] = []
    for pattern in possible_paths:
        if "*" in str(pattern):
            csv_files = sorted(base_dir.glob(str(pattern.relative_to(base_dir))))
            if csv_files:
                break
        elif pattern.exists():
            csv_files = [pattern]
            break

    if not csv_files:
        csv_files = _find_csv_files(base_dir)
        csv_files = [f for f in csv_files if table in f.name.lower()]

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV found for table '{table}' in {base_dir}. "
            f"Run: python -m cli.nexus generate tpcdi --scale-factor 1"
        )

    records: list[dict[str, Any]] = []
    for csv_file in csv_files:
        delimiter = _detect_delimiter(csv_file)
        with csv_file.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            for row in reader:
                typed_row: dict[str, Any] = {}
                for k, v in row.items():
                    if v is None or v == "":
                        typed_row[k] = None
                    else:
                        try:
                            typed_row[k] = int(v)
                        except ValueError:
                            try:
                                typed_row[k] = float(v)
                            except ValueError:
                                typed_row[k] = v
                records.append(typed_row)
    return records


def save_derived_source(
    records: list[dict[str, Any]],
    table: str,
    scenario_id: str,
    format: str = "csv",
    output_dir: Path | None = None,
) -> Path:
    output_dir = output_dir or DERIVED_DATA_DIR
    dir_path = output_dir / scenario_id / format
    dir_path.mkdir(parents=True, exist_ok=True)

    if format == "csv":
        filepath = dir_path / f"{table}.csv"
        if not records:
            filepath.write_text("", encoding="utf-8")
            return filepath
        fieldnames = list(records[0].keys())
        with filepath.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
    elif format == "jsonl":
        filepath = dir_path / f"{table}.jsonl"
        with filepath.open("w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, default=str) + "\n")
    elif format == "parquet":
        filepath = dir_path / f"{table}.parquet"
        records_as_csv = dir_path / f"_tmp_{table}.csv"
        save_derived_source(records, f"_tmp_{table}", scenario_id, "csv", output_dir)
        try:
            import pandas as pd
            df = pd.read_csv(records_as_csv)
            df.to_parquet(filepath, index=False)
        except ImportError:
            filepath = records_as_csv.rename(dir_path / f"{table}.csv")
            return filepath
        finally:
            records_as_csv.unlink(missing_ok=True)
    else:
        raise ValueError(f"Unsupported format: {format}")

    return filepath


def load_scenario_config(scenario_id: str) -> dict[str, Any]:
    config_path = SCENARIOS_DIR / f"{scenario_id}.yml"
    if not config_path.exists():
        raise FileNotFoundError(f"Scenario config not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_scorecard(scenario_id: str, scorecard: dict[str, Any]) -> Path:
    dir_path = REPORTS_DIR / scenario_id
    dir_path.mkdir(parents=True, exist_ok=True)
    filepath = dir_path / "scorecard.json"
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(scorecard, f, indent=2, default=str)
    return filepath


def save_injection_log(scenario_id: str, logs: list[dict[str, Any]]) -> Path:
    dir_path = REPORTS_DIR / scenario_id
    dir_path.mkdir(parents=True, exist_ok=True)
    filepath = dir_path / "injection_log.json"
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(logs, f, indent=2, default=str)
    return filepath
