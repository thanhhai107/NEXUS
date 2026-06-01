"""I/O utilities for loading TPC-DI benchmark data."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TPCDI_RUNTIME_DIR = PROJECT_ROOT / "runtime" / "datasets" / "tpcdi"
BENCHMARK_DIR = PROJECT_ROOT / "benchmark"
REPORTS_DIR = BENCHMARK_DIR / "reports"


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
