"""Tests for parsing downloaded raw data files.

This test module:
1. Downloads small samples from data sources
2. Parses raw downloaded files (JSONL, CSV, JSON)
3. Validates structure and shows sample records

Usage:
    python -m pytest tests/test_sample_datasets.py -v
    python scripts/test_parse_downloaded.py <data_dir>
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "local_test"


def parse_jsonl(path: Path) -> tuple[list[dict], list[str]]:
    """Parse JSONL file and return records and columns."""
    records = []
    columns = set()

    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                records.append(record)
                if isinstance(record, dict):
                    columns.update(record.keys())
            except json.JSONDecodeError:
                records.append({"_parse_error": True, "_raw": line[:100]})

    return records, sorted(columns)


def parse_csv(path: Path) -> tuple[list[dict], list[str]]:
    """Parse CSV file and return records and columns."""
    records = []
    columns = []

    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []
        for row in reader:
            records.append(dict(row))

    return records, columns


def parse_json(path: Path) -> tuple[list[dict] | dict, list[str]]:
    """Parse JSON file and return data and keys."""
    with path.open(encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        columns = []
        if data and isinstance(data[0], dict):
            columns = sorted(data[0].keys())
        return data, columns
    elif isinstance(data, dict):
        return data, sorted(data.keys())

    return data, []


def find_data_files(data_dir: Path) -> list[Path]:
    """Find all data files in directory (exclude metadata)."""
    if not data_dir.exists():
        return []

    data_files = []
    for pattern in ["*.jsonl", "*.csv", "*.json"]:
        data_files.extend(data_dir.rglob(pattern))

    return [f for f in data_files if "metadata" not in str(f)]


def analyze_file(path: Path) -> dict[str, Any]:
    """Analyze a single data file."""
    result = {
        "path": str(path.relative_to(path.parents[1])),
        "size_kb": round(path.stat().st_size / 1024, 2),
        "format": None,
        "record_count": 0,
        "columns": [],
        "sample": [],
        "error": None,
    }

    try:
        if path.suffix == ".jsonl":
            result["format"] = "jsonl"
            records, columns = parse_jsonl(path)
            result["record_count"] = len(records)
            result["columns"] = columns
            result["sample"] = records[:3]

        elif path.suffix == ".csv":
            result["format"] = "csv"
            records, columns = parse_csv(path)
            result["record_count"] = len(records)
            result["columns"] = columns
            result["sample"] = records[:3]

        elif path.suffix == ".json":
            result["format"] = "json"
            data, columns = parse_json(path)
            if isinstance(data, list):
                result["record_count"] = len(data)
                result["sample"] = data[:3]
            else:
                result["sample"] = [data]
            result["columns"] = columns

        else:
            result["format"] = "text"
            with path.open(encoding="utf-8") as f:
                content = f.read(500)
            result["sample"] = [{"_preview": content[:200]}]

    except Exception as e:
        result["error"] = str(e)

    return result


class TestSampleDatasets:
    """Test parsing of downloaded data files."""

    def test_data_directory_exists(self):
        """Data directory should exist after download."""
        assert DATA_DIR.exists(), (
            f"Data directory not found: {DATA_DIR}. "
            "Run: python scripts/test_download.py --source openmeteo --mode small_demo"
        )

    def test_has_data_files(self):
        """Data directory should contain downloaded files."""
        data_files = find_data_files(DATA_DIR)
        assert len(data_files) > 0, (
            f"No data files found in {DATA_DIR}. "
            "Run download script first."
        )

    def test_all_files_parseable(self):
        """All downloaded files should be parseable."""
        data_files = find_data_files(DATA_DIR)
        errors = []

        for file_path in data_files:
            analysis = analyze_file(file_path)
            if analysis["error"]:
                errors.append(f"{file_path.name}: {analysis['error']}")

        assert errors == [], f"Parse errors:\n" + "\n".join(errors)

    def test_sample_records_summary(self, capsys):
        """Print summary of all downloaded files."""
        data_files = find_data_files(DATA_DIR)

        print("\n" + "=" * 60)
        print("Downloaded Data Summary")
        print("=" * 60)

        total_records = 0
        for file_path in sorted(data_files):
            analysis = analyze_file(file_path)
            rel_path = file_path.relative_to(DATA_DIR)

            print(f"\n{rel_path}")
            print(f"  Size: {analysis['size_kb']} KB")
            print(f"  Format: {analysis['format']}")
            print(f"  Records: {analysis['record_count']}")
            total_records += analysis['record_count']

            if analysis["columns"]:
                print(f"  Columns ({len(analysis['columns'])}): {', '.join(analysis['columns'][:10])}")
                if len(analysis["columns"]) > 10:
                    print(f"    ... and {len(analysis['columns']) - 10} more")

            if analysis["sample"]:
                print("  Sample:")
                sample = analysis["sample"][0]
                if isinstance(sample, dict):
                    for key, value in list(sample.items())[:5]:
                        value_str = str(value)
                        if len(value_str) > 50:
                            value_str = value_str[:50] + "..."
                        print(f"    {key}: {value_str}")

        print(f"\n{'=' * 60}")
        print(f"Total: {len(data_files)} files, {total_records} records")


class TestSourceSpecificParsing:
    """Test parsing for specific sources."""

    def test_openmeteo_files(self):
        """Open-Meteo files should have expected structure."""
        openmeteo_files = list(DATA_DIR.glob("openmeteo*/**/*.jsonl"))

        if not openmeteo_files:
            pytest.skip("Open-Meteo files not downloaded")

        for file_path in openmeteo_files:
            records, columns = parse_jsonl(file_path)
            assert len(records) > 0, f"No records in {file_path}"
            assert columns, f"No columns in {file_path}"

            # Check for expected air quality fields
            first_record = records[0]
            if isinstance(first_record, dict):
                # Should have time series data
                has_time = any("time" in c.lower() for c in columns)
                has_measurement = any(
                    c in columns for c in ["pm10", "pm2_5", "no2", "o3", "temperature_2m"]
                )
                assert has_time or has_measurement, f"No expected fields in {file_path}"

    def test_londonair_files(self):
        """LondonAir files should have expected structure."""
        londonair_files = list(DATA_DIR.glob("londonair*/**/*.json"))

        if not londonair_files:
            pytest.skip("LondonAir files not downloaded")

        for file_path in londonair_files:
            data, columns = parse_json(file_path)

            if isinstance(data, list):
                assert len(data) > 0, f"No records in {file_path}"
                first_record = data[0]
                if isinstance(first_record, dict):
                    # Should have site/species data
                    has_code = any("code" in c.lower() for c in columns)
                    has_name = any("name" in c.lower() for c in columns)
                    assert has_code or has_name, f"No expected fields in {file_path}"
            elif isinstance(data, dict):
                assert columns, f"No keys in {file_path}"

    def test_csv_files_structure(self):
        """CSV files should have headers and data."""
        csv_files = list(DATA_DIR.glob("**/*.csv"))

        if not csv_files:
            pytest.skip("CSV files not downloaded")

        for file_path in csv_files:
            records, columns = parse_csv(file_path)
            assert columns, f"No headers in {file_path}"
            assert len(records) > 0, f"No data rows in {file_path}"


if __name__ == "__main__":
    # Run analysis when executed directly
    if len(sys.argv) > 1:
        data_dir = Path(sys.argv[1])
    else:
        data_dir = DATA_DIR

    if not data_dir.exists():
        print(f"Data directory not found: {data_dir}")
        print("Run: python scripts/test_download.py --source openmeteo --mode small_demo")
        sys.exit(1)

    print(f"\nAnalyzing: {data_dir}\n")

    data_files = find_data_files(data_dir)
    print(f"Found {len(data_files)} data files:\n")

    total_records = 0
    for file_path in sorted(data_files):
        analysis = analyze_file(file_path)
        rel_path = file_path.relative_to(data_dir)

        print(f"{rel_path}")
        print(f"  Size: {analysis['size_kb']} KB | Format: {analysis['format']} | Records: {analysis['record_count']}")

        if analysis["columns"]:
            print(f"  Columns ({len(analysis['columns'])}): {', '.join(analysis['columns'][:15])}")
            if len(analysis["columns"]) > 15:
                print(f"    ... and {len(analysis['columns']) - 15} more")

        if analysis["error"]:
            print(f"  ERROR: {analysis['error']}")

        total_records += analysis["record_count"]
        print()

    print(f"Total: {len(data_files)} files, {total_records} records")
