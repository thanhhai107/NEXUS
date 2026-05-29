"""Test parsing downloaded raw data files.

This script:
1. Lists all raw data files in a directory
2. Parses JSONL/CSV/JSON files
3. Shows sample records and columns
4. Validates structure

Usage:
    python scripts/test_parse_downloaded.py <data_dir>
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any


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
            except json.JSONDecodeError as e:
                print(f"  WARNING: JSON parse error at line {len(records)+1}: {e}")
                records.append({"_parse_error": str(e), "_raw": line[:100]})
    
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


def analyze_file(path: Path) -> dict[str, Any]:
    """Analyze a single data file."""
    result = {
        "path": str(path),
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
            with path.open(encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                result["record_count"] = len(data)
                result["sample"] = data[:3]
                if data and isinstance(data[0], dict):
                    result["columns"] = sorted(data[0].keys())
            elif isinstance(data, dict):
                result["columns"] = sorted(data.keys())
                result["sample"] = [data]
                
        else:
            # Try to read as text
            result["format"] = "text"
            with path.open(encoding="utf-8") as f:
                content = f.read(500)
            result["sample"] = [{"_preview": content[:200]}]
            
    except Exception as e:
        result["error"] = str(e)
    
    return result


def analyze_directory(data_dir: Path) -> dict[str, Any]:
    """Analyze all data files in a directory."""
    if not data_dir.exists():
        return {"error": f"Directory does not exist: {data_dir}"}
    
    results = {
        "directory": str(data_dir),
        "files": [],
        "summary": {
            "total_files": 0,
            "total_records": 0,
            "formats": {},
        },
    }
    
    # Find all data files (exclude metadata)
    data_files = []
    for pattern in ["*.jsonl", "*.csv", "*.json"]:
        data_files.extend(data_dir.rglob(pattern))
    
    # Filter out metadata files
    data_files = [f for f in data_files if "metadata" not in str(f)]
    
    print(f"\nAnalyzing {len(data_files)} data files in: {data_dir}\n")
    print("-" * 60)
    
    for file_path in sorted(data_files):
        rel_path = file_path.relative_to(data_dir)
        print(f"\nFile: {rel_path}")
        print("-" * 40)
        
        analysis = analyze_file(file_path)
        results["files"].append(analysis)
        results["summary"]["total_records"] += analysis["record_count"]
        results["summary"]["formats"][analysis["format"]] = \
            results["summary"]["formats"].get(analysis["format"], 0) + 1
        
        # Print summary
        print(f"  Size: {analysis['size_kb']} KB")
        print(f"  Format: {analysis['format']}")
        print(f"  Records: {analysis['record_count']}")
        
        if analysis["columns"]:
            print(f"  Columns ({len(analysis['columns'])}): {', '.join(analysis['columns'][:10])}")
            if len(analysis["columns"]) > 10:
                print(f"    ... and {len(analysis['columns']) - 10} more")
        
        if analysis["error"]:
            print(f"  ERROR: {analysis['error']}")
        elif analysis["sample"]:
            print(f"  Sample record:")
            sample = analysis["sample"][0]
            if isinstance(sample, dict):
                for key, value in list(sample.items())[:5]:
                    print(f"    {key}: {str(value)[:50]}")
    
    results["summary"]["total_files"] = len(results["files"])
    
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total files: {results['summary']['total_files']}")
    print(f"Total records: {results['summary']['total_records']}")
    print(f"Formats: {results['summary']['formats']}")
    
    return results


def main() -> int:
    if len(sys.argv) < 2:
        # Default to local_test directory
        data_dir = PROJECT_ROOT / "data" / "local_test"
        if not data_dir.exists():
            data_dir = Path("data/local_test")
    else:
        data_dir = Path(sys.argv[1])
    
    print(f"Analyzing data directory: {data_dir}")
    
    results = analyze_directory(data_dir)
    
    # Save results
    output_path = data_dir / "analysis_results.json"
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nResults saved to: {output_path}")
    
    return 0


if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    sys.exit(main())
