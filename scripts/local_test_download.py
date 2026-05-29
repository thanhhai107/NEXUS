"""Local test script to download small samples from all sources.

Usage:
    python scripts/local_test_download.py

This will:
1. Create a temp directory for downloads
2. Download small samples from each source
3. Save raw files for inspection
4. Generate a summary report
"""
from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.downloaders.london_downloader import SOURCE_REGISTRY


def get_source_config(source_key: str) -> dict:
    """Return minimal config for local testing."""
    base_config = {
        "core_start": "2025-01-01",
        "core_end": "2025-01-02",
        "transport_start": "2025-01-01",
        "transport_end": "2025-01-02",
        "transport_start_year": 2025,
        "transport_end_year": 2025,
    }
    
    # Source-specific configs
    configs = {
        "openmeteo": {
            "openmeteo": {
                "air_quality_url": "https://air-quality-api.open-meteo.com/v1/air-quality",
                "air_quality_hourly": ["pm10", "pm2_5", "no2", "o3"],
                "weather_url": "https://api.open-meteo.com/v1/forecast",
                "weather_hourly": ["temperature_2m", "relative_humidity_2m"],
                "timezone": "Europe/London",
            },
            "spatial_scope": {
                "boroughs": [
                    {"name": "Westminster", "latitude": 51.4975, "longitude": -0.1357},
                ]
            },
        },
        "openmeteo_historical_weather": {
            "openmeteo_historical_weather": {
                "archive_url": "https://archive-api.open-meteo.com/v1/archive",
                "grid_spacing_km": 10,
                "max_locations_per_request": 10,
                "timezone": "GMT",
                "format": "csv",
                "hourly": ["temperature_2m"],
                "daily": [],
            },
            "spatial_scope": {
                "bbox": {
                    "south": 51.28,
                    "north": 51.29,
                    "west": -0.51,
                    "east": -0.50,
                }
            },
        },
        "londonair": {
            "londonair": {
                "base_url": "https://api.erg.ic.ac.uk/AirQuality",
                "londonair_species": ["NO2", "PM25", "PM10"],
                "londonair_site_limit": 3,
            },
            "spatial_scope": {
                "boroughs": [
                    {"name": "Westminster", "latitude": 51.4975, "longitude": -0.1357},
                ]
            },
        },
        "openaq": {
            "openaq": {
                "city": "London",
                "limit": 10,
            },
        },
        "waqi": {
            "waqi": {
                "stations": ["london"],
                "limit": 5,
            },
        },
        "openweather": {
            "openweather": {
                "cities": ["London"],
                "limit": 3,
            },
        },
        "ncei": {
            "ncei": {
                "station_ids": ["LONDON/GATWICK"],
                "limit": 5,
            },
        },
        "stats19": {
            "stats19": {
                "limit": 10,
            },
        },
        "naptan": {
            "naptan": {
                "atco_area_codes": ["490"],
                "limit": 20,
            },
        },
        "london_journeys": {
            "london_journeys": {
                "limit": 10,
            },
        },
        "dft": {
            "dft": {
                "limit": 10,
            },
        },
        "tfl": {
            "tfl": {
                "modes": ["tube", "bus"],
                "limit": 10,
            },
        },
        "tfl_line_status": {
            "tfl_line_status": {
                "modes": ["tube", "bus"],
            },
        },
        "tfl_arrivals": {
            "tfl_arrivals": {
                "stop_ids": ["490000255W", "490000254W"],
                "limit": 10,
            },
        },
        "ukair_air_quality_archive": {
            "ukair_air_quality_archive": {
                "limit": 5,
            },
        },
    }
    
    return {**base_config, **configs.get(source_key, {})}


def find_jsonl_files(output_dir: Path) -> list[Path]:
    """Find all JSONL files in output directory."""
    return [f for f in output_dir.rglob("*.jsonl") if "metadata" not in str(f)]


def find_csv_files(output_dir: Path) -> list[Path]:
    """Find all CSV files in output directory."""
    return [f for f in output_dir.rglob("*.csv") if "metadata" not in str(f)]


def count_jsonl_records(path: Path) -> int:
    """Count records in a JSONL file."""
    try:
        with path.open(encoding="utf-8") as f:
            return sum(1 for _ in f)
    except Exception:
        return 0


def count_csv_rows(path: Path) -> int:
    """Count rows in a CSV file."""
    try:
        with path.open(encoding="utf-8", newline="") as f:
            return sum(1 for _ in csv.DictReader(f))
    except Exception:
        return 0


def sample_jsonl_file(path: Path, max_lines: int = 5) -> list[dict]:
    """Get sample records from JSONL file."""
    records = []
    try:
        with path.open(encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= max_lines:
                    break
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    records.append({"_parse_error": True, "_raw": line[:100]})
    except Exception as e:
        return [{"_error": str(e)}]
    return records


def sample_csv_file(path: Path, max_rows: int = 5) -> list[dict]:
    """Get sample rows from CSV file."""
    records = []
    try:
        with path.open(encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= max_rows:
                    break
                records.append(dict(row))
    except Exception as e:
        return [{"_error": str(e)}]
    return records


def get_file_structure_summary(path: Path) -> dict:
    """Get summary of file structure."""
    stats = {
        "path": str(path.relative_to(path.parents[1])),
        "size_kb": round(path.stat().st_size / 1024, 2),
    }
    
    if path.suffix == ".jsonl":
        stats["record_count"] = count_jsonl_records(path)
        stats["sample"] = sample_jsonl_file(path, 2)
        if stats["sample"]:
            stats["columns"] = list(stats["sample"][0].keys()) if isinstance(stats["sample"][0], dict) else []
    elif path.suffix == ".csv":
        stats["row_count"] = count_csv_rows(path)
        stats["sample"] = sample_csv_file(path, 2)
        if stats["sample"]:
            stats["columns"] = list(stats["sample"][0].keys()) if isinstance(stats["sample"][0], dict) else []
    elif path.suffix == ".json":
        try:
            with path.open(encoding="utf-8") as f:
                data = json.load(f)
            stats["type"] = "json"
            if isinstance(data, dict):
                stats["keys"] = list(data.keys())[:10]
            elif isinstance(data, list):
                stats["length"] = len(data)
        except Exception as e:
            stats["_error"] = str(e)
    
    return stats


def download_source(source_key: str, output_dir: Path, config_override: dict | None = None) -> dict:
    """Download small sample from a source."""
    from unittest.mock import MagicMock, patch
    
    result = {
        "source_key": source_key,
        "success": False,
        "files": [],
        "error": None,
    }
    
    try:
        spec = SOURCE_REGISTRY[source_key]
        
        # Build context mock
        context = MagicMock()
        context.mode = get_source_config(source_key)
        context.config = context.mode.copy()
        context.spatial_scope = context.config.get("spatial_scope", {})
        context.run_id = f"local-test-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
        context.output_dir = output_dir / spec.source_id / f"run_id={context.run_id}"
        context.output_dir.mkdir(parents=True, exist_ok=True)
        context.resume = False
        context.poll_time = None
        
        # Create mock SourceRun
        run = MagicMock()
        run.source_id = spec.source_id
        run.source_key = spec.source_key
        run.dataset_name = spec.dataset_name
        run.run_id = context.run_id
        run.raw_dir = context.output_dir / "raw"
        run.metadata_dir = context.output_dir / "metadata"
        run.raw_dir.mkdir(parents=True, exist_ok=True)
        run.metadata_dir.mkdir(parents=True, exist_ok=True)
        
        # Track what gets written
        written_files = []
        original_write_json = run.write_json
        
        def mock_write_json(relative_path: str, data, *, record_count=None):
            file_path = run.raw_dir / relative_path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            written_files.append(str(relative_path))
            
            if file_path.suffix == ".json":
                with file_path.open("w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, default=str)
            elif file_path.suffix == ".jsonl":
                with file_path.open("w", encoding="utf-8") as f:
                    if isinstance(data, list):
                        for item in data:
                            f.write(json.dumps(item, default=str) + "\n")
                    else:
                        f.write(json.dumps(data, default=str) + "\n")
            elif file_path.suffix == ".csv":
                import csv as csv_module
                with file_path.open("w", encoding="utf-8", newline="") as f:
                    if isinstance(data, list) and data:
                        writer = csv_module.DictWriter(f, fieldnames=data[0].keys() if isinstance(data[0], dict) else [])
                        writer.writeheader()
                        writer.writerows(data)
                    elif isinstance(data, str):
                        f.write(data)
            else:
                with file_path.open("wb") as f:
                    if isinstance(data, bytes):
                        f.write(data)
                    else:
                        f.write(str(data).encode("utf-8"))
            
            return file_path
        
        run.write_json = mock_write_json
        run.mark_complete = MagicMock()
        run.mark_failed = MagicMock()
        run.mark_skipped = MagicMock()
        run.should_skip = MagicMock(return_value=False)
        run.finish = MagicMock(return_value={})
        run.log_request = MagicMock()
        run.failed_requests = []
        run._resolve_output_path = lambda p: run.raw_dir / p
        run._staging_path = lambda p: run.raw_dir / f"{p}.tmp"
        run._atomic_publish = lambda tmp, path: tmp.rename(path) if tmp.exists() else None
        run._record_output = MagicMock()
        
        # Run download
        print(f"  Testing {source_key}...")
        spec.func(run, context)
        
        result["success"] = True
        result["files"] = written_files
        result["output_dir"] = str(context.output_dir)
        
        print(f"    OK - {len(written_files)} files")
        
    except Exception as e:
        result["error"] = str(e)
        print(f"    FAILED: {e}")
    
    return result


def main() -> int:
    print("=" * 60)
    print("Local Data Source Download Test")
    print("=" * 60)
    
    # Create output directory
    output_dir = PROJECT_ROOT / "data" / "local_test"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results = []
    
    # Test each source
    sources_to_test = [
        "openmeteo",
        "londonair",
        # "openaq",  # Requires API key
        # "waqi",  # Requires API key
        # "openweather",  # Requires API key
        # "ncei",  # Requires API key
        # "stats19",  # Large download
        # "naptan",  # Large download
        # "london_journeys",  # Check if endpoint works
        # "dft",  # Check if endpoint works
        # "tfl",  # Check if endpoint works
        # "tfl_line_status",  # Check if endpoint works
        # "tfl_arrivals",  # Check if endpoint works
        # "ukair_air_quality_archive",  # Check if endpoint works
        "openmeteo_historical_weather",
    ]
    
    for source_key in sources_to_test:
        result = download_source(source_key, output_dir)
        results.append(result)
    
    # Generate summary report
    print("\n" + "=" * 60)
    print("Summary Report")
    print("=" * 60)
    
    report_path = output_dir / "download_summary.json"
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sources": results,
    }
    
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\nReport saved to: {report_path}")
    
    # List all downloaded files
    print("\nDownloaded files:")
    for source_dir in output_dir.iterdir():
        if source_dir.is_dir():
            print(f"\n  {source_dir.name}/")
            for file_path in source_dir.rglob("*"):
                if file_path.is_file():
                    rel_path = file_path.relative_to(source_dir)
                    size_kb = round(file_path.stat().st_size / 1024, 2)
                    print(f"    {rel_path} ({size_kb} KB)")
    
    # Success/failure summary
    success_count = sum(1 for r in results if r["success"])
    print(f"\nResults: {success_count}/{len(results)} sources successful")
    
    return 0 if success_count == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
