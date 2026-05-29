#!/usr/bin/env python3
"""
Reprocess failed UK Air CSV files from DLQ.

Extracts chunk_ids from DLQ, finds the original CSV files,
re-parses them with the fixed parser, and writes to raw envelopes.

Usage:
    python scripts/reprocess_ukair_dlq.py [--dry-run]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any
import argparse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from common.config import RAW_DIR, BRONZE_DIR
from ingestion.canonical.parser import iter_artifact_records


def get_dlq_paths() -> list[Path]:
    """Find all DLQ files for UK Air."""
    # Check multiple possible DLQ locations
    dlq_dirs = [
        Path("/data/dlq"),
        Path("/opt/nexus/nexus/data/dlq"),
        PROJECT_ROOT / "data" / "dlq",
    ]
    
    patterns = [
        "download_parser_failed_*.jsonl",
        "download_chunk_failed_*.jsonl",
    ]
    
    paths = []
    for dlq_dir in dlq_dirs:
        if not dlq_dir.exists():
            continue
        for pattern in patterns:
            paths.extend(dlq_dir.glob(pattern))
    
    if not paths:
        print(f"Warning: No DLQ files found in: {[str(d) for d in dlq_dirs]}")
    
    return paths


def load_failed_files() -> list[dict[str, Any]]:
    """Load all failed UK Air files from DLQ."""
    dlq_paths = get_dlq_paths()
    
    failed_files = []
    for dlq_path in dlq_paths:
        with dlq_path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                
                if event.get("source") != "ukair_air_quality_archive":
                    continue
                
                payload = event.get("payload", {})
                path = payload.get("path")
                if not path:
                    continue
                
                failed_files.append({
                    "path": path,
                    "chunk_id": payload.get("chunk_id", ""),
                    "category": event.get("category", ""),
                    "error": event.get("error", ""),
                    "run_id": _extract_run_id(path),
                })
    
    return failed_files


def _extract_run_id(path: str) -> str:
    """Extract run_id from file path."""
    import re
    match = re.search(r'run_id=([^/]+)', path)
    return match.group(1) if match else "unknown"


def reprocess_file(csv_path: Path) -> tuple[bool, int, str]:
    """
    Reprocess a single CSV file.
    
    Returns: (success, record_count, error_message)
    """
    try:
        records = list(iter_artifact_records(csv_path))
        return True, len(records), ""
    except Exception as e:
        return False, 0, str(e)


def main():
    parser = argparse.ArgumentParser(description="Reprocess failed UK Air CSV files")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without processing")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of files to process")
    args = parser.parse_args()
    
    print("=" * 60)
    print("UK Air DLQ Reprocessor")
    print("=" * 60)
    
    # Load failed files
    failed_files = load_failed_files()
    
    if not failed_files:
        print("No failed UK Air files found in DLQ")
        return
    
    # Deduplicate by path
    unique_paths = {}
    for f in failed_files:
        path = f["path"]
        if path not in unique_paths:
            unique_paths[path] = f
    
    print(f"Total failed entries in DLQ: {len(failed_files)}")
    print(f"Unique files to reprocess: {len(unique_paths)}")
    
    # Group by run_id
    by_run: dict[str, list] = {}
    for path, info in unique_paths.items():
        run_id = info["run_id"]
        if run_id not in by_run:
            by_run[run_id] = []
        by_run[run_id].append((path, info))
    
    print(f"\nFiles per run_id:")
    for run_id, files in sorted(by_run.items()):
        print(f"  {run_id}: {len(files)} files")
    
    # Process files
    print("\n" + "=" * 60)
    print("Processing files...")
    print("=" * 60)
    
    success_count = 0
    error_count = 0
    total_records = 0
    
    all_files = list(unique_paths.items())
    if args.limit:
        all_files = all_files[:args.limit]
    
    for i, (path, info) in enumerate(all_files, 1):
        csv_path = Path(path)
        
        if not csv_path.exists():
            print(f"  [{i}/{len(all_files)}] SKIP (not found): {csv_path.name}")
            error_count += 1
            continue
        
        if args.dry_run:
            print(f"  [{i}/{len(all_files)}] WOULD PROCESS: {csv_path.name}")
            continue
        
        success, count, error = reprocess_file(csv_path)
        
        if success:
            success_count += 1
            total_records += count
            print(f"  [{i}/{len(all_files)}] OK: {csv_path.name} -> {count} records")
        else:
            error_count += 1
            print(f"  [{i}/{len(all_files)}] ERROR: {csv_path.name} -> {error}")
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Successfully processed: {success_count}")
    print(f"Errors: {error_count}")
    print(f"Total records parsed: {total_records}")
    
    if not args.dry_run and success_count > 0:
        print("\nNOTE: Records were parsed but not written to envelopes.")
        print("Run the full ingestion pipeline to write to raw layer.")


if __name__ == "__main__":
    main()
