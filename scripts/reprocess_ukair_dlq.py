#!/usr/bin/env python3
"""
Reprocess failed UK Air CSV files from DLQ.

Extracts chunk_ids from DLQ, finds the original CSV files,
re-parses them with the fixed parser, and writes to raw envelopes.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from common.config import RAW_DIR, BRONZE_DIR
from ingestion.canonical.parser import iter_artifact_records
from ingestion.canonical.envelope import EnvelopeContext, build_raw_envelope
from ingestion.canonical.writer import write_raw_envelopes


def main():
    dlq_path = PROJECT_ROOT / "data" / "dlq" / "dlq.jsonl"
    
    if not dlq_path.exists():
        print(f"DLQ file not found: {dlq_path}")
        print("Run this script on the VM where the DLQ data exists.")
        sys.exit(1)
    
    # Extract UK Air failed chunks
    failed_files = []
    with dlq_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            event = json.loads(line)
            if event.get("source") != "ukair_air_quality_archive":
                continue
            if event.get("category") != "download_parser_failed":
                continue
            
            chunk_id = event.get("payload", {}).get("chunk_id", "")
            source_path = event.get("payload", {}).get("path", "")
            
            if source_path:
                failed_files.append({
                    "chunk_id": chunk_id,
                    "path": source_path,
                    "run_id": event.get("run_id", "unknown"),
                    "dataset": event.get("dataset", "ukair_air_quality_archive"),
                })
    
    print(f"Found {len(failed_files)} failed UK Air CSV files")
    
    if not failed_files:
        print("No files to reprocess")
        return
    
    # Group by run_id
    runs: dict[str, list] = {}
    for f in failed_files:
        run_id = f["run_id"]
        if run_id not in runs:
            runs[run_id] = []
        runs[run_id].append(f)
    
    total_records = 0
    total_errors = 0
    
    for run_id, files in runs.items():
        print(f"\n=== Processing run_id={run_id} ({len(files)} files) ===")
        
        for file_info in files:
            csv_path = Path(file_info["path"])
            
            if not csv_path.exists():
                print(f"  SKIP (not found): {csv_path}")
                continue
            
            try:
                records = list(iter_artifact_records(csv_path))
                print(f"  OK: {csv_path.name} -> {len(records)} records")
                total_records += len(records)
            except Exception as e:
                print(f"  ERROR: {csv_path.name} -> {e}")
                total_errors += 1
    
    print(f"\n=== Summary ===")
    print(f"Total records parsed: {total_records}")
    print(f"Files with errors: {total_errors}")


if __name__ == "__main__":
    main()
