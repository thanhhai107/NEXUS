"""Backfill DAG for catching up on missing data.

Detects gaps in data processing by querying Iceberg tables and
re-downloads missing data for the detected time ranges.

Usage:
    - Trigger manually: airflow dags trigger nexus_backfill
    - Trigger via API: POST /api/dags/nexus_backfill/dagRuns
    - Auto-trigger from polling DAG when gap detected

DAG Tasks:
    1. detect_gaps() - Query all sources for gaps
    2. create_backfill_windows() - Calculate time windows for backfill
    3. execute_backfill() - Run downloader for each window
    4. verify_backfill() - Confirm all data backfilled
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.decorators import task

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[3]
NEXUS_REPO_PATH = os.getenv("NEXUS_REPO_PATH", str(PROJECT_ROOT))

# Import config helpers
import sys
sys.path.insert(0, str(PROJECT_ROOT))

from common.config import (
    load_polling_config,
    load_backfill_config,
)


# Default args
default_args = {
    "owner": "nexus",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "tags": ["nexus", "backfill"],
}


def get_sources_to_backfill() -> list[str]:
    """Get list of sources that should be checked for backfill."""
    polling_config = load_polling_config()
    sources = []

    for source, config in polling_config.items():
        if not isinstance(config, dict):
            continue

        if config.get("enabled") and config.get("backfill_on_restart"):
            sources.append(source)

    return sources


@task
def detect_gaps() -> list[dict[str, Any]]:
    """Detect gaps for all polling sources.

    Returns:
        List of gap info dicts for sources that need backfill
    """
    from processing.common.watermark import WatermarkTracker

    tracker = WatermarkTracker(use_iceberg=True)
    backfill_config = load_backfill_config()
    gap_threshold_minutes = backfill_config.get("gap_threshold_minutes", 30)

    gaps = []
    sources = get_sources_to_backfill()

    for source in sources:
        gap_info = tracker.get_gap_info(source)

        if gap_info.get("needs_backfill"):
            gaps.append({
                "source": source,
                "last_processed": gap_info["last_processed"].isoformat() if gap_info["last_processed"] else None,
                "gap_minutes": gap_info["gap_minutes"],
                "threshold_minutes": gap_threshold_minutes,
            })

    print(f"Detected {len(gaps)} sources needing backfill: {[g['source'] for g in gaps]}")
    return gaps


@task
def create_backfill_windows(gaps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Create time windows for backfill based on gaps.

    Args:
        gaps: List of gap info from detect_gaps()

    Returns:
        List of backfill windows with start/end times
    """
    backfill_config = load_backfill_config()
    max_range_days = backfill_config.get("max_backfill_range_days", 7)
    concurrency = backfill_config.get("concurrency", 2)

    windows = []
    now = datetime.now(timezone.utc)

    for gap in gaps:
        source = gap["source"]
        last_processed = gap.get("last_processed")

        if not last_processed:
            # No watermark, skip
            continue

        last_processed_dt = datetime.fromisoformat(last_processed)
        if last_processed_dt.tzinfo is None:
            last_processed_dt = last_processed_dt.replace(tzinfo=timezone.utc)

        gap_minutes = gap.get("gap_minutes", 0)

        # Cap the backfill range
        if gap_minutes > max_range_days * 24 * 60:
            gap_minutes = max_range_days * 24 * 60

        end_time = now
        start_time = last_processed_dt

        window = {
            "source": source,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "gap_minutes": gap_minutes,
            "concurrency_slot": len(windows) % concurrency,
        }
        windows.append(window)

    print(f"Created {len(windows)} backfill windows")
    return windows


@task
def execute_backfill(windows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Execute backfill for each window.

    Runs the downloader with date range parameters for each source.

    Args:
        windows: List of backfill windows

    Returns:
        List of execution results
    """
    results = []

    for window in windows:
        source = window["source"]
        start_time = window["start_time"]
        end_time = window["end_time"]

        # Build command
        cmd = (
            f"cd {NEXUS_REPO_PATH} && "
            f"python ingestion/downloaders/london_downloader.py "
            f"--source {source} "
            f"--mode backfill "
            f"--start-time {start_time} "
            f"--end-time {end_time} "
            f"--run-id backfill_{{ ts_nodash }}"
        )

        result = {
            "source": source,
            "start_time": start_time,
            "end_time": end_time,
            "command": cmd,
            "status": "pending",
        }

        try:
            import subprocess
            proc = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour max
            )

            if proc.returncode == 0:
                result["status"] = "success"
                result["output"] = proc.stdout
            else:
                result["status"] = "failed"
                result["error"] = proc.stderr

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)

        results.append(result)
        print(f"Backfill for {source}: {result['status']}")

    return results


@task
def verify_backfill(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Verify backfill completion.

    Args:
        results: List of execution results

    Returns:
        Summary of backfill results
    """
    total = len(results)
    success = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] in ("failed", "error"))

    summary = {
        "total_windows": total,
        "successful": success,
        "failed": failed,
        "sources": [r["source"] for r in results],
        "failed_sources": [r["source"] for r in results if r["status"] in ("failed", "error")],
    }

    print(f"Backfill complete: {success}/{total} successful")

    if failed > 0:
        print(f"Failed sources: {summary['failed_sources']}")

    return summary


# DAG definition
with DAG(
    dag_id="nexus_backfill",
    description="Backfill missing data for polling sources",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nexus", "backfill"],
) as backfill_dag:

    gaps = detect_gaps()
    windows = create_backfill_windows(gaps)
    results = execute_backfill(windows)
    summary = verify_backfill(results)


# Alternative: Per-source backfill DAGs
def create_source_backfill_dag(source: str) -> DAG:
    """Create a dedicated backfill DAG for a specific source."""

    default_args_source = {
        **default_args,
        "start_date": datetime(2025, 1, 1),
    }

    dag_id = f"nexus_backfill_{source}"

    with DAG(
        dag_id=dag_id,
        description=f"Backfill for {source}",
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
        default_args=default_args_source,
        tags=["nexus", "backfill", source],
    ) as dag:

        @task
        def detect_gap_for_source(source: str = source) -> dict[str, Any] | None:
            from processing.common.watermark import WatermarkTracker

            tracker = WatermarkTracker(use_iceberg=True)
            gap_info = tracker.get_gap_info(source)

            if gap_info.get("needs_backfill"):
                return {
                    "source": source,
                    "start_time": gap_info["last_processed"].isoformat(),
                    "end_time": datetime.now(timezone.utc).isoformat(),
                    "gap_minutes": gap_info["gap_minutes"],
                }
            return None

        @task
        def run_backfill_for_source(gap: dict[str, Any] | None) -> dict[str, str]:
            if not gap:
                return {"source": source, "status": "skipped", "reason": "No gap detected"}

            cmd = (
                f"cd {NEXUS_REPO_PATH} && "
                f"python ingestion/downloaders/london_downloader.py "
                f"--source {source} "
                f"--mode backfill "
                f"--start-time {gap['start_time']} "
                f"--end-time {gap['end_time']} "
                f"--run-id backfill_{{ ts_nodash }}"
            )

            import subprocess
            proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=3600)

            return {
                "source": source,
                "status": "success" if proc.returncode == 0 else "failed",
                "output": proc.stdout if proc.returncode == 0 else proc.stderr,
            }

        gap = detect_gap_for_source()
        run_backfill_for_source(gap)

    return dag


# Generate per-source backfill DAGs
# These can be triggered individually
source_dags = {}
for source in get_sources_to_backfill():
    source_dag = create_source_backfill_dag(source)
    source_dags[f"nexus_backfill_{source}"] = source_dag

globals().update(source_dags)
