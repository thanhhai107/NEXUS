"""Column-level Lineage Module.

Tracks column-level lineage relationships between datasets.
Enables impact analysis when schema changes occur.

Usage:
    # Record column lineage
    from governance.column import record_column_lineage
    record_column_lineage(
        dataset="silver.bus_clean",
        columns=["scheduled_arrival", "actual_arrival"],
        upstream_dataset="bronze.tfl_bus",
        upstream_columns=["arrival_time"],
    )

    # Get column dependencies
    from governance.column import get_column_dependencies
    deps = get_column_dependencies("bronze.tfl_bus", "arrival_time")

    # Analyze impact
    from governance.column import analyze_impact
    report = analyze_impact(
        dataset="bronze.tfl_bus",
        changed_column="arrival_time",
        change_type="dropped",
    )
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common.config import LOGS_DIR


DEFAULT_COLUMN_LINEAGE_LOG = LOGS_DIR / "column_lineage.jsonl"


@dataclass
class ColumnLineage:
    """Represents a column-level lineage relationship."""
    dataset: str
    columns: list[str]
    upstream_dataset: str | None = None
    upstream_columns: list[str] | None = None
    job_name: str | None = None
    run_id: str | None = None
    recorded_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "columns": self.columns,
            "upstream_dataset": self.upstream_dataset,
            "upstream_columns": self.upstream_columns,
            "job_name": self.job_name,
            "run_id": self.run_id,
            "recorded_at": self.recorded_at,
        }


def record_column_lineage(
    dataset: str,
    columns: list[str],
    upstream_dataset: str | None = None,
    upstream_columns: list[str] | None = None,
    job_name: str | None = None,
    run_id: str | None = None,
    lineage_log: Path = DEFAULT_COLUMN_LINEAGE_LOG,
) -> Path:
    """Record column-level lineage relationship.

    Args:
        dataset: Output dataset name
        columns: List of output columns
        upstream_dataset: Input dataset name
        upstream_columns: List of input columns that feed into output columns
        job_name: Name of the transformation job
        run_id: Run identifier
        lineage_log: Path to lineage log file

    Returns:
        Path to lineage log file
    """
    lineage = ColumnLineage(
        dataset=dataset,
        columns=columns,
        upstream_dataset=upstream_dataset,
        upstream_columns=upstream_columns,
        job_name=job_name,
        run_id=run_id,
    )

    lineage_log.parent.mkdir(parents=True, exist_ok=True)

    with lineage_log.open("a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(lineage.to_dict(), ensure_ascii=False) + "\n")

    return lineage_log


def get_column_dependencies(
    dataset: str,
    column: str | None = None,
    lineage_log: Path = DEFAULT_COLUMN_LINEAGE_LOG,
) -> list[dict[str, Any]]:
    """Get all column dependencies for a dataset or column.

    Args:
        dataset: Dataset name to query
        column: Optional column name to filter by
        lineage_log: Path to lineage log file

    Returns:
        List of column lineage records that reference this dataset/column
    """
    if not lineage_log.exists():
        return []

    dependencies = []

    with lineage_log.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            record = json.loads(line)

            # Check if this record is upstream of the query dataset
            if record.get("dataset") == dataset:
                if column is None or column in record.get("columns", []):
                    dependencies.append(record)
                continue

            # Check if this record references the query dataset as upstream
            if record.get("upstream_dataset") == dataset:
                if column is None or column in (record.get("upstream_columns") or []):
                    dependencies.append(record)

    return dependencies


def get_downstream_dependencies(
    dataset: str,
    lineage_log: Path = DEFAULT_COLUMN_LINEAGE_LOG,
) -> list[dict[str, Any]]:
    """Get all downstream datasets that depend on this dataset.

    Args:
        dataset: Dataset name to query
        lineage_log: Path to lineage log file

    Returns:
        List of column lineage records where this dataset is upstream
    """
    if not lineage_log.exists():
        return []

    downstream = []

    with lineage_log.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            record = json.loads(line)

            if record.get("upstream_dataset") == dataset:
                downstream.append(record)

    return downstream
