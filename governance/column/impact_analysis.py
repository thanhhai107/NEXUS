"""Impact Analysis Module.

Analyzes the impact of schema changes on downstream dependencies.
Uses column lineage to find affected datasets, views, and dashboards.

Usage:
    from governance.column import analyze_impact

    # Analyze impact of dropping a column
    report = analyze_impact(
        dataset="bronze.tpcds_store_sales",
        changed_column="arrival_time",
        change_type="dropped",
    )

    # Print affected datasets
    print(report)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from governance.column.column_lineage import (
    DEFAULT_COLUMN_LINEAGE_LOG,
    get_column_dependencies,
    get_downstream_dependencies,
)


@dataclass
class ImpactReport:
    """Report of impact analysis for a schema change."""
    dataset: str
    changed_column: str
    change_type: str
    affected_tables: list[str] = field(default_factory=list)
    affected_views: list[str] = field(default_factory=list)
    affected_dashboards: list[str] = field(default_factory=list)
    affected_jobs: list[str] = field(default_factory=list)
    severity: str = "medium"
    total_affected: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "changed_column": self.changed_column,
            "change_type": self.change_type,
            "severity": self.severity,
            "total_affected": self.total_affected,
            "affected_tables": self.affected_tables,
            "affected_views": self.affected_views,
            "affected_dashboards": self.affected_dashboards,
            "affected_jobs": self.affected_jobs,
        }

    def __str__(self) -> str:
        lines = [
            f"Impact Report: {self.dataset}.{self.changed_column}",
            f"Change Type: {self.change_type}",
            f"Severity: {self.severity}",
            f"Total Affected: {self.total_affected}",
            "",
        ]
        if self.affected_tables:
            lines.append(f"Tables ({len(self.affected_tables)}):")
            for t in self.affected_tables:
                lines.append(f"  - {t}")
            lines.append("")
        if self.affected_views:
            lines.append(f"Views ({len(self.affected_views)}):")
            for v in self.affected_views:
                lines.append(f"  - {v}")
            lines.append("")
        if self.affected_dashboards:
            lines.append(f"Dashboards ({len(self.affected_dashboards)}):")
            for d in self.affected_dashboards:
                lines.append(f"  - {d}")
            lines.append("")
        if self.affected_jobs:
            lines.append(f"Jobs ({len(self.affected_jobs)}):")
            for j in self.affected_jobs:
                lines.append(f"  - {j}")
        return "\n".join(lines)


def analyze_impact(
    dataset: str,
    changed_column: str,
    change_type: str = "dropped",
    lineage_log: str | None = None,
) -> ImpactReport:
    """Analyze downstream impact of a column change.

    Args:
        dataset: Dataset name where change occurred
        changed_column: Column that changed
        change_type: Type of change (dropped, type_changed, renamed)
        lineage_log: Optional path to lineage log

    Returns:
        ImpactReport with affected dependencies
    """
    log_path = lineage_log or str(DEFAULT_COLUMN_LINEAGE_LOG)

    report = ImpactReport(
        dataset=dataset,
        changed_column=changed_column,
        change_type=change_type,
    )

    # Find all downstream dependencies
    downstream = get_downstream_dependencies(dataset, log_path)

    # Check if the changed column is used in any downstream
    for dep in downstream:
        dep_dataset = dep.get("dataset", "")
        upstream_cols = dep.get("upstream_columns") or []

        # If the changed column feeds into downstream columns
        if changed_column in upstream_cols:
            _classify_affected(report, dep_dataset, dep)

    # Also check direct column dependencies
    column_deps = get_column_dependencies(dataset, changed_column, log_path)
    for dep in column_deps:
        dep_dataset = dep.get("dataset", "")
        if dep_dataset and dep_dataset not in report.affected_tables:
            _classify_affected(report, dep_dataset, dep)

    # Calculate severity based on impact
    report.total_affected = (
        len(report.affected_tables)
        + len(report.affected_views)
        + len(report.affected_dashboards)
        + len(report.affected_jobs)
    )

    if report.total_affected >= 10:
        report.severity = "critical"
    elif report.total_affected >= 5:
        report.severity = "high"
    elif report.total_affected >= 1:
        report.severity = "medium"
    else:
        report.severity = "low"

    # Higher severity for dropped columns
    if change_type == "dropped" and report.severity != "low":
        if report.severity == "medium":
            report.severity = "high"

    return report


def _classify_affected(
    report: ImpactReport,
    dep_dataset: str,
    dep_record: dict[str, Any],
) -> None:
    """Classify a dependency into the appropriate category."""
    job_name = dep_record.get("job_name", "")

    # Classify based on dataset/job naming conventions
    if dep_dataset.startswith("gold."):
        report.affected_tables.append(dep_dataset)
    elif dep_dataset.startswith("silver."):
        report.affected_tables.append(dep_dataset)
    elif dep_dataset.startswith("views."):
        report.affected_views.append(dep_dataset)
    elif "_view" in dep_dataset.lower():
        report.affected_views.append(dep_dataset)
    elif "_dashboard" in dep_dataset.lower() or "_chart" in dep_dataset.lower():
        report.affected_dashboards.append(dep_dataset)
    elif job_name:
        report.affected_jobs.append(job_name)
    else:
        report.affected_tables.append(dep_dataset)
