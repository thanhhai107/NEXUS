"""Coverage Calculation for Orchestration.

Calculates coverage metrics for pipeline runs.
Extracted from ingestion/base/core.py for orchestration use.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from orchestration.airflow.config import get_source_coverage_policy
from orchestration.shared.manifest import (
    ChunkResult,
    RunManifest,
    CoverageStatus,
    read_manifest,
)


@dataclass
class CoverageResult:
    """Result of coverage calculation."""
    expected: int
    successful: int
    failed: int
    skipped: int
    covered: int  # successful + skipped
    ratio: float
    status: str
    missing_required: list[str] = None
    
    def __post_init__(self):
        if self.missing_required is None:
            self.missing_required = []


def calculate_coverage(
    run_id: str,
    dataset: str,
    source: str | None = None,
) -> CoverageResult:
    """Calculate coverage for a run.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
        source: Optional source name for policy lookup
    
    Returns:
        CoverageResult with metrics
    """
    manifest = read_manifest(run_id, dataset)
    
    if manifest is None:
        return CoverageResult(
            expected=0,
            successful=0,
            failed=0,
            skipped=0,
            covered=0,
            ratio=0.0,
            status=CoverageStatus.FAILED,
        )
    
    expected = manifest.expected_chunks
    successful = manifest.successful_chunks
    failed = manifest.failed_chunks
    skipped = manifest.skipped_chunks
    covered = successful + skipped
    ratio = round(covered / expected, 4) if expected > 0 else 0.0
    
    # Check for missing required chunks
    policy = get_source_coverage_policy(source or dataset) if source else {
        "min_success_ratio": 1.0,
        "allow_publish_with_warnings": False,
        "required_chunks": [],
    }
    
    required_chunks = policy.get("required_chunks", [])
    missing_required = []
    
    if required_chunks:
        observed_ids = {c.chunk_id for c in manifest.chunks}
        for chunk_id in required_chunks:
            if chunk_id not in observed_ids:
                missing_required.append(chunk_id)
    
    # Determine status
    if missing_required or any(
        c.status == "failed" and c.required 
        for c in manifest.chunks
    ):
        status = CoverageStatus.FAILED
    elif ratio >= policy.get("min_success_ratio", 1.0):
        status = CoverageStatus.COMPLETE
    elif ratio > 0 and policy.get("allow_publish_with_warnings", False):
        status = CoverageStatus.PARTIAL
    else:
        status = CoverageStatus.FAILED
    
    return CoverageResult(
        expected=expected,
        successful=successful,
        failed=failed,
        skipped=skipped,
        covered=covered,
        ratio=ratio,
        status=status,
        missing_required=missing_required,
    )


def should_publish(
    run_id: str,
    dataset: str,
    source: str | None = None,
) -> tuple[bool, str]:
    """Check if a run should be published based on coverage.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
        source: Optional source name for policy lookup
    
    Returns:
        Tuple of (should_publish, reason)
    """
    coverage = calculate_coverage(run_id, dataset, source)
    policy = get_source_coverage_policy(source or dataset) if source else {}
    
    if coverage.status == CoverageStatus.FAILED:
        return False, f"Coverage failed: missing {len(coverage.missing_required)} required chunks"
    
    if coverage.status == CoverageStatus.COMPLETE:
        return True, "Coverage complete"
    
    if coverage.status == CoverageStatus.PARTIAL:
        if policy.get("allow_publish_with_warnings", False):
            return True, "Publishing with warnings"
        return False, "Partial coverage but warnings not allowed"
    
    return False, "Unknown coverage status"


def get_coverage_summary(
    run_id: str,
    dataset: str,
    source: str | None = None,
) -> dict[str, Any]:
    """Get a summary dict of coverage for a run.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
        source: Optional source name
    
    Returns:
        Dict with coverage summary
    """
    coverage = calculate_coverage(run_id, dataset, source)
    
    return {
        "run_id": run_id,
        "dataset": dataset,
        "expected_chunks": coverage.expected,
        "successful_chunks": coverage.successful,
        "failed_chunks": coverage.failed,
        "skipped_chunks": coverage.skipped,
        "coverage_ratio": coverage.ratio,
        "coverage_status": coverage.status,
        "missing_required": coverage.missing_required,
    }
