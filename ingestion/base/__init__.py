"""
Base infrastructure for NEXUS ingestion framework.

Provides shared classes and utilities used by both batch and streaming pipelines:
- DownloadContext: Configuration holder
- SourceRun: Runtime state with checkpointing
- HTTP client with retry and rate limiting
- Validation and error handling
"""

from ingestion.base.core import DownloadContext, SourceFailure, SourceRun
from ingestion.base.http import request_json, download_file
from ingestion.base.utils import (
    load_config,
    resolve_mode,
    resolve_output_dir,
    run_id_now,
    sanitize_segment,
    extract_records,
)
from ingestion.base.contracts import (
    SourceSpec,
    RetryPolicy,
    TimeoutPolicy,
    RateLimitPolicy,
    CoveragePolicy,
    RunManifest,
)

__all__ = [
    # Core
    "DownloadContext",
    "SourceFailure",
    "SourceRun",
    # HTTP
    "request_json",
    "download_file",
    # Utils
    "load_config",
    "resolve_mode",
    "resolve_output_dir",
    "run_id_now",
    "sanitize_segment",
    "extract_records",
    # Contracts
    "SourceSpec",
    "RetryPolicy",
    "TimeoutPolicy",
    "RateLimitPolicy",
    "CoveragePolicy",
    "RunManifest",
]
