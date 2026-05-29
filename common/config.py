"""
NEXUS Configuration Module.

Unified config cho cả local (runtime/) và VM (/data/).
Cấu trúc thư mục theo Medallion Architecture:
    runtime/           (local)
        /data/        (VM — AWS EC2 / GCP Compute Engine)
            ├── lake/           Bronze → Silver → Gold
            ├── pipeline/       Orchestration
            ├── warehouse/      Query engines
            ├── staging/        Workspace tạm
            ├── dlq/            Dead Letter Queue
            └── quarantine/      Invalid records
"""

from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "config"
DOMAINS_DIR = PROJECT_ROOT / "domains"

try:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env", override=False)
except ImportError:
    pass


def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


# =============================================================================
# RUNTIME DIRECTORY RESOLUTION
# =============================================================================
# Resolution order:
#   1. NEXUS_RUNTIME_DIR environment variable (highest priority)
#   2. NEXUS_RUNTIME_MODE + default paths:
#      - "local": PROJECT_ROOT / "runtime"
#      - "vm": "/data" (AWS EC2 or GCP VM)
#   3. PROJECT_ROOT / "runtime" (default fallback)

def _resolve_runtime_dir() -> Path:
    # Check environment variable first
    env_runtime = os.getenv("NEXUS_RUNTIME_DIR")
    if env_runtime:
        return Path(env_runtime).resolve()

    # Check runtime mode
    runtime_mode = os.getenv("NEXUS_RUNTIME_MODE", "local").lower()
    if runtime_mode == "vm":
        return Path("/data").resolve()
    
    # Default: PROJECT_ROOT / "runtime"
    return PROJECT_ROOT / "runtime"


RUNTIME_DIR = _resolve_runtime_dir()


def get_runtime_mode() -> str:
    """Get the current runtime mode.
    
    Returns:
        "local" or "vm" depending on NEXUS_RUNTIME_MODE setting
    """
    return os.getenv("NEXUS_RUNTIME_MODE", "local").lower()


def is_vm_mode() -> bool:
    """Check if running in VM mode (using /data/).
    
    Returns:
        True if NEXUS_RUNTIME_MODE=vm or NEXUS_FORCE_VM=true
        (NEXUS_FORCE_GCP is still accepted for backward compatibility)
    """
    if os.getenv("NEXUS_FORCE_VM") or os.getenv("NEXUS_FORCE_GCP"):
        return True
    return get_runtime_mode() == "vm"


# ============================================================================
# LAKE STRUCTURE (Medallion Architecture)
# ============================================================================
# Data lake với 3 tier: Bronze → Silver → Gold
#
# Directory structure:
#   lake/bronze/{dataset}/run_id={run_id}/
#       ├── metadata/       # Checkpoint, profile, request log
#       ├── published/      # Published manifest
#       ├── raw/            # Downloaded raw files
#       └── staging/        # Temporary files during download
#
#   lake/silver/{dataset}/                   # Validated, standardized data
#   lake/gold/{dataset}/                     # Business aggregates
#   lake/schemas/{dataset}.schema.json

LAKE_DIR = RUNTIME_DIR / "lake"
BRONZE_DIR = LAKE_DIR / "bronze"  # Raw data gốc (source format)
SILVER_DIR = LAKE_DIR / "silver"  # Validated + standardized data
GOLD_DIR = LAKE_DIR / "gold"      # Business aggregates (optional)
SCHEMAS_DIR = LAKE_DIR / "schemas"  # JSON schemas cho từng dataset


# ============================================================================
# RUNTIME WORKSPACE (Temporary)
# ============================================================================

STAGING_DIR = RUNTIME_DIR / "staging"  # Files đang download
TMP_DIR = RUNTIME_DIR / "tmp"          # Temp files
LOGS_DIR = RUNTIME_DIR / "logs"       # Application logs
METRICS_DIR = RUNTIME_DIR / "metrics"  # Prometheus metrics


# ============================================================================
# ERROR HANDLING
# ============================================================================

DLQ_DIR = RUNTIME_DIR / "dlq"          # Dead Letter Queue
QUARANTINE_DIR = RUNTIME_DIR / "quarantine"  # Invalid records


# ============================================================================
# RAW ENVELOPE LANDING ZONE
# ============================================================================
# Canonical raw JSONL envelopes live outside the lake tiers. Raw source artifacts
# remain under BRONZE_DIR/{dataset}/run_id={run_id}/raw until they are adapted
# into this shared landing zone. Spark raw→Bronze jobs consume RAW_DIR.

RAW_DIR = RUNTIME_DIR / "raw"


# ============================================================================
# LEGACY ALIASES (Backward Compatibility - Deprecated)
# ============================================================================
# Will be removed after migration

DATASETS_DIR = BRONZE_DIR  # Legacy alias - use BRONZE_DIR


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_runtime_path(*parts: str) -> Path:
    """Get path within runtime directory, creating it if needed."""
    path = RUNTIME_DIR.joinpath(*parts)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_bronze_path(
    dataset: str,
    run_id: str,
    subdir: str = "raw",
) -> Path:
    """Get bronze path for a dataset run.
    
    Args:
        dataset: Dataset name (e.g., 'londonair_monitoring', 'tfl_arrivals')
        run_id: Run ID (e.g., '20260527T033320Z')
        subdir: Subdirectory ('raw', 'staging', 'metadata', 'published')
    
    Returns:
        Path to the bronze subdirectory for this run
    """
    path = BRONZE_DIR / dataset / f"run_id={run_id}" / subdir
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_silver_path(
    dataset: str,
    run_id: str,
    filename: str | None = None,
) -> Path:
    """Get silver path for a dataset.
    
    Args:
        dataset: Dataset name
        run_id: Run ID
        filename: Optional filename (e.g., '{dataset}_{run_id}.jsonl')
    
    Returns:
        Path to the silver directory or file for this dataset run
    """
    if filename:
        path = SILVER_DIR / dataset / filename
    else:
        path = SILVER_DIR / dataset
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def get_schema_path(dataset: str) -> Path:
    """Get schema path for a dataset."""
    return SCHEMAS_DIR / f"{dataset}.schema.json"


def is_aws_ec2() -> bool:
    """Check if running on AWS EC2 by checking IMDS (Instance Metadata Service).

    Tries IMDSv2 first (token-based), falls back to IMDSv1.

    Note: This checks actual AWS metadata service. For mode-based detection,
    use is_vm_mode() instead.
    """
    try:
        socket.setdefaulttimeout(2)
        sock = socket.create_connection(("169.254.169.254", 80), timeout=2)
        sock.close()
        return True
    except (socket.timeout, socket.error, OSError):
        return False


def is_gcp_vm() -> bool:
    """Check if running on GCP VM by checking metadata service.
    
    Deprecated: Use is_aws_ec2() for AWS deployments. This function is kept
    for backward compatibility with existing GCP deployments.

    Note: This checks actual GCP metadata service. For mode-based detection,
    use is_vm_mode() instead.
    """
    try:
        socket.setdefaulttimeout(2)
        sock = socket.create_connection(("metadata.google.internal", 80), timeout=2)
        sock.close()
        return True
    except (socket.timeout, socket.error, OSError):
        return False


def get_effective_runtime_dir() -> tuple[Path, str]:
    """Get effective runtime dir and where it's mounted.

    Returns:
        Tuple of (runtime_dir, location) where location is 'vm' or 'local'
    """
    if is_vm_mode() or is_aws_ec2() or is_gcp_vm() or os.getenv("NEXUS_FORCE_GCP"):
        return (RUNTIME_DIR, "vm")
    return (RUNTIME_DIR, "local")


def get_config_runtime_dir() -> str:
    """Get runtime_dir from config file (for documentation/UI purposes)."""
    config_yaml = load_yaml(CONFIG_DIR / "download_defaults.yml")
    return config_yaml.get("runtime_dir", "")


def set_config_runtime_dir(value: str) -> None:
    """Set runtime_dir in config file."""
    config_path = CONFIG_DIR / "download_defaults.yml"
    config_yaml = load_yaml(config_path)
    config_yaml["runtime_dir"] = value
    with config_path.open("w", encoding="utf-8") as f:
        yaml.dump(config_yaml, f, default_flow_style=False, sort_keys=False)


# ============================================================================
# CONFIGURATION LOADERS
# ============================================================================

def load_polling_config(config_dir: Path = CONFIG_DIR) -> dict[str, Any]:
    """Load polling configuration from download_defaults.yml."""
    config = load_yaml(config_dir / "download_defaults.yml")
    return config.get("polling", {})


def load_backfill_config(config_dir: Path = CONFIG_DIR) -> dict[str, Any]:
    """Load backfill configuration from download_defaults.yml."""
    config = load_yaml(config_dir / "download_defaults.yml")
    return config.get("backfill", {})


def get_polling_interval(source: str) -> int:
    """Get polling interval in seconds for a source."""
    polling_config = load_polling_config()
    source_config = polling_config.get(source, {})
    return source_config.get("interval_seconds", 0)


def is_polling_enabled(source: str) -> bool:
    """Check if polling is enabled for a source."""
    return get_polling_interval(source) > 0


def load_dataset_catalog(domains_dir: Path = DOMAINS_DIR) -> dict[str, Any]:
    """Load dataset catalog from domains/ directory."""
    datasets: dict[str, Any] = {}
    for path in sorted(domains_dir.glob("*/datasets.yml")):
        datasets.update(load_yaml(path).get("datasets", {}))
    return {"datasets": datasets}


def load_quality_config(
    domains_dir: Path = DOMAINS_DIR,
    config_dir: Path = CONFIG_DIR,
) -> dict[str, Any]:
    """Load quality rules from domains/ and config/ directories."""
    default_rules = load_yaml(config_dir / "quality_defaults.yml").get("default_rules", {})
    datasets: dict[str, Any] = {}
    for path in sorted(domains_dir.glob("*/quality_rules.yml")):
        datasets.update(load_yaml(path).get("datasets", {}))
    return {"default_rules": default_rules, "datasets": datasets}


def load_semantic_config(
    domains_dir: Path = DOMAINS_DIR,
    config_dir: Path = CONFIG_DIR,
) -> dict[str, Any]:
    default_semantic = load_yaml(config_dir / "semantic_defaults.yml").get("default_semantic", {})
    datasets: dict[str, Any] = {}
    for path in sorted(domains_dir.glob("*/semantic_rules.yml")):
        datasets.update(load_yaml(path).get("datasets", {}))
    return {"default_semantic": default_semantic, "datasets": datasets}

def load_governance_defaults(config_dir: Path = CONFIG_DIR) -> dict[str, Any]:
    """Load governance defaults."""
    return load_yaml(config_dir / "governance_defaults.yml").get("default_governance", {})
