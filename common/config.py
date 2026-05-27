"""
NEXUS Configuration Module.

Unified config cho cả local (runtime/) và VM (/data/).
Cấu trúc thư mục theo Medallion Architecture:
    runtime/           (local)
        /data/        (VM)
            ├── lake/           Bronze → Silver → Gold
            ├── pipeline/       Orchestration
            ├── warehouse/      Query engines
            ├── runtime/        Workspace tạm
            ├── dlq/            Dead Letter Queue
            └── quarantine/     Invalid records
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


def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


# ============================================================================
# RUNTIME DIRECTORY RESOLUTION
# ============================================================================
# Resolution order:
#   1. NEXUS_RUNTIME_DIR environment variable (highest priority)
#   2. runtime_dir in download_defaults.yml
#   3. PROJECT_ROOT / "runtime" (default)

def _resolve_runtime_dir() -> Path:
    env_runtime = os.getenv("NEXUS_RUNTIME_DIR")
    if env_runtime:
        return Path(env_runtime).resolve()

    config_yaml = load_yaml(CONFIG_DIR / "download_defaults.yml")
    config_runtime = config_yaml.get("runtime_dir", "")
    if config_runtime:
        config_path = Path(config_runtime).resolve()
        return config_path if config_path.is_absolute() else PROJECT_ROOT / config_path

    return PROJECT_ROOT / "runtime"


RUNTIME_DIR = _resolve_runtime_dir()


# ============================================================================
# LAKE STRUCTURE (Medallion Architecture)
# ============================================================================
# Data lake với 3 tier: Bronze → Silver → Gold

LAKE_DIR = RUNTIME_DIR / "lake"
BRONZE_DIR = LAKE_DIR / "bronze"  # Raw data gốc (source format)
SILVER_DIR = LAKE_DIR / "silver"  # Validated + Envelope wrapped
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
# LEGACY ALIASES (Backward Compatibility - Deprecated)
# ============================================================================
# Will be removed after migration

DATASETS_DIR = BRONZE_DIR  # Legacy alias - use BRONZE_DIR
RAW_DIR = SILVER_DIR       # Legacy alias - use SILVER_DIR


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_runtime_path(*parts: str) -> Path:
    """Get path within runtime directory, creating it if needed."""
    path = RUNTIME_DIR.joinpath(*parts)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_bronze_path(dataset: str, run_id: str, filename: str) -> Path:
    """Get bronze path for a dataset file with year/month partitioning."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    path = BRONZE_DIR / dataset / f"year={now.year}" / f"month={now.month:02d}"
    path.mkdir(parents=True, exist_ok=True)
    return path / filename


def get_silver_path(dataset: str, run_id: str, filename: str) -> Path:
    """Get silver path for a dataset file with year/month partitioning."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    path = SILVER_DIR / dataset / f"year={now.year}" / f"month={now.month:02d}"
    path.mkdir(parents=True, exist_ok=True)
    return path / filename


def get_schema_path(dataset: str) -> Path:
    """Get schema path for a dataset."""
    return SCHEMAS_DIR / f"{dataset}.schema.json"


def is_gcp_vm() -> bool:
    """Check if running on GCP VM by checking metadata service."""
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
        Tuple of (runtime_dir, location) where location is 'gcp' or 'local'
    """
    if is_gcp_vm() or os.getenv("NEXUS_FORCE_GCP"):
        return (RUNTIME_DIR, "gcp")
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
