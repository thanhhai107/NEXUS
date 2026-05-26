from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "config"
DOMAINS_DIR = PROJECT_ROOT / "domains"

# Runtime directory: supports NEXUS_RUNTIME_DIR env var (for GCP VM: /data)
# Falls back to PROJECT_ROOT / "runtime" for local development
_RUNTIME_DIR = os.getenv("NEXUS_RUNTIME_DIR")
if _RUNTIME_DIR:
    RUNTIME_DIR = Path(_RUNTIME_DIR).resolve()
else:
    RUNTIME_DIR = PROJECT_ROOT / "runtime"

LOGS_DIR = RUNTIME_DIR / "logs"

# Subdirectories within RUNTIME_DIR
DATASETS_DIR = RUNTIME_DIR / "datasets"  # Downloaded raw files (source format)
RAW_DIR = RUNTIME_DIR / "raw"  # Canonical envelope format
QUARANTINE_DIR = RUNTIME_DIR / "quarantine"  # Invalid records
DLQ_DIR = RUNTIME_DIR / "dlq"  # Dead Letter Queue


def get_runtime_path(*parts: str) -> Path:
    """Get path within runtime directory, creating it if needed."""
    path = RUNTIME_DIR.joinpath(*parts)
    path.mkdir(parents=True, exist_ok=True)
    return path


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


# Polling configuration
def load_polling_config(config_dir: Path = CONFIG_DIR) -> dict[str, Any]:
    """Load polling configuration from download_defaults.yml.

    Returns:
        Dict with 'polling' key containing per-source polling config
    """
    config = load_yaml(config_dir / "download_defaults.yml")
    return config.get("polling", {})


def load_backfill_config(config_dir: Path = CONFIG_DIR) -> dict[str, Any]:
    """Load backfill configuration from download_defaults.yml.

    Returns:
        Dict with backfill settings
    """
    config = load_yaml(config_dir / "download_defaults.yml")
    return config.get("backfill", {})


def get_polling_interval(source: str) -> int:
    """Get polling interval in seconds for a source.

    Args:
        source: Source key (e.g., 'tfl_arrivals', 'waqi')

    Returns:
        Polling interval in seconds, or 0 if polling disabled
    """
    polling_config = load_polling_config()
    source_config = polling_config.get(source, {})
    return source_config.get("interval_seconds", 0)


def is_polling_enabled(source: str) -> bool:
    """Check if polling is enabled for a source.

    Args:
        source: Source key

    Returns:
        True if polling enabled and interval > 0
    """
    return get_polling_interval(source) > 0


def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def load_dataset_catalog(domains_dir: Path = DOMAINS_DIR) -> dict[str, Any]:
    datasets: dict[str, Any] = {}
    for path in sorted(domains_dir.glob("*/datasets.yml")):
        datasets.update(load_yaml(path).get("datasets", {}))
    return {"datasets": datasets}


def load_quality_config(
    domains_dir: Path = DOMAINS_DIR,
    config_dir: Path = CONFIG_DIR,
) -> dict[str, Any]:
    default_rules = load_yaml(config_dir / "quality_defaults.yml").get("default_rules", {})
    datasets: dict[str, Any] = {}
    for path in sorted(domains_dir.glob("*/quality_rules.yml")):
        datasets.update(load_yaml(path).get("datasets", {}))
    return {"default_rules": default_rules, "datasets": datasets}


def load_governance_defaults(config_dir: Path = CONFIG_DIR) -> dict[str, Any]:
    return load_yaml(config_dir / "governance_defaults.yml").get("default_governance", {})
