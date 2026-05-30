"""Orchestration Configuration Module.

Loads configuration from YAML files in the config/ directory.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

CONFIG_DIR = Path(__file__).resolve().parents[3] / "orchestration" / "airflow" / "config"


def load_yaml(path: Path) -> dict[str, Any]:
    """Load YAML configuration file."""
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_polling_config() -> dict[str, Any]:
    """Load polling configuration."""
    return load_yaml(CONFIG_DIR / "polling_config.yml")


def load_retry_config() -> dict[str, Any]:
    """Load retry configuration."""
    return load_yaml(CONFIG_DIR / "retry_config.yml")


def load_coverage_policy() -> dict[str, Any]:
    """Load coverage policy configuration."""
    return load_yaml(CONFIG_DIR / "coverage_policy.yml")


def get_source_polling_config(source: str) -> dict[str, Any]:
    """Get polling config for a specific source."""
    config = load_polling_config()
    return config.get("sources", {}).get(source, {})


def get_source_retry_config(source: str) -> dict[str, Any]:
    """Get retry config for a specific source, with defaults."""
    config = load_retry_config()
    default = config.get("default", {})
    per_source = config.get("per_source", {}).get(source, {})
    return {**default, **per_source}


def get_source_coverage_policy(source: str) -> dict[str, Any]:
    """Get coverage policy for a specific source."""
    config = load_coverage_policy()
    default = config.get("default", {})
    per_source = config.get("per_source", {}).get(source, {})
    return {**default, **per_source}


def is_polling_enabled(source: str) -> bool:
    """Check if polling is enabled for a source."""
    source_config = get_source_polling_config(source)
    return source_config.get("enabled", False) and source_config.get("interval_seconds", 0) > 0


def get_polling_interval(source: str) -> int:
    """Get polling interval in seconds for a source."""
    source_config = get_source_polling_config(source)
    return source_config.get("interval_seconds", 0)


def get_source_timeout(source: str) -> int:
    """Get timeout in minutes for a source."""
    source_config = get_source_polling_config(source)
    return source_config.get("timeout_minutes", 10)


def list_enabled_sources() -> list[str]:
    """List all enabled polling sources."""
    config = load_polling_config()
    sources = config.get("sources", {})
    return [
        name for name, cfg in sources.items()
        if cfg.get("enabled") and cfg.get("interval_seconds", 0) > 0
    ]
