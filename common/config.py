from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = PROJECT_ROOT / "config"
DOMAINS_DIR = PROJECT_ROOT / "domains"
RUNTIME_DIR = PROJECT_ROOT / "runtime"
LOGS_DIR = RUNTIME_DIR / "logs"


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
