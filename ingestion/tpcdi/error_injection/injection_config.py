"""Load and resolve injection config from domains/tpc/injection_config.yml."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_CONFIG_PATH = _PROJECT_ROOT / "domains" / "tpc" / "injection_config.yml"

_CONFIG_CACHE: dict[str, Any] | None = None
_CONFIG_MTIME: float | None = None


def load_injection_config(reload: bool = False) -> dict[str, Any]:
    """Load injection_config.yml, cached in memory.

    Returns a dict with keys: global, mutation_defaults, categories, severity_profiles.
    """
    global _CONFIG_CACHE, _CONFIG_MTIME

    if not reload and _CONFIG_CACHE is not None:
        if _CONFIG_PATH.exists():
            mtime = _CONFIG_PATH.stat().st_mtime
            if _CONFIG_MTIME is not None and mtime == _CONFIG_MTIME:
                return _CONFIG_CACHE

    if not _CONFIG_PATH.exists():
        return {}

    with open(_CONFIG_PATH, encoding="utf-8") as fh:
        _CONFIG_CACHE = yaml.safe_load(fh) or {}
    _CONFIG_MTIME = _CONFIG_PATH.stat().st_mtime
    return _CONFIG_CACHE


def get_global(key: str, default: Any = None) -> Any:
    """Get a global config value."""
    cfg = load_injection_config()
    return cfg.get("global", {}).get(key, default)


def get_mutation_defaults(injector_type: str, mutation_type: str | None = None) -> dict[str, Any]:
    """Get defaults for a specific injector and optional mutation_type.

    Example: ``get_mutation_defaults("semantic", "unit_changed")``
    returns ``{"factor": 100.0}``.
    """
    cfg = load_injection_config()
    defaults = cfg.get("mutation_defaults", {})
    injector = defaults.get(injector_type, {})
    if mutation_type:
        return injector.get(mutation_type, {})
    return injector


def get_category_weights() -> dict[str, float]:
    """Return {category_key: weight} mapping."""
    cfg = load_injection_config()
    cats = cfg.get("categories", {})
    return {k: v.get("weight", 0.1) for k, v in cats.items()}


def get_severity_profile(profile_name: str) -> dict[str, Any]:
    """Get a pre-configured severity profile (minimal, standard, aggressive)."""
    cfg = load_injection_config()
    return cfg.get("severity_profiles", {}).get(profile_name, {})


def resolve_seed() -> int:
    """Resolve injection seed with env override."""
    env_seed = os.environ.get("NEXUS_INJECTION_SEED")
    if env_seed is not None:
        try:
            return int(env_seed)
        except ValueError:
            pass
    return int(get_global("seed", 42))
