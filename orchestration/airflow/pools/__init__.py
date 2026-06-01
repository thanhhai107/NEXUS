"""Airflow Pool Definitions.

Loads pool configurations from YAML files.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

POOLS_DIR = Path(__file__).resolve().parents[2] / "orchestration" / "airflow" / "pools"


def load_pools_config() -> dict[str, Any]:
    """Load pool definitions from source_pools.yml."""
    pools_file = POOLS_DIR / "source_pools.yml"
    if not pools_file.exists():
        return {}
    
    with pools_file.open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    return config.get("pools", {})


def get_pool_name(source: str) -> str:
    """Get Airflow pool name for a source.
    
    Args:
        source: Source identifier (e.g., 'tpcds_store_sales')

    Returns:
        Pool name (e.g., 'tpcds_pool', 'tpc_pool')
    """
    # Check environment variable first
    env_name = f"NEXUS_AIRFLOW_POOL_{source.upper().replace('-', '_')}"
    env_pool = os.getenv(env_name)
    if env_pool:
        return env_pool
    
    # Check config
    pools = load_pools_config()
    specific_pool = f"{source}_pool"
    if specific_pool in pools:
        return specific_pool
    
    # Check if source has pool defined in config
    for pool_name, pool_config in pools.items():
        if isinstance(pool_config, dict) and pool_config.get("sources", []):
            if source in pool_config["sources"]:
                return pool_name
    
    # Fallback to default
    return os.getenv("NEXUS_AIRFLOW_API_POOL", "default_pool")


def get_pool_slots(pool_name: str) -> int:
    """Get slot count for a pool.
    
    Args:
        pool_name: Pool name
    
    Returns:
        Number of slots (default: 1)
    """
    pools = load_pools_config()
    pool_config = pools.get(pool_name, {})
    
    if isinstance(pool_config, dict):
        return pool_config.get("slots", 1)
    
    return 1


def list_pools() -> list[str]:
    """List all available pool names."""
    pools = load_pools_config()
    return list(pools.keys())
