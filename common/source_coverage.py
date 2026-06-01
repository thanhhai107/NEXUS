"""Source coverage module — TPC-DS SF=1 only."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from common.config import DOMAINS_DIR, load_dataset_catalog

COVERAGE_MAP_FILE = "ingestion_coverage_map.json"

STREAM_SOURCE_KEYS: dict[str, str] = {}


def write_ingestion_coverage_map(
    output_path: Path | None = None,
    source_dir: Path | None = None,
    domains_dir: Path | None = None,
    config_dir: Path | None = None,
) -> dict[str, Any]:
    domains_dir = domains_dir or DOMAINS_DIR
    output_path = output_path or (domains_dir / COVERAGE_MAP_FILE)

    catalog = load_dataset_catalog(domains_dir)
    datasets = catalog.get("datasets", {})

    entries: list[dict[str, Any]] = []
    for name, info in datasets.items():
        entries.append({
            "dataset": name,
            "domain": "tpc",
            "source_type": info.get("source_type", "data_caterer"),
            "bronze_table": info.get("bronze_table", f"nexus.bronze.{name}"),
            "silver_table": info.get("silver_table", f"nexus.silver.{name}"),
            "gold_table": info.get("gold_table", f"nexus.gold.{name}"),
            "status": "implemented",
            "connector_module": "ingestion.data_caterer.runner",
            "connector_mode": "batch",
        })

    payload: dict[str, Any] = {
        "datasets": entries,
        "summary": {
            "dataset_count": len(entries),
            "source_count": 1,
            "implemented_count": len(entries),
            "planned_count": 0,
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        __import__("json").dumps(payload, indent=2, default=str),
        encoding="utf-8",
    )

    return payload
