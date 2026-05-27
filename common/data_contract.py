from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from common.config import PROJECT_ROOT, load_dataset_catalog, load_quality_config
from common.semantic import load_semantic_contract
from common.source_registry import (
    SourceRegistryEntry,
    build_registry_entry,
    derive_ingestion_method,
    derive_update_frequency,
)


@dataclass(frozen=True)
class DataContract:
    dataset: str
    source: SourceRegistryEntry
    required_columns: tuple[str, ...]
    primary_keys: tuple[str, ...]
    freshness_column: str | None
    max_age_hours: int
    schema: Mapping[str, Any] | None
    quality_thresholds: Mapping[str, Any]
    auto_fix: Mapping[str, Any]
    semantic_dedup_keys: tuple[str, ...]
    late_data_policy: Mapping[str, Any]
    semantic: Mapping[str, Any]
    extra: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "source": self.source.to_dict(),
            "required_columns": list(self.required_columns),
            "primary_keys": list(self.primary_keys),
            "freshness_column": self.freshness_column,
            "max_age_hours": self.max_age_hours,
            "schema_path": self.source.schema_path,
            "quality_thresholds": dict(self.quality_thresholds),
            "auto_fix": dict(self.auto_fix),
            "semantic_dedup_keys": list(self.semantic_dedup_keys),
            "late_data_policy": dict(self.late_data_policy),
            "semantic": dict(self.semantic),
            "ingestion_method": self.source.ingestion_method,
            "update_frequency": self.source.update_frequency,
            "extra": dict(self.extra),
        }


def _read_schema_file(schema_path: str | None) -> dict[str, Any] | None:
    if not schema_path:
        return None
    candidate = Path(schema_path)
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    if not candidate.exists():
        return None
    try:
        with candidate.open("r", encoding="utf-8") as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError):
        return None


def derive_semantic_dedup_keys(
    dataset: Mapping[str, Any],
    primary_keys: tuple[str, ...],
) -> tuple[str, ...]:
    configured = dataset.get("semantic_dedup_keys")
    if configured:
        return tuple(str(key) for key in configured)
    if primary_keys:
        return primary_keys
    return ("_nexus_record_id",)


def derive_late_data_policy(
    dataset: Mapping[str, Any],
    freshness_column: str | None,
) -> dict[str, Any]:
    configured = dict(dataset.get("late_data_policy") or {})
    source_type = str(dataset.get("source_type") or "").lower()
    default_watermark = "2 hours" if source_type in {"api_stream", "gtfs_realtime", "kafka_topic"} else "24 hours"
    return {
        "event_time_field": configured.get("event_time_field") or freshness_column or "_nexus_event_time",
        "watermark": configured.get("watermark") or default_watermark,
        "allowed_lateness": configured.get("allowed_lateness") or configured.get("watermark") or default_watermark,
        "late_record_action": configured.get("late_record_action") or "retain_for_reprocess",
        "aggregation_window": configured.get("aggregation_window") or "1 hour",
    }


def load_data_contract(dataset_name: str) -> DataContract:
    catalog = load_dataset_catalog().get("datasets", {})
    if dataset_name not in catalog:
        raise KeyError(f"Unknown dataset: {dataset_name}")
    dataset = catalog[dataset_name]
    quality_config = load_quality_config()
    rules = quality_config.get("datasets", {}).get(dataset_name, {}) or {}
    thresholds = dict(quality_config.get("default_rules", {}))

    registry_entry = build_registry_entry(dataset_name, dataset)
    schema = _read_schema_file(registry_entry.schema_path)
    required_columns = tuple(rules.get("required_columns") or ())
    primary_keys = tuple(dataset.get("primary_keys") or ())
    freshness_column = rules.get("freshness_column")
    max_age_hours = int(dataset.get("freshness_hours") or 24)
    auto_fix = dict(rules.get("auto_fix") or {})
    semantic_dedup_keys = derive_semantic_dedup_keys(dataset, primary_keys)
    late_data_policy = derive_late_data_policy(dataset, freshness_column)
    semantic = load_semantic_contract(dataset_name).to_dict()

    return DataContract(
        dataset=dataset_name,
        source=registry_entry,
        required_columns=required_columns,
        primary_keys=primary_keys,
        freshness_column=freshness_column,
        max_age_hours=max_age_hours,
        schema=schema,
        quality_thresholds=thresholds,
        auto_fix=auto_fix,
        semantic_dedup_keys=semantic_dedup_keys,
        late_data_policy=late_data_policy,
        semantic=semantic,
        extra={
            "target": dict(dataset.get("target") or {}),
            "topic": dataset.get("topic"),
            "poll_seconds": dataset.get("poll_seconds"),
            "ingestion_method_source": "explicit" if dataset.get("ingestion_method") else "derived",
            "update_frequency_source": "explicit" if dataset.get("update_frequency") else "derived",
        },
    )


def list_data_contracts() -> list[DataContract]:
    catalog = load_dataset_catalog().get("datasets", {})
    return [load_data_contract(name) for name in sorted(catalog)]


__all__ = [
    "DataContract",
    "derive_ingestion_method",
    "derive_update_frequency",
    "derive_late_data_policy",
    "derive_semantic_dedup_keys",
    "list_data_contracts",
    "load_data_contract",
]
