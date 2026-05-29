from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from common.config import load_dataset_catalog


INGESTION_METHOD_BY_SOURCE_TYPE: dict[str, str] = {
    "csv_download": "batch_csv_download",
    "parquet_batch": "batch_parquet",
    "rest_api": "batch_api",
    "api_stream": "stream_api",
    "arcgis_hub": "batch_api",
    "gtfs_realtime": "stream_gtfs_realtime",
}


def _humanize_hours(hours: float) -> str:
    if hours <= 0:
        return "on_demand"
    if hours < 1:
        minutes = max(1, int(hours * 60))
        return f"every_{minutes}m"
    if hours < 24:
        return f"every_{int(hours)}h"
    days = hours / 24
    if days < 7:
        return f"every_{int(days)}d"
    weeks = days / 7
    if weeks < 4:
        return f"every_{int(weeks)}w"
    months = days / 30
    if months < 12:
        return f"every_{int(months)}mo"
    return f"every_{int(days / 365)}y"


def derive_ingestion_method(dataset: Mapping[str, Any]) -> str:
    if dataset.get("ingestion_method"):
        return str(dataset["ingestion_method"])
    source_type = str(dataset.get("source_type", "")).lower()
    return INGESTION_METHOD_BY_SOURCE_TYPE.get(source_type, source_type or "unknown")


def derive_update_frequency(dataset: Mapping[str, Any]) -> str:
    if dataset.get("update_frequency"):
        return str(dataset["update_frequency"])
    poll_seconds = dataset.get("poll_seconds")
    if poll_seconds:
        try:
            seconds = float(poll_seconds)
            if seconds < 60:
                return f"every_{int(seconds)}s"
            return _humanize_hours(seconds / 3600)
        except (TypeError, ValueError):
            pass
    freshness_hours = dataset.get("freshness_hours")
    if freshness_hours is not None:
        try:
            return _humanize_hours(float(freshness_hours))
        except (TypeError, ValueError):
            return "unknown"
    return "unknown"


@dataclass(frozen=True)
class SourceRegistryEntry:
    name: str
    domain: str
    description: str
    source_type: str
    source_uri: str
    ingestion_method: str
    update_frequency: str
    schema_path: str | None
    primary_keys: tuple[str, ...]
    owner: str
    steward: str
    sensitivity: str
    extra: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "domain": self.domain,
            "description": self.description,
            "source_type": self.source_type,
            "source_uri": self.source_uri,
            "ingestion_method": self.ingestion_method,
            "update_frequency": self.update_frequency,
            "schema_path": self.schema_path,
            "primary_keys": list(self.primary_keys),
            "owner": self.owner,
            "steward": self.steward,
            "sensitivity": self.sensitivity,
            "extra": dict(self.extra),
        }


def _governance_field(dataset: Mapping[str, Any], key: str, default: str) -> str:
    governance = dataset.get("governance") or {}
    value = governance.get(key)
    return str(value) if value else default


def build_registry_entry(name: str, dataset: Mapping[str, Any]) -> SourceRegistryEntry:
    extra_keys = {"target", "governance", "schema_path", "primary_keys"}
    extra = {key: value for key, value in dataset.items() if key not in extra_keys}
    return SourceRegistryEntry(
        name=name,
        domain=str(dataset.get("domain", "unknown")),
        description=str(dataset.get("description", "")),
        source_type=str(dataset.get("source_type", "unknown")),
        source_uri=str(dataset.get("source_uri", "")),
        ingestion_method=derive_ingestion_method(dataset),
        update_frequency=derive_update_frequency(dataset),
        schema_path=dataset.get("schema_path"),
        primary_keys=tuple(str(key) for key in dataset.get("primary_keys") or ()),
        owner=_governance_field(dataset, "owner", "data-platform"),
        steward=_governance_field(dataset, "steward", "data-steward"),
        sensitivity=_governance_field(dataset, "sensitivity", "public"),
        extra=extra,
    )


def list_sources() -> list[SourceRegistryEntry]:
    catalog = load_dataset_catalog().get("datasets", {})
    return [build_registry_entry(name, dataset) for name, dataset in sorted(catalog.items())]


def get_source(name: str) -> SourceRegistryEntry:
    catalog = load_dataset_catalog().get("datasets", {})
    if name not in catalog:
        raise KeyError(f"Unknown source: {name}")
    return build_registry_entry(name, catalog[name])
