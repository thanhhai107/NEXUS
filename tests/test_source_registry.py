from common.data_contract import load_data_contract
from common.source_registry import (
    derive_ingestion_method,
    derive_update_frequency,
    get_source,
    list_sources,
)


def test_registry_lists_known_dataset():
    sources = list_sources()
    names = {entry.name for entry in sources}
    assert "stats19_collisions" in names
    entry = get_source("stats19_collisions")
    assert entry.domain == "transport"
    assert entry.ingestion_method == "batch_csv_download"
    assert entry.update_frequency == "every_1y"
    assert entry.owner == "transport-data-platform"


def test_derive_ingestion_method_falls_back_to_source_type():
    assert derive_ingestion_method({"source_type": "unknown_type"}) == "unknown_type"
    assert derive_ingestion_method({}) == "unknown"


def test_derive_update_frequency_uses_poll_seconds_when_present():
    assert derive_update_frequency({"poll_seconds": 30}) == "every_30s"
    assert derive_update_frequency({"freshness_hours": 1}) == "every_1h"
    assert derive_update_frequency({"freshness_hours": 168}) == "every_1w"
    assert derive_update_frequency({"freshness_hours": 8760}) == "every_1y"


def test_explicit_overrides_take_priority():
    dataset = {
        "source_type": "rest_api",
        "freshness_hours": 24,
        "ingestion_method": "custom_pull",
        "update_frequency": "hourly",
    }
    assert derive_ingestion_method(dataset) == "custom_pull"
    assert derive_update_frequency(dataset) == "hourly"


def test_data_contract_loads_schema_and_quality_rules():
    contract = load_data_contract("openaq_measurements")
    assert contract.dataset == "openaq_measurements"
    assert "location_id" in contract.required_columns
    assert contract.freshness_column == "datetime"
    assert contract.source.ingestion_method.startswith("stream")
    assert contract.schema is not None
    assert isinstance(contract.quality_thresholds, dict)
    assert contract.semantic_dedup_keys == ("location_id", "parameter", "datetime")
    assert contract.late_data_policy["event_time_field"] == "datetime"
    assert contract.late_data_policy["watermark"] == "2 hours"
