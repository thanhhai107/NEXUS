from __future__ import annotations

from common.data_contract import load_data_contract
from common.semantic import (
    build_business_glossary_export,
    build_openmetadata_export,
    load_semantic_contract,
    load_unit_mapping_table,
    normalize_unit_key,
)
from governance.entity_resolution import resolve_entities


def test_semantic_contract_loads_dataset_rules() -> None:
    contract = load_semantic_contract("openaq_measurements")

    assert contract.domain == "environment"
    assert contract.dataset_rules["grain"]["aggregation_level"] == "raw_observation"
    assert contract.dataset_rules["standards"]["time"]["storage_timezone"] == "UTC"


def test_data_contract_exposes_semantic_metadata() -> None:
    contract = load_data_contract("openaq_measurements")

    assert contract.semantic["dataset"] == "openaq_measurements"
    assert contract.semantic["dataset_rules"]["standards"]["units"]["canonical_unit"] == "ug/m3"


def test_unit_mapping_seed_is_loaded() -> None:
    mappings = load_unit_mapping_table()

    assert any(row["canonical_unit"] == "km" for row in mappings)
    assert normalize_unit_key("µg/m³") == "ug/m3"

def test_semantic_rules_cover_all_datasets() -> None:
    from common.config import load_dataset_catalog, load_semantic_config

    datasets = set(load_dataset_catalog()["datasets"])
    semantic_datasets = set(load_semantic_config()["datasets"])

    assert datasets <= semantic_datasets

def test_openmetadata_export_contains_field_metadata() -> None:
    payload = build_openmetadata_export(["openaq_measurements"])
    dataset = payload["datasets"][0]
    value_column = next(column for column in dataset["columns"] if column["name"] == "value")

    assert payload["tool"] == "OpenMetadata"
    assert dataset["glossaryTerm"] == "Air Quality Measurement"
    assert value_column["customProperties"]["unit"]["canonical_unit"] == "ug/m3"

def test_business_glossary_export_contains_alias_mapping() -> None:
    payload = build_business_glossary_export(["openaq_measurements"])

    assert any(term["term"] == "Air Quality Measurement" for term in payload["terms"])
    assert any(term.get("canonical_field") == "monitoring_site_id" for term in payload["terms"])

def test_entity_resolution_creates_canonical_id_and_crosswalk() -> None:
    result = resolve_entities(
        "openaq_measurements",
        [
            {"location_id": "500", "location": "London Marylebone", "latitude": "51.52", "longitude": "-0.15"},
            {"location_id": "500", "location": "Marylebone London", "latitude": "51.5201", "longitude": "-0.1501"},
        ],
    ).to_dict()

    canonical_ids = {record["canonical_entity_id"] for record in result["matched_records"]}
    assert len(canonical_ids) == 1
    assert result["crosswalk"][0]["match_method"] == "exact"
