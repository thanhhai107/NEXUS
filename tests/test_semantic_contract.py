from __future__ import annotations

from common.data_contract import load_data_contract
from common.semantic import load_semantic_contract, load_unit_mapping_table, normalize_unit_key


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
