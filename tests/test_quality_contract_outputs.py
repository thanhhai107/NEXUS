from __future__ import annotations

from common.data_contract import load_data_contract
from governance.quality.gx_suite import generate_expectation_suite
from governance.quality.openmetadata import build_quality_result_payload
from governance.quality.silver import prepare_silver_records


def test_generated_gx_suite_contains_contract_and_semantic_expectations() -> None:
    contract = load_data_contract("openaq_measurements")

    suite = generate_expectation_suite(
        dataset=contract.dataset,
        required_columns=contract.required_columns,
        primary_keys=contract.primary_keys,
        freshness_column=contract.freshness_column,
        semantic_rules=contract.semantic["dataset_rules"],
    )

    expectation_types = [item["expectation_type"] for item in suite["expectations"]]
    assert suite["suite_name"] == "openaq_measurements.contract"
    assert "expect_compound_columns_to_be_unique" in expectation_types
    assert "expect_column_values_to_be_in_set" in expectation_types
    assert any(item["kwargs"].get("column") == "value_canonical" for item in suite["expectations"])


def test_silver_preparation_flags_missing_duplicates_outliers_and_conflicts() -> None:
    result = prepare_silver_records(
        [
            {"id": "1", "value": "10", "status": "ok"},
            {"id": "1", "value": "11", "status": "changed"},
            {"id": "2", "value": "12", "status": ""},
            {"id": "3", "value": "1000", "status": "ok"},
        ],
        required_columns=["id", "status"],
        dedup_keys=["id"],
        numeric_columns=["value"],
    )

    assert result.summary["output_count"] == 3
    assert result.summary["duplicate_record_count"] == 1
    assert result.summary["inconsistency_record_count"] == 1
    assert result.summary["missing_value_count"] == 1
    assert result.summary["outlier_record_count"] == 1
    assert result.records[2]["_nexus_outlier_fields"] == ["value"]


def test_openmetadata_quality_payload_has_test_case_result() -> None:
    payload = build_quality_result_payload(
        dataset="demo",
        status="failed",
        quality={
            "record_count": 2,
            "readiness_score": 0.5,
            "missing_ratio": 0.25,
            "duplicate_ratio": 0.0,
            "freshness_score": 1.0,
            "schema_valid": False,
            "issues": ["Schema validation failed."],
        },
        batch_id="b1",
    )

    assert payload["tool"] == "OpenMetadata"
    assert payload["testCaseResult"]["testCaseStatus"] == "Failed"
    assert payload["testCaseResult"]["result"]["schema_valid"] is False
