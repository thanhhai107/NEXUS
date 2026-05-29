from __future__ import annotations

from typing import Any, Mapping, Sequence


def generate_expectation_suite(
    *,
    dataset: str,
    required_columns: Sequence[str],
    primary_keys: Sequence[str],
    freshness_column: str | None,
    semantic_rules: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Generate a portable Great Expectations suite payload from a data contract."""
    expectations: list[dict[str, Any]] = [
        {
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 1},
        }
    ]
    for column in required_columns:
        expectations.extend([
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": column},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": column},
            },
        ])

    keys = [key for key in primary_keys if key]
    if len(keys) == 1:
        expectations.append({
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": keys[0]},
        })
    elif len(keys) > 1:
        expectations.append({
            "expectation_type": "expect_compound_columns_to_be_unique",
            "kwargs": {"column_list": keys},
        })

    if freshness_column:
        expectations.append({
            "expectation_type": "expect_column_values_to_be_dateutil_parseable",
            "kwargs": {"column": freshness_column},
        })

    units = dict(((semantic_rules or {}).get("standards") or {}).get("units") or {})
    unit_field = units.get("unit_field")
    value_field = units.get("value_field")
    converted_value_field = units.get("converted_value_field") or (
        f"{value_field}_canonical" if value_field else None
    )
    if unit_field and units.get("allowed_source_units"):
        expectations.append({
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": unit_field,
                "value_set": list(units["allowed_source_units"]),
            },
        })
    if converted_value_field:
        expectations.append({
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": converted_value_field},
        })
        expected_range = dict(units.get("expected_range") or {})
        if expected_range:
            expectations.append({
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": converted_value_field,
                    "min_value": expected_range.get("min"),
                    "max_value": expected_range.get("max"),
                },
            })

    return {
        "suite_name": f"{dataset}.contract",
        "meta": {
            "dataset": dataset,
            "generated_by": "nexus.data_contract",
        },
        "expectations": expectations,
    }


__all__ = ["generate_expectation_suite"]
