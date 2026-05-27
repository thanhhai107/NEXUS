from __future__ import annotations

import contextlib
import io
import os
import re
from typing import Any, Mapping, Sequence

Record = Mapping[str, object]


def gx_enabled() -> bool:
    value = os.getenv("NEXUS_GX_ENABLED", "true").strip().lower()
    return value not in {"0", "false", "no", "off"}


def run_great_expectations_validation(
    dataset: str,
    records: Sequence[Record],
    required_columns: Sequence[str],
    primary_keys: Sequence[str],
    freshness_column: str,
) -> dict[str, Any]:
    """Run GX Core expectations against an in-memory record batch."""
    if not gx_enabled():
        return {
            "enabled": False,
            "success": None,
            "expectation_count": 0,
            "successful_expectation_count": 0,
            "failed_expectations": [],
            "results": [],
        }

    try:
        import great_expectations as gx
        import pandas as pd
        from great_expectations.expectations import (
            ExpectColumnToExist,
            ExpectColumnValuesToBeBetween,
            ExpectColumnValuesToBeDateutilParseable,
            ExpectColumnValuesToBeInSet,
            ExpectColumnValuesToBeUnique,
            ExpectColumnValuesToNotBeNull,
            ExpectCompoundColumnsToBeUnique,
            ExpectTableRowCountToBeBetween,
        )
    except ImportError as exc:
        return _fallback_validation(
            records=records,
            required_columns=required_columns,
            primary_keys=primary_keys,
            freshness_column=freshness_column,
            fallback_reason=f"{type(exc).__name__}: {exc}",
        )

    try:
        data_frame = pd.DataFrame([dict(record) for record in records])
        data_frame, semantic_unit_check = _apply_semantic_unit_mapping(dataset, data_frame)
        observed_columns = set(data_frame.columns)
        context = gx.get_context(mode="ephemeral")
        datasource = context.data_sources.add_pandas(name=f"nexus_{_safe_name(dataset)}")
        asset = datasource.add_dataframe_asset(name=_safe_name(dataset))
        batch_definition = asset.add_batch_definition_whole_dataframe("current_batch")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": data_frame})

        expectations = [ExpectTableRowCountToBeBetween(min_value=1)]
        for column in required_columns:
            expectations.append(ExpectColumnToExist(column=column))
            if column in observed_columns:
                expectations.append(ExpectColumnValuesToNotBeNull(column=column))

        primary_key_columns = [column for column in primary_keys if column]
        if len(primary_key_columns) == 1 and primary_key_columns[0] in observed_columns:
            expectations.append(ExpectColumnValuesToBeUnique(column=primary_key_columns[0]))
        elif len(primary_key_columns) > 1 and all(
            column in observed_columns for column in primary_key_columns
        ):
            expectations.append(ExpectCompoundColumnsToBeUnique(column_list=primary_key_columns))

        if freshness_column and freshness_column in observed_columns:
            expectations.append(ExpectColumnValuesToBeDateutilParseable(column=freshness_column))

        if semantic_unit_check:
            unit_field = semantic_unit_check["unit_field"]
            converted_value_field = semantic_unit_check["converted_value_field"]
            allowed_source_units = semantic_unit_check.get("allowed_source_units") or []
            expected_range = semantic_unit_check.get("expected_range") or {}
            if unit_field in observed_columns and allowed_source_units:
                expectations.append(
                    ExpectColumnValuesToBeInSet(
                        column=unit_field,
                        value_set=allowed_source_units,
                    )
                )
            if converted_value_field in observed_columns:
                expectations.append(ExpectColumnValuesToNotBeNull(column=converted_value_field))
                if expected_range:
                    expectations.append(
                        ExpectColumnValuesToBeBetween(
                            column=converted_value_field,
                            min_value=expected_range.get("min"),
                            max_value=expected_range.get("max"),
                        )
                    )

        results = []
        for expectation in expectations:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                validation = batch.validate(expectation)
            results.append(_compact_result(validation.to_json_dict()))
    except Exception as exc:
        return _error_summary(f"{type(exc).__name__}: {exc}")

    successful_count = sum(1 for result in results if result["success"])
    failed_expectations = [
        _expectation_label(result)
        for result in results
        if not result["success"]
    ]
    return {
        "enabled": True,
        "success": successful_count == len(results),
        "expectation_count": len(results),
        "successful_expectation_count": successful_count,
        "failed_expectations": failed_expectations,
        "results": results,
    }


def _safe_name(value: str) -> str:
    name = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_")
    return name or "dataset"


def _apply_semantic_unit_mapping(dataset: str, data_frame: Any) -> tuple[Any, dict[str, Any]]:
    """Add a derived canonical-value column for GX unit-conversion checks."""
    try:
        from common.semantic import (
            load_semantic_contract,
            load_unit_mapping_table,
            normalize_unit_key,
        )
    except Exception:
        return data_frame, {}

    try:
        contract = load_semantic_contract(dataset).to_dict()
    except KeyError:
        return data_frame, {}

    dataset_rules = dict(contract.get("dataset_rules") or {})
    standards = dict(dataset_rules.get("standards") or {})
    units = dict(standards.get("units") or {})
    if not units:
        return data_frame, {}

    value_field = str(units.get("value_field") or "")
    unit_field = str(units.get("unit_field") or "")
    implicit_source_unit = units.get("implicit_source_unit")
    if not value_field or value_field not in data_frame.columns:
        return data_frame, {}

    if not unit_field and implicit_source_unit:
        unit_field = f"__nexus_{value_field}_unit"
        data_frame[unit_field] = str(implicit_source_unit)
    if not unit_field or unit_field not in data_frame.columns:
        return data_frame, {}

    dimension_type = str(units.get("dimension_type") or "").strip().lower()
    mapping_rows = [
        row
        for row in load_unit_mapping_table()
        if not dimension_type or str(row.get("dimension_type") or "").strip().lower() == dimension_type
    ]
    supported_rows = [
        row
        for row in mapping_rows
        if str(row.get("conversion_supported", "true")).strip().lower()
        not in {"0", "false", "no", "unsupported"}
    ]
    lookup = {
        str(row.get("source_unit_normalized") or ""): row
        for row in supported_rows
        if row.get("source_unit_normalized")
    }
    if not lookup:
        return data_frame, {}

    converted_value_field = str(units.get("converted_value_field") or f"__nexus_{value_field}_canonical")
    converted_values = []
    for raw_unit, raw_value in zip(data_frame[unit_field], data_frame[value_field]):
        mapping = lookup.get(normalize_unit_key(raw_unit))
        converted_values.append(_convert_unit_value(raw_value, mapping))

    data_frame[converted_value_field] = converted_values
    expected_range = dict(units.get("expected_range") or {})
    expected_range = {
        key: parsed
        for key, value in expected_range.items()
        if (parsed := _optional_float(value)) is not None
    }
    allowed_source_units = list(units.get("allowed_source_units") or [])
    if not allowed_source_units:
        allowed_source_units = sorted(
            {
                str(row.get("source_unit"))
                for row in mapping_rows
                if row.get("source_unit")
            }
        )

    return data_frame, {
        "unit_field": unit_field,
        "value_field": value_field,
        "converted_value_field": converted_value_field,
        "canonical_unit": units.get("canonical_unit"),
        "allowed_source_units": allowed_source_units,
        "expected_range": expected_range,
        "mapping_rows": len(mapping_rows),
    }


def _convert_unit_value(raw_value: object, mapping: Mapping[str, Any] | None) -> float | None:
    if not mapping:
        return None
    try:
        value = float(raw_value)
        scale_factor = float(mapping.get("scale_factor"))
        offset = float(mapping.get("offset") or 0)
    except (TypeError, ValueError):
        return None
    return round((value * scale_factor) + offset, 6)


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compact_result(validation: dict[str, Any]) -> dict[str, Any]:
    config = dict(validation.get("expectation_config") or {})
    kwargs = dict(config.get("kwargs") or {})
    kwargs.pop("batch_id", None)
    result = dict(validation.get("result") or {})
    exception_info = dict(validation.get("exception_info") or {})

    return {
        "expectation": config.get("type"),
        "success": bool(validation.get("success")),
        "kwargs": kwargs,
        "observed_value": result.get("observed_value"),
        "unexpected_count": result.get("unexpected_count"),
        "unexpected_percent": result.get("unexpected_percent"),
        "exception": exception_info.get("exception_message"),
    }


def _expectation_label(result: Mapping[str, Any]) -> str:
    kwargs = ", ".join(f"{key}={value}" for key, value in dict(result.get("kwargs") or {}).items())
    expectation = result.get("expectation") or "unknown_expectation"
    return f"{expectation}({kwargs})" if kwargs else str(expectation)


def _fallback_validation(
    *,
    records: Sequence[Record],
    required_columns: Sequence[str],
    primary_keys: Sequence[str],
    freshness_column: str,
    fallback_reason: str,
) -> dict[str, Any]:
    """Small deterministic validator used when GX is not installed locally."""

    rows = [dict(record) for record in records]
    observed_columns = set().union(*(row.keys() for row in rows)) if rows else set()
    results: list[dict[str, Any]] = [
        {
            "expectation": "expect_table_row_count_to_be_between",
            "success": len(rows) >= 1,
            "kwargs": {"min_value": 1},
            "observed_value": len(rows),
            "unexpected_count": 0 if rows else 1,
            "unexpected_percent": 0 if rows else 100,
            "exception": None,
        }
    ]

    for column in required_columns:
        exists = column in observed_columns
        results.append(
            {
                "expectation": "expect_column_to_exist",
                "success": exists,
                "kwargs": {"column": column},
                "observed_value": exists,
                "unexpected_count": 0 if exists else len(rows),
                "unexpected_percent": 0 if exists else 100,
                "exception": None,
            }
        )
        if exists:
            null_count = sum(1 for row in rows if row.get(column) in {None, ""})
            results.append(
                {
                    "expectation": "expect_column_values_to_not_be_null",
                    "success": null_count == 0,
                    "kwargs": {"column": column},
                    "observed_value": len(rows) - null_count,
                    "unexpected_count": null_count,
                    "unexpected_percent": round((null_count / len(rows)) * 100, 4) if rows else 0,
                    "exception": None,
                }
            )

    primary_key_columns = [column for column in primary_keys if column]
    if len(primary_key_columns) == 1 and primary_key_columns[0] in observed_columns:
        column = primary_key_columns[0]
        values = [row.get(column) for row in rows]
        duplicate_count = len(values) - len(set(values))
        results.append(
            {
                "expectation": "expect_column_values_to_be_unique",
                "success": duplicate_count == 0,
                "kwargs": {"column": column},
                "observed_value": len(set(values)),
                "unexpected_count": duplicate_count,
                "unexpected_percent": round((duplicate_count / len(rows)) * 100, 4) if rows else 0,
                "exception": None,
            }
        )
    elif len(primary_key_columns) > 1 and all(column in observed_columns for column in primary_key_columns):
        values = [tuple(row.get(column) for column in primary_key_columns) for row in rows]
        duplicate_count = len(values) - len(set(values))
        results.append(
            {
                "expectation": "expect_compound_columns_to_be_unique",
                "success": duplicate_count == 0,
                "kwargs": {"column_list": primary_key_columns},
                "observed_value": len(set(values)),
                "unexpected_count": duplicate_count,
                "unexpected_percent": round((duplicate_count / len(rows)) * 100, 4) if rows else 0,
                "exception": None,
            }
        )

    if freshness_column and freshness_column in observed_columns:
        invalid_count = sum(
            1 for row in rows
            if not _is_parseable_datetime(row.get(freshness_column))
        )
        results.append(
            {
                "expectation": "expect_column_values_to_be_dateutil_parseable",
                "success": invalid_count == 0,
                "kwargs": {"column": freshness_column},
                "observed_value": len(rows) - invalid_count,
                "unexpected_count": invalid_count,
                "unexpected_percent": round((invalid_count / len(rows)) * 100, 4) if rows else 0,
                "exception": None,
            }
        )

    successful_count = sum(1 for result in results if result["success"])
    failed_expectations = [_expectation_label(result) for result in results if not result["success"]]
    return {
        "enabled": True,
        "success": successful_count == len(results),
        "expectation_count": len(results),
        "successful_expectation_count": successful_count,
        "failed_expectations": failed_expectations,
        "results": results,
        "engine": "fallback",
        "fallback_reason": fallback_reason,
    }


def _is_parseable_datetime(value: object) -> bool:
    if not value:
        return False
    try:
        from datetime import datetime

        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def _error_summary(error: str) -> dict[str, Any]:
    return {
        "enabled": True,
        "success": False,
        "expectation_count": 0,
        "successful_expectation_count": 0,
        "failed_expectations": [],
        "results": [],
        "error": error,
    }
