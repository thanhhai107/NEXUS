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
            ExpectColumnValuesToBeDateutilParseable,
            ExpectColumnValuesToBeUnique,
            ExpectColumnValuesToNotBeNull,
            ExpectCompoundColumnsToBeUnique,
            ExpectTableRowCountToBeBetween,
        )
    except ImportError as exc:
        return _error_summary(f"{type(exc).__name__}: {exc}")

    try:
        data_frame = pd.DataFrame([dict(record) for record in records])
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
