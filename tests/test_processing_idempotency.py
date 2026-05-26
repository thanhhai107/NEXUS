from __future__ import annotations

import pytest

from processing.common.idempotency import (
    insert_columns,
    insert_values,
    merge_assignments,
    merge_condition,
    parse_key_list,
    safe_identifier,
)


def test_parse_key_list() -> None:
    assert parse_key_list("location_id, parameter, datetime") == [
        "location_id",
        "parameter",
        "datetime",
    ]
    assert parse_key_list(["a", " b "]) == ["a", "b"]


def test_merge_sql_fragments_are_null_safe() -> None:
    keys = ["window_start", "window_end", "location_id"]
    assert merge_condition(keys) == (
        "target.`window_start` <=> source.`window_start` AND "
        "target.`window_end` <=> source.`window_end` AND "
        "target.`location_id` <=> source.`location_id`"
    )
    assert merge_assignments(["record_count"]) == "`record_count` = source.`record_count`"
    assert insert_columns(["a", "b"]) == "`a`, `b`"
    assert insert_values(["a", "b"]) == "source.`a`, source.`b`"


def test_safe_identifier_rejects_sql_fragments() -> None:
    with pytest.raises(ValueError):
        safe_identifier("id; DROP TABLE demo")
