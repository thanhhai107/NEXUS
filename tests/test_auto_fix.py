from __future__ import annotations

from governance.quality.auto_fix import apply_auto_fix, normalize_field_name, normalize_field_names


def test_auto_fix_trims_normalizes_and_fills_missing() -> None:
    result = apply_auto_fix(
        [{" Full Name ": " Ada ", "Department": ""}],
        {
            "trim_strings": True,
            "normalize_column_names": True,
            "fill_missing": {"Department": "unknown"},
        },
    )

    assert result.records == [{"full_name": "Ada", "department": "unknown"}]
    assert result.summary["changed_record_count"] == 1
    assert result.summary["trimmed_value_count"] == 1
    assert result.summary["filled_missing_count"] == 1


def test_normalize_quality_field_names() -> None:
    auto_fix = {"normalize_column_names": True}
    assert normalize_field_name("Start_Time", auto_fix) == "start_time"
    assert normalize_field_names(["ID", "Start_Time"], auto_fix) == ["id", "start_time"]
