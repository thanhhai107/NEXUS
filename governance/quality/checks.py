from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Mapping, Sequence

from governance.quality.schema import validate_json_schema


Record = Mapping[str, object]


@dataclass
class QualityResult:
    dataset: str
    record_count: int
    missing_ratio: float
    duplicate_ratio: float
    schema_valid: bool
    freshness_score: float
    readiness_score: float
    issues: list[str] = field(default_factory=list)


def missing_value_ratio(records: Sequence[Record], required_columns: Sequence[str]) -> float:
    if not records or not required_columns:
        return 0.0

    missing = 0
    total = len(records) * len(required_columns)
    for record in records:
        for column in required_columns:
            value = record.get(column)
            if value is None or value == "":
                missing += 1
    return missing / total


def duplicate_ratio(records: Sequence[Record], primary_keys: Sequence[str]) -> float:
    if not records or not primary_keys:
        return 0.0

    keys = [tuple(record.get(key) for key in primary_keys) for record in records]
    duplicate_count = sum(count - 1 for count in Counter(keys).values() if count > 1)
    return duplicate_count / len(records)


def validate_schema(records: Sequence[Record], required_columns: Sequence[str]) -> tuple[bool, list[str]]:
    observed_columns = set().union(*(record.keys() for record in records)) if records else set()
    missing_columns = sorted(set(required_columns) - observed_columns)
    if missing_columns:
        return False, [f"Missing required columns: {', '.join(missing_columns)}"]
    return True, []


def freshness_score(records: Sequence[Record], freshness_column: str, max_age_hours: int) -> float:
    if not records:
        return 0.0

    now = datetime.now(timezone.utc)
    fresh_records = 0

    for record in records:
        raw_value = record.get(freshness_column)
        if not raw_value:
            continue
        try:
            parsed = datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
        except ValueError:
            continue
        age_hours = (now - parsed.astimezone(timezone.utc)).total_seconds() / 3600
        if age_hours <= max_age_hours:
            fresh_records += 1

    return fresh_records / len(records)


def readiness_score(
    missing_ratio: float,
    duplicate_ratio_value: float,
    schema_valid: bool,
    freshness_score_value: float,
) -> float:
    """Calculate a simple 0-1 readiness score for downstream consumption."""
    schema_component = 1.0 if schema_valid else 0.0
    score = (
        (1.0 - missing_ratio) * 0.30
        + (1.0 - duplicate_ratio_value) * 0.20
        + schema_component * 0.30
        + freshness_score_value * 0.20
    )
    return round(max(0.0, min(1.0, score)), 4)


def run_quality_checks(
    dataset: str,
    records: Sequence[Record],
    required_columns: Sequence[str],
    primary_keys: Sequence[str],
    freshness_column: str,
    max_age_hours: int,
    json_schema: Mapping[str, object] | None = None,
) -> QualityResult:
    issues: list[str] = []
    missing = missing_value_ratio(records, required_columns)
    duplicates = duplicate_ratio(records, primary_keys)
    required_schema_valid, required_schema_issues = validate_schema(records, required_columns)
    json_schema_valid, json_schema_issues = validate_json_schema(records, json_schema)
    schema_valid = required_schema_valid and json_schema_valid
    freshness = freshness_score(records, freshness_column, max_age_hours)
    readiness = readiness_score(missing, duplicates, schema_valid, freshness)

    issues.extend(required_schema_issues)
    issues.extend(json_schema_issues)
    if missing > 0:
        issues.append(f"Missing value ratio is {missing:.2%}")
    if duplicates > 0:
        issues.append(f"Duplicate ratio is {duplicates:.2%}")
    if freshness < 1:
        issues.append(f"Freshness score is {freshness:.2%}")

    return QualityResult(
        dataset=dataset,
        record_count=len(records),
        missing_ratio=round(missing, 4),
        duplicate_ratio=round(duplicates, 4),
        schema_valid=schema_valid,
        freshness_score=round(freshness, 4),
        readiness_score=readiness,
        issues=issues,
    )


def evaluate_quality_status(
    result: QualityResult,
    thresholds: Mapping[str, object] | None = None,
) -> tuple[str, list[str]]:
    """Evaluate all configured thresholds, not just readiness."""
    config = dict(thresholds or {})
    max_missing_ratio = float(config.get("max_missing_ratio", 1.0))
    max_duplicate_ratio = float(config.get("max_duplicate_ratio", 1.0))
    min_freshness_score = float(config.get("min_freshness_score", 0.0))
    min_readiness_score = float(config.get("min_readiness_score", 0.0))

    violations: list[str] = []
    if not result.schema_valid:
        violations.append("Schema validation failed.")
    if result.missing_ratio > max_missing_ratio:
        violations.append(
            f"Missing ratio {result.missing_ratio:.2%} exceeds max {max_missing_ratio:.2%}."
        )
    if result.duplicate_ratio > max_duplicate_ratio:
        violations.append(
            f"Duplicate ratio {result.duplicate_ratio:.2%} exceeds max {max_duplicate_ratio:.2%}."
        )
    if result.freshness_score < min_freshness_score:
        violations.append(
            f"Freshness score {result.freshness_score:.2%} is below min {min_freshness_score:.2%}."
        )
    if result.readiness_score < min_readiness_score:
        violations.append(
            f"Readiness score {result.readiness_score:.2%} is below min {min_readiness_score:.2%}."
        )
    return ("failed" if violations else "passed", violations)


def detect_anomalies(metric_history: Iterable[float], threshold: float = 0.20) -> list[str]:
    """Flag large readiness-score drops in a sequence of quality metrics."""
    values = list(metric_history)
    anomalies: list[str] = []
    for index in range(1, len(values)):
        drop = values[index - 1] - values[index]
        if drop >= threshold:
            anomalies.append(f"Quality score dropped by {drop:.2f} at index {index}")
    return anomalies
