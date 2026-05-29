from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Mapping, Sequence

Record = Mapping[str, object]


@dataclass(frozen=True)
class SchemaDriftIssue:
    issue_code: str
    field_name: str
    severity: str
    action: str
    expected_type: list[str] = field(default_factory=list)
    actual_type: list[str] = field(default_factory=list)
    candidate_field: str | None = None
    confidence: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "issue_category": "schema_drift",
            "issue_code": self.issue_code,
            "field_name": self.field_name,
            "severity": self.severity,
            "action": self.action,
            "expected_type": self.expected_type,
            "actual_type": self.actual_type,
            "candidate_field": self.candidate_field,
            "confidence": self.confidence,
        }


@dataclass(frozen=True)
class SchemaDriftResult:
    status: str
    should_quarantine: bool
    actual_fields: list[str]
    expected_fields: list[str]
    missing_fields: list[str]
    new_fields: list[str]
    type_changes: list[dict[str, Any]]
    rename_candidates: list[dict[str, Any]]
    issues: list[SchemaDriftIssue]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "should_quarantine": self.should_quarantine,
            "actual_fields": self.actual_fields,
            "expected_fields": self.expected_fields,
            "missing_fields": self.missing_fields,
            "new_fields": self.new_fields,
            "type_changes": self.type_changes,
            "rename_candidates": self.rename_candidates,
            "issues": [issue.to_dict() for issue in self.issues],
        }


def compare_schema_drift(
    expected_schema: Mapping[str, Any] | None,
    records: Sequence[Record],
    *,
    required_fields: Sequence[str] | None = None,
    primary_keys: Sequence[str] | None = None,
    downstream_fields: Sequence[str] | None = None,
    alias_map: Mapping[str, str] | None = None,
    rename_threshold: float = 0.82,
) -> SchemaDriftResult:
    """Compare observed records against the expected JSON Schema and policy hints."""
    schema = dict(expected_schema or {})
    properties = dict(schema.get("properties") or {})
    expected_fields = sorted(properties)
    actual_fields = sorted({key for record in records for key in record})
    required = set(required_fields or schema.get("required") or [])
    key_fields = set(primary_keys or [])
    downstream = set(downstream_fields or [])
    aliases = dict(alias_map or {})

    missing_fields = sorted(set(expected_fields) - set(actual_fields))
    new_fields = sorted(set(actual_fields) - set(expected_fields))
    rename_candidates = _rename_candidates(missing_fields, new_fields, aliases, rename_threshold)
    renamed_missing = {candidate["field_name"] for candidate in rename_candidates}
    type_changes = _type_changes(properties, records)

    issues: list[SchemaDriftIssue] = []
    for field_name in missing_fields:
        critical = field_name in required or field_name in key_fields
        used_downstream = field_name in downstream
        if field_name in renamed_missing:
            action = "review_alias_mapping"
            severity = "medium"
            issue_code = "field_rename_candidate"
        elif critical:
            action = "quarantine_record"
            severity = "critical"
            issue_code = "missing_required_field"
        elif used_downstream:
            action = "block_downstream_publication"
            severity = "high"
            issue_code = "dropped_downstream_field"
        else:
            action = "fill_null_or_default_in_silver"
            severity = "medium"
            issue_code = "missing_optional_field"
        candidate = next(
            (item for item in rename_candidates if item["field_name"] == field_name),
            {},
        )
        issues.append(
            SchemaDriftIssue(
                issue_code=issue_code,
                field_name=field_name,
                severity=severity,
                action=action,
                candidate_field=candidate.get("candidate_field"),
                confidence=candidate.get("confidence"),
            )
        )

    for field_name in new_fields:
        issues.append(
            SchemaDriftIssue(
                issue_code="new_unknown_field",
                field_name=field_name,
                severity="low",
                action="preserve_in_bronze_and_review_before_promotion",
                actual_type=sorted(_observed_types(records, field_name)),
            )
        )

    for change in type_changes:
        issues.append(
            SchemaDriftIssue(
                issue_code="field_type_change",
                field_name=change["field_name"],
                severity="high" if change["incompatible"] else "medium",
                action="quarantine_record" if change["incompatible"] else "safe_cast_in_silver",
                expected_type=change["expected_type"],
                actual_type=change["actual_type"],
            )
        )

    should_quarantine = any(issue.severity == "critical" or issue.action == "quarantine_record" for issue in issues)
    status = "failed" if should_quarantine else "warning" if issues else "passed"
    return SchemaDriftResult(
        status=status,
        should_quarantine=should_quarantine,
        actual_fields=actual_fields,
        expected_fields=expected_fields,
        missing_fields=missing_fields,
        new_fields=new_fields,
        type_changes=type_changes,
        rename_candidates=rename_candidates,
        issues=issues,
    )


def _rename_candidates(
    missing_fields: Sequence[str],
    new_fields: Sequence[str],
    alias_map: Mapping[str, str],
    threshold: float,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for missing in missing_fields:
        alias = alias_map.get(missing)
        if alias in new_fields:
            candidates.append({
                "field_name": missing,
                "candidate_field": alias,
                "confidence": 1.0,
                "match_type": "configured_alias",
            })
            continue
        scored = [
            (SequenceMatcher(None, missing.lower(), new.lower()).ratio(), new)
            for new in new_fields
        ]
        if not scored:
            continue
        confidence, candidate = max(scored, key=lambda item: item[0])
        if confidence >= threshold:
            candidates.append({
                "field_name": missing,
                "candidate_field": candidate,
                "confidence": round(confidence, 4),
                "match_type": "name_similarity",
            })
    return candidates


def _type_changes(properties: Mapping[str, Any], records: Sequence[Record]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for field_name, definition in properties.items():
        actual_types = _observed_types(records, field_name)
        if not actual_types:
            continue
        expected_types = _json_type_set(definition.get("type"))
        comparable_expected = expected_types - {"null"}
        comparable_actual = actual_types - {"null"}
        if comparable_expected and comparable_actual and comparable_actual.isdisjoint(comparable_expected):
            compatible = _safe_cast_possible(comparable_expected, comparable_actual)
            changes.append({
                "field_name": field_name,
                "expected_type": sorted(expected_types),
                "actual_type": sorted(actual_types),
                "incompatible": not compatible,
            })
    return changes


def _observed_types(records: Sequence[Record], field_name: str) -> set[str]:
    observed: set[str] = set()
    for record in records:
        if field_name in record:
            observed.add(_json_type(record.get(field_name)))
    return observed


def _json_type_set(raw_type: object) -> set[str]:
    if isinstance(raw_type, str):
        return {raw_type}
    if isinstance(raw_type, list):
        return {str(item) for item in raw_type}
    return set()


def _json_type(value: object) -> str:
    if value is None or value == "":
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, Mapping):
        return "object"
    if isinstance(value, list):
        return "array"
    return "string"


def _safe_cast_possible(expected: set[str], actual: set[str]) -> bool:
    numeric = {"integer", "number"}
    if expected <= numeric and actual <= numeric:
        return True
    if actual == {"string"} and expected & {"integer", "number", "boolean", "string"}:
        return True
    return False


__all__ = [
    "SchemaDriftIssue",
    "SchemaDriftResult",
    "compare_schema_drift",
]
