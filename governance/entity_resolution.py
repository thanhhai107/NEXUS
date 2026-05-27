from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from math import isfinite
from typing import Any, Mapping, Sequence

from common.config import load_dataset_catalog
from common.semantic import load_semantic_contract

Record = Mapping[str, object]


@dataclass(frozen=True)
class EntityResolutionResult:
    dataset: str
    entity_type: str
    record_count: int
    matched_records: list[dict[str, Any]]
    crosswalk: list[dict[str, Any]]
    summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "entity_type": self.entity_type,
            "record_count": self.record_count,
            "matched_records": self.matched_records,
            "crosswalk": self.crosswalk,
            "summary": self.summary,
        }


def resolve_entities(
    dataset: str,
    records: Sequence[Record],
    *,
    fuzzy_threshold: float = 0.88,
    probabilistic_threshold: float = 0.82,
) -> EntityResolutionResult:
    """Resolve records into canonical entities using configured semantic rules."""
    contract = load_semantic_contract(dataset).to_dict()
    rules = dict(contract.get("dataset_rules") or {})
    matching = dict(rules.get("entity_matching") or {})
    grain = dict(rules.get("grain") or {})
    entity_type = str(matching.get("entity_type") or dataset)
    id_fields = _field_list(matching.get("source_id_fields")) or _field_list(grain.get("entity_grain"))
    candidate_fields = _field_list(matching.get("candidate_fields"))
    methods = set(_field_list(matching.get("methods")) or ["exact", "rule_based", "fuzzy", "probabilistic"])
    canonical_field = str(matching.get("canonical_entity_id_field") or "canonical_entity_id")

    clusters: list[dict[str, Any]] = []
    matched_records: list[dict[str, Any]] = []
    crosswalk_index: dict[tuple[str, str], dict[str, Any]] = {}
    match_method_counts: dict[str, int] = {}

    for record in records:
        resolved = _resolve_record(
            entity_type=entity_type,
            record=record,
            id_fields=id_fields,
            candidate_fields=candidate_fields,
            methods=methods,
            clusters=clusters,
            fuzzy_threshold=fuzzy_threshold,
            probabilistic_threshold=probabilistic_threshold,
        )
        match_method_counts[resolved["match_method"]] = match_method_counts.get(resolved["match_method"], 0) + 1
        output_record = dict(record)
        output_record[canonical_field] = resolved["canonical_entity_id"]
        output_record["_nexus_entity_match_method"] = resolved["match_method"]
        output_record["_nexus_entity_match_confidence"] = resolved["confidence_score"]
        matched_records.append(output_record)

        source_entity_id = resolved["source_entity_id"]
        crosswalk_key = (source_entity_id, resolved["canonical_entity_id"])
        if crosswalk_key not in crosswalk_index:
            crosswalk_index[crosswalk_key] = _crosswalk_entry(
                dataset=dataset,
                entity_type=entity_type,
                source_entity_id=source_entity_id,
                canonical_entity_id=resolved["canonical_entity_id"],
                match_method=resolved["match_method"],
                confidence_score=resolved["confidence_score"],
                matched_fields=resolved["matched_fields"],
            )

    crosswalk = list(crosswalk_index.values())
    return EntityResolutionResult(
        dataset=dataset,
        entity_type=entity_type,
        record_count=len(records),
        matched_records=matched_records,
        crosswalk=crosswalk,
        summary={
            "canonical_entity_count": len({row["canonical_entity_id"] for row in crosswalk}),
            "crosswalk_count": len(crosswalk),
            "match_method_counts": match_method_counts,
            "configured_methods": sorted(methods),
            "deferred_methods": sorted(methods & {"splink", "llm_assisted_review"}),
        },
    )


def dataset_has_entity_matching(dataset: str) -> bool:
    catalog = load_dataset_catalog().get("datasets", {})
    if dataset not in catalog:
        raise KeyError(f"Unknown dataset: {dataset}")
    rules = load_semantic_contract(dataset).to_dict().get("dataset_rules") or {}
    return bool(rules.get("entity_matching") or rules.get("grain"))


def _resolve_record(
    *,
    entity_type: str,
    record: Record,
    id_fields: list[str],
    candidate_fields: list[str],
    methods: set[str],
    clusters: list[dict[str, Any]],
    fuzzy_threshold: float,
    probabilistic_threshold: float,
) -> dict[str, Any]:
    source_key = _field_signature(record, id_fields)
    if source_key and "exact" in methods:
        canonical_id = _canonical_id(entity_type, source_key)
        _upsert_cluster(clusters, canonical_id, record, candidate_fields)
        return _resolved(canonical_id, source_key, "exact", 1.0, id_fields)

    rule_key = _rule_signature(record, candidate_fields)
    if rule_key and "rule_based" in methods:
        canonical_id = _canonical_id(entity_type, rule_key)
        _upsert_cluster(clusters, canonical_id, record, candidate_fields)
        return _resolved(canonical_id, rule_key, "rule_based", 0.95, candidate_fields)

    best = _best_cluster(record, clusters, candidate_fields)
    if best:
        cluster, score = best
        if "probabilistic" in methods and score >= probabilistic_threshold:
            _upsert_cluster(clusters, cluster["canonical_entity_id"], record, candidate_fields)
            return _resolved(
                cluster["canonical_entity_id"],
                _fallback_source_id(record, candidate_fields),
                "probabilistic",
                score,
                candidate_fields,
            )
        if "fuzzy" in methods and score >= fuzzy_threshold:
            _upsert_cluster(clusters, cluster["canonical_entity_id"], record, candidate_fields)
            return _resolved(
                cluster["canonical_entity_id"],
                _fallback_source_id(record, candidate_fields),
                "fuzzy",
                score,
                candidate_fields,
            )

    fallback = _fallback_source_id(record, candidate_fields)
    canonical_id = _canonical_id(entity_type, fallback)
    _upsert_cluster(clusters, canonical_id, record, candidate_fields)
    return _resolved(canonical_id, fallback, "new_entity", 0.5, candidate_fields)


def _resolved(
    canonical_id: str,
    source_entity_id: str,
    match_method: str,
    confidence: float,
    matched_fields: list[str],
) -> dict[str, Any]:
    return {
        "canonical_entity_id": canonical_id,
        "source_entity_id": source_entity_id,
        "match_method": match_method,
        "confidence_score": round(max(0.0, min(1.0, confidence)), 4),
        "matched_fields": matched_fields,
    }


def _crosswalk_entry(
    *,
    dataset: str,
    entity_type: str,
    source_entity_id: str,
    canonical_entity_id: str,
    match_method: str,
    confidence_score: float,
    matched_fields: list[str],
) -> dict[str, Any]:
    return {
        "source_system": dataset,
        "entity_type": entity_type,
        "source_entity_id": source_entity_id,
        "canonical_entity_id": canonical_entity_id,
        "match_method": match_method,
        "confidence_score": confidence_score,
        "matched_fields": matched_fields,
        "valid_from": datetime.now(timezone.utc).isoformat(),
        "valid_to": None,
    }


def _field_list(value: object) -> list[str]:
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item)]
    if value:
        return [str(value)]
    return []


def _field_signature(record: Record, fields: list[str]) -> str:
    values = [(field, record.get(field)) for field in fields if _has_value(record.get(field))]
    if not values:
        return ""
    return "|".join(f"{field}={_normalize(value)}" for field, value in values)


def _rule_signature(record: Record, candidate_fields: list[str]) -> str:
    text_values = []
    geo_values = []
    for field in candidate_fields:
        value = record.get(field)
        if not _has_value(value):
            continue
        if field.lower() in {"latitude", "longitude", "lat", "lon", "lng"}:
            geo_values.append(f"{field}={_rounded_float(value, 4)}")
        else:
            text_values.append(f"{field}={_normalize(value)}")
    if text_values or geo_values:
        return "|".join(text_values + geo_values)
    return ""


def _fallback_source_id(record: Record, candidate_fields: list[str]) -> str:
    configured = _rule_signature(record, candidate_fields)
    if configured:
        return configured
    stable = "|".join(f"{key}={_normalize(value)}" for key, value in sorted(record.items()) if _has_value(value))
    return stable or "unknown"


def _upsert_cluster(
    clusters: list[dict[str, Any]],
    canonical_id: str,
    record: Record,
    candidate_fields: list[str],
) -> None:
    for cluster in clusters:
        if cluster["canonical_entity_id"] == canonical_id:
            cluster["records"].append(record)
            return
    clusters.append({
        "canonical_entity_id": canonical_id,
        "records": [record],
        "text": _candidate_text(record, candidate_fields),
        "geo": _candidate_geo(record),
    })


def _best_cluster(
    record: Record,
    clusters: list[dict[str, Any]],
    candidate_fields: list[str],
) -> tuple[dict[str, Any], float] | None:
    if not clusters:
        return None
    text = _candidate_text(record, candidate_fields)
    geo = _candidate_geo(record)
    best_cluster: dict[str, Any] | None = None
    best_score = 0.0
    for cluster in clusters:
        text_score = SequenceMatcher(None, text, str(cluster.get("text") or "")).ratio() if text else 0.0
        geo_score = _geo_score(geo, cluster.get("geo"))
        score = (text_score * 0.75) + (geo_score * 0.25 if geo_score is not None else 0)
        if score > best_score:
            best_cluster = cluster
            best_score = score
    if best_cluster is None:
        return None
    return best_cluster, round(best_score, 4)


def _candidate_text(record: Record, candidate_fields: list[str]) -> str:
    values = [
        _normalize(record.get(field))
        for field in candidate_fields
        if field.lower() not in {"latitude", "longitude", "lat", "lon", "lng"}
        and _has_value(record.get(field))
    ]
    return " ".join(values)


def _candidate_geo(record: Record) -> tuple[float, float] | None:
    lat = record.get("latitude") or record.get("lat")
    lon = record.get("longitude") or record.get("lng") or record.get("lon")
    try:
        lat_float = float(lat)
        lon_float = float(lon)
    except (TypeError, ValueError):
        return None
    if not (isfinite(lat_float) and isfinite(lon_float)):
        return None
    return lat_float, lon_float


def _geo_score(left: tuple[float, float] | None, right: object) -> float | None:
    if not left or not isinstance(right, tuple):
        return None
    distance = abs(left[0] - right[0]) + abs(left[1] - right[1])
    if distance <= 0.0005:
        return 1.0
    if distance <= 0.005:
        return 0.9
    if distance <= 0.05:
        return 0.7
    return 0.0


def _normalize(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _rounded_float(value: object, digits: int) -> str:
    try:
        return str(round(float(value), digits))
    except (TypeError, ValueError):
        return _normalize(value)


def _has_value(value: object) -> bool:
    return value not in (None, "")


def _canonical_id(entity_type: str, source_key: str) -> str:
    digest = hashlib.sha256(f"{entity_type}|{source_key}".encode("utf-8")).hexdigest()[:16]
    return f"ceid_{digest}"


__all__ = [
    "EntityResolutionResult",
    "dataset_has_entity_matching",
    "resolve_entities",
]
