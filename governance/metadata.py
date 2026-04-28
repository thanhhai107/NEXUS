from __future__ import annotations

import copy
from typing import Any, Mapping

from common.config import load_dataset_catalog, load_governance_defaults


def dataset_governance_metadata(dataset_name: str) -> dict[str, Any]:
    datasets = load_dataset_catalog().get("datasets", {})
    dataset = datasets.get(dataset_name, {})
    defaults = load_governance_defaults()
    metadata = _deep_merge(defaults, dataset.get("governance") or {})
    metadata.setdefault("owner", "data-platform")
    metadata.setdefault("steward", "data-steward")
    metadata.setdefault("sensitivity", "public")
    metadata.setdefault("retention", {"policy": "open-data-standard", "days": 365})
    metadata.setdefault("access_policy", {})
    policy = metadata["access_policy"]
    policy.setdefault("allowed_roles", ["admin", "steward", "analyst", "public"])
    policy.setdefault("denied_roles", [])
    policy.setdefault("api_access", True)
    policy.setdefault("trino_access", True)
    policy.setdefault("superset_access", True)
    return metadata


def all_dataset_governance_metadata() -> dict[str, dict[str, Any]]:
    return {
        dataset_name: dataset_governance_metadata(dataset_name)
        for dataset_name in load_dataset_catalog().get("datasets", {})
    }


def _deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    output = copy.deepcopy(dict(base))
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(output.get(key), Mapping):
            output[key] = _deep_merge(output[key], value)
        else:
            output[key] = copy.deepcopy(value)
    return output
