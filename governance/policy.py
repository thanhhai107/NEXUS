from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from common.config import load_dataset_catalog
from governance.metadata import dataset_governance_metadata

SURFACE_FLAGS = {
    "api": "api_access",
    "trino": "trino_access",
    "superset": "superset_access",
}


@dataclass(frozen=True)
class AccessDecision:
    allowed: bool
    reason: str
    role: str
    dataset: str
    surface: str


def normalize_role(role: str | None) -> str:
    return (role or "public").strip().lower() or "public"


def evaluate_dataset_access(dataset_name: str, role: str | None, surface: str = "api") -> AccessDecision:
    normalized_role = normalize_role(role)
    datasets = load_dataset_catalog().get("datasets", {})
    if dataset_name not in datasets:
        return AccessDecision(False, "Unknown dataset.", normalized_role, dataset_name, surface)

    governance = dataset_governance_metadata(dataset_name)
    policy: Mapping[str, Any] = governance.get("access_policy") or {}
    surface_flag = SURFACE_FLAGS.get(surface, f"{surface}_access")

    if not bool(policy.get(surface_flag, True)):
        return AccessDecision(False, f"{surface} access is disabled.", normalized_role, dataset_name, surface)

    denied_roles = {str(item).lower() for item in policy.get("denied_roles") or []}
    if normalized_role in denied_roles:
        return AccessDecision(False, "Role is explicitly denied.", normalized_role, dataset_name, surface)

    allowed_roles = {str(item).lower() for item in policy.get("allowed_roles") or []}
    if "*" in allowed_roles or normalized_role in allowed_roles:
        return AccessDecision(True, "Role is allowed.", normalized_role, dataset_name, surface)

    return AccessDecision(False, "Role is not allowed by dataset policy.", normalized_role, dataset_name, surface)


def filter_datasets_for_role(
    datasets: Mapping[str, Any],
    role: str | None,
    surface: str = "api",
) -> dict[str, Any]:
    return {
        name: metadata
        for name, metadata in datasets.items()
        if evaluate_dataset_access(name, role, surface).allowed
    }
