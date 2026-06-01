"""Heterogeneity injector (H1-H5).

Injectors:
    H1 - different_formats: emit same data in multiple formats
    H2 - different_data_models: relational vs document
    H3 - different_protocols: file vs REST vs Kafka (protocol metadata only)
    H4 - different_frequencies: vary update cadence metadata
    H5 - different_ingestion_modes: batch vs streaming mode metadata
"""

from __future__ import annotations

import copy
import json
import random
from typing import Any


class HeterogeneityInjector:
    """Inject source heterogeneity variations."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        random.seed(seed)

    def inject_different_formats(
        self,
        records: list[dict[str, Any]],
        format: str = "parquet",
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        return copy.deepcopy(records)

    def inject_different_data_models(
        self,
        records: list[dict[str, Any]],
        model: str = "document",
        nesting_keys: list[str] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        if model != "document" or not nesting_keys:
            return copy.deepcopy(records)
        result = copy.deepcopy(records)
        for record in result:
            nested: dict[str, Any] = {}
            for key in nesting_keys:
                if key in record:
                    nested[key] = record.pop(key)
            if nested:
                record["_nested"] = json.dumps(nested, default=str)
        return result

    def inject_different_protocols(
        self,
        records: list[dict[str, Any]],
        protocol: str = "rest_api",
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        return copy.deepcopy(records)

    def inject_different_frequencies(
        self,
        records: list[dict[str, Any]],
        frequency_hours: int = 24,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        return copy.deepcopy(records)

    def inject_different_ingestion_modes(
        self,
        records: list[dict[str, Any]],
        mode: str = "streaming",
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        return copy.deepcopy(records)
