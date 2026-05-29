from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.regenerate_sample_datasets import SAMPLE_SPECS, SOURCE_DISCOVERY_CATALOG

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_source_discovery_backed_sample_specs_reference_existing_schemas() -> None:
    catalog = json.loads(SOURCE_DISCOVERY_CATALOG.read_text(encoding="utf-8"))
    schemas = catalog.get("schemas", {})

    missing = [
        spec.source_discovery_schema
        for spec in SAMPLE_SPECS
        if spec.source_discovery_schema and spec.source_discovery_schema not in schemas
    ]

    assert missing == []


def test_regenerated_sample_files_have_expected_rows_and_required_columns() -> None:
    for spec in SAMPLE_SPECS:
        path = PROJECT_ROOT / "assets" / "samples" / spec.output
        schema = json.loads((PROJECT_ROOT / spec.schema_path).read_text(encoding="utf-8"))
        required = set(schema.get("required") or [])

        with path.open("r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            rows = list(reader)

        assert reader.fieldnames is not None, spec.output
        assert required.issubset(set(reader.fieldnames)), spec.output
        assert len(rows) == spec.row_count, spec.output
        for row in rows:
            for column in required:
                assert row.get(column) not in (None, ""), f"{spec.output}:{column}"
