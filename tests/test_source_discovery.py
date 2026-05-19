from __future__ import annotations

import json

from common.source_discovery import (
    load_schema_definition,
    schema_filename,
    source_summary,
    sync_discovery,
    to_nexus_json_schema,
)


def test_source_summary_reads_discovery_catalog(tmp_path) -> None:
    source_dir = make_source_discovery_dir(tmp_path)

    summary = source_summary(source_dir)

    assert summary["repository"] == "https://github.com/Akapi895/data-bigdata"
    assert summary["source_count"] == 1
    assert summary["schema_count"] == 1
    assert summary["sources"][0]["name"] == "DemoSource"


def test_schema_conversion_preserves_types_and_refs(tmp_path) -> None:
    source_dir = make_source_discovery_dir(tmp_path)
    definition = load_schema_definition("DemoSource_DemoRecord", source_dir)

    schema = to_nexus_json_schema("DemoSource_DemoRecord", definition)

    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["required"] == ["id", "observed_at"]
    assert schema["properties"]["id"]["type"] == "integer"
    assert schema["properties"]["observed_at"]["format"] == "date-time"
    assert schema["properties"]["station"]["type"] == ["object", "null"]
    assert schema["properties"]["station"]["x-source-discovery-ref"] == "DemoSource_Station"


def test_sync_discovery_writes_runtime_files(tmp_path) -> None:
    source_dir = make_source_discovery_dir(tmp_path)
    output_dir = tmp_path / "runtime" / "source_discovery"

    result = sync_discovery(
        source_dir=source_dir,
        output_dir=output_dir,
        selected_schemas=["DemoSource_DemoRecord"],
    )

    assert result["schemas_written"] == 1
    assert (output_dir / "sources.json").exists()
    assert (output_dir / "endpoint_verification_report.json").exists()

    schema_path = output_dir / "schemas" / "DemoSource_DemoRecord.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    assert schema["x-source-discovery-schema"] == "DemoSource_DemoRecord"


def test_schema_filename_is_safe() -> None:
    assert schema_filename("TfL Unified API/Foo:Bar") == "TfL_Unified_API_Foo_Bar"


def make_source_discovery_dir(tmp_path):
    source_dir = tmp_path / "source_discovery"
    source_dir.mkdir(parents=True)

    schema_definition = {
        "type": "object",
        "description": "Demo record",
        "required_fields": ["id", "observed_at"],
        "properties": {
            "id": {"type": "integer", "nullable": False},
            "observed_at": {"type": "string", "format": "date-time"},
            "station": {"type": "DemoSource_Station", "nullable": True},
        },
    }
    (source_dir / "all_schemas.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-05-14T23:45:02",
                "sources": [
                    {
                        "name": "DemoSource",
                        "base_url": "https://example.test",
                        "type": "rest",
                        "version": "v1",
                        "endpoints_count": 1,
                        "schemas_count": 1,
                    }
                ],
                "schemas": {"DemoSource_DemoRecord": schema_definition},
            }
        ),
        encoding="utf-8",
    )
    (source_dir / "endpoint_verification_report.json").write_text(
        json.dumps({"summary": {"passed": 1, "failed": 0}, "results": []}),
        encoding="utf-8",
    )
    return source_dir
