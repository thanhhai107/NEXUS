from __future__ import annotations

from common.source_coverage import build_ingestion_coverage_map, write_ingestion_coverage_map


def test_build_ingestion_coverage_map_covers_discovery_catalog(tmp_path) -> None:
    coverage = build_ingestion_coverage_map()
    summary = coverage["summary"]

    assert summary["source_count"] == 14
    assert summary["discovered_endpoint_count"] == 174
    assert summary["mapped_endpoint_count"] == 174
    assert summary["schema_count"] == 224
    assert summary["mapped_schema_count"] == 224
    assert "tfl_transport_status" in summary["existing_datasets"]
    assert "tfl_air_quality" in summary["planned_datasets"]

    waqi = next(source for source in coverage["sources"] if source["source"] == "WAQI")
    assert waqi["connector"]["type"] == "api_stream"
    assert waqi["auth"]["query_param"] == "token"
    assert waqi["airflow"]["dag_id"] == "nexus_streaming_pipeline"

    existing_endpoint = next(endpoint for endpoint in waqi["endpoints"] if endpoint["path"] == "/feed/{station}/")
    assert existing_endpoint["dataset"] == "waqi_air_quality"
    assert existing_endpoint["dataset_status"] == "existing"
    assert existing_endpoint["verification"]["status"] == "passed"
    assert existing_endpoint["quality_rules"]["status"] == "configured"
    assert existing_endpoint["schema_mapping"]["target_schema_status"] == "existing"

    planned_endpoint = next(endpoint for endpoint in waqi["endpoints"] if endpoint["path"] == "/search/")
    assert planned_endpoint["dataset"] == "waqi_station_search"
    assert planned_endpoint["dataset_status"] == "planned"
    assert planned_endpoint["quality_rules"]["status"] == "planned_from_source_schema"
    assert planned_endpoint["airflow"]["status"] == "planned"


def test_write_ingestion_coverage_map_writes_file(tmp_path) -> None:
    output_path = tmp_path / "ingestion_coverage_map.json"

    result = write_ingestion_coverage_map(output_path)

    assert output_path.exists()
    assert result["output_path"] == str(output_path)
    assert result["summary"]["mapped_schema_count"] == 224
