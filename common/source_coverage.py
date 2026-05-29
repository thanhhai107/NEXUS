from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable

from common.config import CONFIG_DIR, DOMAINS_DIR, load_dataset_catalog, load_quality_config
from common.source_discovery import (
    DEFAULT_SOURCE_DIR,
    SOURCE_REPOSITORY_URL,
    load_discovery_catalog,
)

COVERAGE_MAP_FILE = "ingestion_coverage_map.json"

SOURCE_PREFIXES = {
    "TfL Unified API": "TfL_Unified_API",
}

REPORT_SOURCE_ALIASES = {
    "DfTRoadTraffic": "DfT_Road_Traffic",
    "LondonDatastore": "London_Datastore",
    "NaPTAN": "NaPTAN_NPTG",
    "OpenMeteo_AirQuality": "OpenMeteo",
    "TfL": "TfL Unified API",
    "TfL_GIS": "TfL_GIS_Hub",
    "TfL_OpenData": "TfL_Open_Data",
}

SOURCE_POLICIES: dict[str, dict[str, Any]] = {
    "DfT_Road_Traffic": {
        "domain": "transport",
        "default_dataset": "dft_road_traffic",
        "connector_type": "rest_api",
        "auth": {"mode": "none", "env_vars": []},
        "pagination": {"mode": "json_api_links", "params": ["page[size]"], "max_pages_default": 5},
        "rate_limit": {"requests_per_minute": 60, "backoff": "exponential_on_429"},
        "backfill_policy": {"mode": "annual_snapshot", "schedule": "@monthly", "cursor_field": "year"},
    },
    "LondonAir": {
        "domain": "environment",
        "default_dataset": "londonair_monitoring",
        "connector_type": "api_stream",
        "auth": {"mode": "optional_key", "env_vars": ["LONDONAIR_API_KEY"]},
        "pagination": {"mode": "none"},
        "rate_limit": {"requests_per_minute": 30, "backoff": "fixed_then_exponential"},
        "backfill_policy": {"mode": "latest_snapshot", "schedule": "hourly", "cursor_field": "event_time"},
    },
    "London_Datastore": {
        "domain": "transport",
        "default_dataset": "london_journeys",
        "connector_type": "data_portal",
        "auth": {"mode": "none", "env_vars": []},
        "pagination": {"mode": "file_download"},
        "rate_limit": {"requests_per_minute": 20, "backoff": "exponential_on_429"},
        "backfill_policy": {"mode": "full_refresh", "schedule": "@monthly", "cursor_field": "period_beginning"},
    },
    "NaPTAN_NPTG": {
        "domain": "transport",
        "default_dataset": "naptan_stops",
        "connector_type": "csv_download",
        "auth": {"mode": "none", "env_vars": []},
        "pagination": {"mode": "file_download"},
        "rate_limit": {"requests_per_minute": 20, "backoff": "exponential_on_429"},
        "backfill_policy": {"mode": "full_refresh", "schedule": "@monthly", "cursor_field": "modificationdatetime"},
    },
    "NCEI": {
        "domain": "environment",
        "default_dataset": "ncei_cdo_climate",
        "connector_type": "rest_api",
        "auth": {"mode": "header_token", "env_vars": ["NCEI_API_TOKEN"], "header": "token"},
        "pagination": {"mode": "limit_offset", "params": ["limit", "offset"], "max_pages_default": 5},
        "rate_limit": {"requests_per_minute": 60, "backoff": "exponential_on_429"},
        "backfill_policy": {"mode": "date_window", "schedule": "@daily", "cursor_field": "date"},
    },
    "OpenAQ": {
        "domain": "environment",
        "default_dataset": "openaq_measurements",
        "connector_type": "api_stream",
        "auth": {"mode": "api_key_header", "env_vars": ["OPENAQ_API_KEY"], "header": "X-API-Key"},
        "pagination": {"mode": "page_limit_offset", "params": ["page", "limit", "offset"], "max_pages_default": 5},
        "rate_limit": {"requests_per_minute": 60, "backoff": "exponential_on_429"},
        "backfill_policy": {"mode": "date_window", "schedule": "hourly", "cursor_field": "datetime"},
    },
    "OpenMeteo": {
        "domain": "environment",
        "default_dataset": "openmeteo_air_quality",
        "connector_type": "api_stream",
        "auth": {"mode": "none", "env_vars": []},
        "pagination": {"mode": "time_window_params", "params": ["start_date", "end_date"]},
        "rate_limit": {"requests_per_minute": 60, "backoff": "exponential_on_429"},
        "backfill_policy": {"mode": "date_window", "schedule": "hourly", "cursor_field": "time"},
    },
    "OpenWeather": {
        "domain": "environment",
        "default_dataset": "openweather_current",
        "connector_type": "api_stream",
        "auth": {"mode": "query_api_key", "env_vars": ["OPENWEATHER_API_KEY"], "query_param": "appid"},
        "pagination": {"mode": "time_window_params", "params": ["dt", "start", "end"]},
        "rate_limit": {"requests_per_minute": 60, "backoff": "exponential_on_429"},
        "backfill_policy": {"mode": "latest_snapshot", "schedule": "30min", "cursor_field": "event_time"},
    },
    # NOTE: Deprecated in favor of Overture Maps transportation (GeoParquet, no API key needed).
    "OS_Open_Roads": {
        "domain": "transport",
        "default_dataset": "os_open_roads",
        "connector_type": "geospatial_download",
        "auth": {"mode": "api_key", "env_vars": ["OS_DATAHUB_API_KEY"]},
        "pagination": {"mode": "file_or_tile_download"},
        "rate_limit": {"requests_per_minute": 20, "backoff": "exponential_on_429"},
        "backfill_policy": {"mode": "full_refresh", "schedule": "@monthly", "cursor_field": None},
    },
    "STATS19": {
        "domain": "transport",
        "default_dataset": "stats19_collisions",
        "connector_type": "csv_download",
        "auth": {"mode": "none", "env_vars": []},
        "pagination": {"mode": "file_download"},
        "rate_limit": {"requests_per_minute": 20, "backoff": "exponential_on_429"},
        "backfill_policy": {"mode": "full_refresh", "schedule": "@monthly", "cursor_field": "date"},
    },
    "TfL_GIS_Hub": {
        "domain": "transport",
        "default_dataset": "tfl_gis_assets",
        "connector_type": "arcgis_hub",
        "auth": {"mode": "none", "env_vars": []},
        "pagination": {"mode": "arcgis_result_offset", "params": ["resultOffset", "resultRecordCount"]},
        "rate_limit": {"requests_per_minute": 60, "backoff": "exponential_on_429"},
        "backfill_policy": {"mode": "full_refresh", "schedule": "@monthly", "cursor_field": None},
    },
    "TfL_Open_Data": {
        "domain": "transport",
        "default_dataset": "tfl_open_data_static",
        "connector_type": "static_file",
        "auth": {"mode": "none", "env_vars": []},
        "pagination": {"mode": "file_download"},
        "rate_limit": {"requests_per_minute": 20, "backoff": "exponential_on_429"},
        "backfill_policy": {"mode": "full_refresh", "schedule": "@monthly", "cursor_field": None},
    },
    "TfL Unified API": {
        "domain": "transport",
        "default_dataset": "tfl_transport_status",
        "connector_type": "openapi_rest",
        "auth": {"mode": "query_api_key", "env_vars": ["TFL_API_KEY"], "query_param": "app_key"},
        "pagination": {"mode": "endpoint_specific", "params": ["page", "from", "to"]},
        "rate_limit": {"requests_per_minute": 120, "backoff": "exponential_on_429"},
        "backfill_policy": {"mode": "endpoint_specific", "schedule": "minute_or_daily", "cursor_field": "event_time"},
    },
    "WAQI": {
        "domain": "environment",
        "default_dataset": "waqi_air_quality",
        "connector_type": "api_stream",
        "auth": {"mode": "query_token", "env_vars": ["WAQI_API_TOKEN"], "query_param": "token"},
        "pagination": {"mode": "bounds_or_station_params", "params": ["latlng", "station", "keyword"]},
        "rate_limit": {"requests_per_minute": 30, "backoff": "exponential_on_429"},
        "backfill_policy": {"mode": "latest_snapshot", "schedule": "hourly", "cursor_field": "observed_at"},
    },
}

STREAM_SOURCE_KEYS = {
    "LondonAir": "londonair",
    "OpenAQ": "openaq",
    "OpenMeteo": "openmeteo",
    "OpenWeather": "openweather",
    "TfL Unified API": "tfl",
    "WAQI": "waqi",
}


def build_ingestion_coverage_map(
    source_dir: Path = DEFAULT_SOURCE_DIR,
    domains_dir: Path = DOMAINS_DIR,
    config_dir: Path = CONFIG_DIR,
) -> dict[str, Any]:
    catalog = load_discovery_catalog(source_dir)
    dataset_catalog = load_dataset_catalog(domains_dir).get("datasets", {})
    quality_config = load_quality_config(domains_dir=domains_dir, config_dir=config_dir)
    quality_rules = quality_config.get("datasets", {})
    default_quality_rules = quality_config.get("default_rules", {})
    endpoint_report = _load_endpoint_report(source_dir)
    report_endpoints = _canonical_report_endpoints(endpoint_report)

    schema_catalog = catalog.get("schemas", {})
    sources: list[dict[str, Any]] = []
    all_endpoint_entries: list[dict[str, Any]] = []
    all_schema_entries: list[dict[str, Any]] = []

    for source in catalog.get("sources", []):
        source_name = str(source.get("name"))
        policy = _source_policy(source_name)
        schema_names = _schema_names_for_source(source_name, schema_catalog)
        endpoints = _endpoint_specs_for_source(source_name, source, source_dir, report_endpoints)
        source_dataset = policy["default_dataset"]
        source_connector = _connector(policy, source_dataset, dataset_catalog)
        source_airflow = _airflow_plan(source_name, source_dataset, source_connector, dataset_catalog)

        endpoint_entries = [
            _endpoint_mapping(
                source=source,
                endpoint=endpoint,
                schema_names=schema_names,
                schema_catalog=schema_catalog,
                policy=policy,
                dataset_catalog=dataset_catalog,
                quality_rules=quality_rules,
                default_quality_rules=default_quality_rules,
            )
            for endpoint in endpoints
        ]
        schema_entries = [
            _schema_mapping(
                source=source,
                schema_name=schema_name,
                schema_definition=schema_catalog.get(schema_name, {}),
                policy=policy,
                dataset_catalog=dataset_catalog,
                quality_rules=quality_rules,
                default_quality_rules=default_quality_rules,
            )
            for schema_name in schema_names
        ]

        all_endpoint_entries.extend(endpoint_entries)
        all_schema_entries.extend(schema_entries)
        sources.append(
            {
                "source": source_name,
                "base_url": source.get("base_url"),
                "source_type": source.get("type"),
                "version": source.get("version"),
                "domain": policy["domain"],
                "default_dataset": source_dataset,
                "discovered_endpoint_count": int(source.get("endpoints_count") or 0),
                "mapped_endpoint_count": len(endpoint_entries),
                "mapped_schema_count": len(schema_entries),
                "connector": source_connector,
                "airflow": source_airflow,
                "auth": policy["auth"],
                "pagination": policy["pagination"],
                "rate_limit": policy["rate_limit"],
                "backfill_policy": policy["backfill_policy"],
                "endpoints": endpoint_entries,
                "schemas": schema_entries,
            }
        )

    return {
        "repository": SOURCE_REPOSITORY_URL,
        "generated_at": catalog.get("generated_at"),
        "source_dir": str(source_dir),
        "summary": _coverage_summary(catalog, sources, all_endpoint_entries, all_schema_entries, dataset_catalog),
        "datasets": _dataset_index(all_endpoint_entries, all_schema_entries, dataset_catalog),
        "sources": sources,
    }


def write_ingestion_coverage_map(
    output_path: Path,
    source_dir: Path = DEFAULT_SOURCE_DIR,
    domains_dir: Path = DOMAINS_DIR,
    config_dir: Path = CONFIG_DIR,
) -> dict[str, Any]:
    coverage = build_ingestion_coverage_map(source_dir, domains_dir, config_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(coverage, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return {
        "output_path": str(output_path),
        "summary": coverage["summary"],
    }


def _source_policy(source_name: str) -> dict[str, Any]:
    policy = SOURCE_POLICIES.get(source_name)
    if policy:
        return dict(policy)

    fallback_dataset = _slug(source_name)
    return {
        "domain": "unknown",
        "default_dataset": fallback_dataset,
        "connector_type": "planned_custom",
        "auth": {"mode": "unknown", "env_vars": []},
        "pagination": {"mode": "unknown"},
        "rate_limit": {"requests_per_minute": 20, "backoff": "exponential_on_429"},
        "backfill_policy": {"mode": "manual", "schedule": None, "cursor_field": None},
    }


def _schema_prefix(source_name: str) -> str:
    return SOURCE_PREFIXES.get(source_name, source_name.replace(" ", "_"))


def _schema_names_for_source(source_name: str, schema_catalog: dict[str, Any]) -> list[str]:
    prefix = _schema_prefix(source_name)
    return sorted(name for name in schema_catalog if name.startswith(f"{prefix}_"))


def _load_endpoint_report(source_dir: Path) -> dict[str, Any]:
    path = source_dir / "endpoint_verification_report.json"
    if not path.exists():
        return {"results": []}
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _canonical_report_endpoints(endpoint_report: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {}
    for result in endpoint_report.get("results", []):
        report_source = str(result.get("source"))
        canonical_source = REPORT_SOURCE_ALIASES.get(report_source, report_source)
        for endpoint in result.get("endpoints", []):
            entry = dict(endpoint)
            entry["report_source"] = report_source
            output.setdefault(canonical_source, []).append(entry)
    return output


def _source_schema_file(source_name: str, source_dir: Path) -> Path:
    return source_dir / "schemas" / f"{_schema_prefix(source_name)}_schemas.json"


def _endpoint_specs_for_source(
    source_name: str,
    source: dict[str, Any],
    source_dir: Path,
    report_endpoints: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    schema_file = _source_schema_file(source_name, source_dir)
    endpoints: list[dict[str, Any]] = []

    if schema_file.exists():
        with schema_file.open("r", encoding="utf-8") as file:
            source_payload = json.load(file)
        for index, endpoint in enumerate(source_payload.get("endpoints", []), start=1):
            endpoints.append(
                {
                    **endpoint,
                    "source_endpoint_index": index,
                    "endpoint_detail_status": "source_discovery",
                    "verification": _verification_for(endpoint, report_endpoints.get(source_name, [])),
                }
            )
    else:
        for index, endpoint in enumerate(report_endpoints.get(source_name, []), start=1):
            endpoints.append(
                {
                    "path": endpoint.get("path"),
                    "method": endpoint.get("method", "GET"),
                    "operation_id": None,
                    "summary": None,
                    "parameters": [],
                    "responses": {},
                    "source_endpoint_index": index,
                    "endpoint_detail_status": "verification_report_only",
                    "verification": _verification_payload(endpoint),
                }
            )

    discovered_count = int(source.get("endpoints_count") or 0)
    for index in range(len(endpoints) + 1, discovered_count + 1):
        endpoints.append(
            {
                "path": None,
                "method": None,
                "operation_id": None,
                "summary": None,
                "parameters": [],
                "responses": {},
                "source_endpoint_index": index,
                "endpoint_detail_status": "missing_from_integrated_artifact",
                "verification": {"status": "not_verified", "success": None},
            }
        )

    return endpoints


def _verification_for(endpoint: dict[str, Any], report_endpoints: list[dict[str, Any]]) -> dict[str, Any]:
    for report_endpoint in report_endpoints:
        if _paths_match(str(endpoint.get("path") or ""), str(report_endpoint.get("path") or "")):
            return _verification_payload(report_endpoint)
    return {"status": "not_verified", "success": None}


def _verification_payload(report_endpoint: dict[str, Any]) -> dict[str, Any]:
    success = report_endpoint.get("success")
    if success is True:
        status = "passed"
    elif success is False:
        status = "failed"
    else:
        status = "not_verified"
    return {
        "status": status,
        "success": success,
        "http_status": report_endpoint.get("status"),
        "response_time_ms": report_endpoint.get("response_time_ms"),
        "content_type": report_endpoint.get("content_type"),
        "error": report_endpoint.get("error"),
    }


def _paths_match(template: str, concrete: str) -> bool:
    if not template or not concrete:
        return False
    if template == concrete:
        return True
    normalized_concrete = concrete.replace("/v1/", "/")
    if template == normalized_concrete:
        return True
    pattern = re.escape(template)
    pattern = re.sub(r"\\\{[^}]+\\\}", r"[^/]+", pattern)
    return re.fullmatch(pattern, normalized_concrete) is not None


def _endpoint_mapping(
    source: dict[str, Any],
    endpoint: dict[str, Any],
    schema_names: list[str],
    schema_catalog: dict[str, Any],
    policy: dict[str, Any],
    dataset_catalog: dict[str, Any],
    quality_rules: dict[str, Any],
    default_quality_rules: dict[str, Any],
) -> dict[str, Any]:
    source_name = str(source.get("name"))
    dataset = _dataset_for_endpoint(source_name, endpoint, policy)
    endpoint_schema_names = _schemas_for_dataset(source_name, dataset, schema_names)
    if not endpoint_schema_names:
        endpoint_schema_names = _schemas_matching_endpoint(endpoint, schema_names)
    quality = _quality_for(dataset, endpoint_schema_names, schema_catalog, quality_rules, default_quality_rules)
    connector = _connector(policy, dataset, dataset_catalog)

    return {
        "endpoint_id": _endpoint_id(source_name, endpoint),
        "source": source_name,
        "path": endpoint.get("path"),
        "method": endpoint.get("method"),
        "operation_id": endpoint.get("operation_id"),
        "summary": endpoint.get("summary"),
        "parameters": endpoint.get("parameters", []),
        "responses": endpoint.get("responses", {}),
        "endpoint_detail_status": endpoint.get("endpoint_detail_status"),
        "verification": endpoint.get("verification"),
        "dataset": dataset,
        "dataset_status": _dataset_status(dataset, dataset_catalog),
        "domain": _dataset_domain(dataset, dataset_catalog, policy),
        "connector": connector,
        "auth": policy["auth"],
        "pagination": policy["pagination"],
        "rate_limit": policy["rate_limit"],
        "backfill_policy": policy["backfill_policy"],
        "schema_mapping": _schema_mapping_payload(dataset, endpoint_schema_names, dataset_catalog),
        "quality_rules": quality,
        "airflow": _airflow_plan(source_name, dataset, connector, dataset_catalog),
    }


def _schema_mapping(
    source: dict[str, Any],
    schema_name: str,
    schema_definition: dict[str, Any],
    policy: dict[str, Any],
    dataset_catalog: dict[str, Any],
    quality_rules: dict[str, Any],
    default_quality_rules: dict[str, Any],
) -> dict[str, Any]:
    source_name = str(source.get("name"))
    dataset = _dataset_for_schema(source_name, schema_name, policy)
    connector = _connector(policy, dataset, dataset_catalog)
    return {
        "schema_name": schema_name,
        "source": source_name,
        "dataset": dataset,
        "dataset_status": _dataset_status(dataset, dataset_catalog),
        "domain": _dataset_domain(dataset, dataset_catalog, policy),
        "connector": connector,
        "auth": policy["auth"],
        "pagination": policy["pagination"],
        "rate_limit": policy["rate_limit"],
        "backfill_policy": policy["backfill_policy"],
        "schema_mapping": _schema_mapping_payload(dataset, [schema_name], dataset_catalog),
        "quality_rules": _quality_from_schema(dataset, schema_definition, quality_rules, default_quality_rules),
        "airflow": _airflow_plan(source_name, dataset, connector, dataset_catalog),
    }


def _dataset_for_endpoint(source_name: str, endpoint: dict[str, Any], policy: dict[str, Any]) -> str:
    text = " ".join(
        str(value or "")
        for value in (
            endpoint.get("path"),
            endpoint.get("summary"),
            endpoint.get("operation_id"),
        )
    ).lower()

    if source_name == "DfT_Road_Traffic":
        if "region" in text:
            return "dft_road_traffic_regions"
        if "local-authorit" in text:
            return "dft_road_traffic_local_authorities"
        return "dft_road_traffic"
    if source_name == "LondonAir":
        if "daily" in text:
            return "londonair_daily_index"
        if "monitoringsites" in text or "site" in text and "hourly" not in text:
            return "londonair_sites"
        if "species" in text:
            return "londonair_species"
        if "healthadvice" in text:
            return "londonair_health_advice"
        return "londonair_monitoring"
    if source_name == "London_Datastore":
        if "borough" in text:
            return "london_boroughs"
        return "london_journeys"
    if source_name == "NaPTAN_NPTG":
        if "localit" in text:
            return "nptg_localities"
        if "admin" in text:
            return "nptg_admin_areas"
        return "naptan_stops"
    if source_name == "NCEI":
        if "/data" in text:
            return "ncei_cdo_climate"
        if "station" in text:
            return "ncei_stations"
        if "dataset" in text:
            return "ncei_datasets"
        if "datatype" in text:
            return "ncei_datatypes"
        if "location" in text:
            return "ncei_locations"
        if "categor" in text:
            return "ncei_categories"
        return "ncei_reference"
    if source_name == "OpenAQ":
        if "measurement" in text or "/hours" in text or "/days" in text:
            return "openaq_measurements"
        if "sensor" in text:
            return "openaq_sensors"
        if "parameter" in text:
            return "openaq_parameters"
        if "country" in text:
            return "openaq_countries"
        return "openaq_locations"
    if source_name == "OpenMeteo":
        if "air-quality" in text or "airquality" in text:
            return "openmeteo_air_quality"
        if "archive" in text or "climate" in text:
            return "openmeteo_archive"
        if "marine" in text:
            return "openmeteo_marine"
        if "search" in text or "geo" in text:
            return "openmeteo_geocoding"
        return "openmeteo_forecast"
    if source_name == "OpenWeather":
        if "air_pollution" in text or "airpollution" in text:
            return "openweather_air_pollution"
        if "forecast" in text:
            return "openweather_forecast"
        if "geo/" in text:
            return "openweather_geocoding"
        if "solar" in text:
            return "openweather_solar_radiation"
        if "fire" in text:
            return "openweather_fire_index"
        if "roadrisk" in text or "road risk" in text:
            return "openweather_road_risk"
        return "openweather_current"
    if source_name == "STATS19":
        if "vehicle" in text:
            return "stats19_vehicles"
        if "casualt" in text:
            return "stats19_casualties"
        return "stats19_collisions"
    if source_name == "TfL_GIS_Hub":
        if "borough" in text:
            return "tfl_gis_boroughs"
        if "cycle" in text:
            return "tfl_gis_cycle_routes"
        if "road" in text:
            return "tfl_gis_road_network"
        return "tfl_gis_stations"
    if source_name == "TfL_Open_Data":
        if "sequence" in text:
            return "tfl_bus_sequences"
        if "station" in text or "kml" in text:
            return "tfl_station_kml"
        return "tfl_bus_stops"
    if source_name == "TfL Unified API":
        return _tfl_dataset(text)
    if source_name == "WAQI":
        if "search" in text or "station/find" in text:
            return "waqi_station_search"
        if "map" in text or "city" in text:
            return "waqi_map_markers"
        if "forecast" in text:
            return "waqi_forecast"
        return "waqi_air_quality"
    return policy["default_dataset"]


def _dataset_for_schema(source_name: str, schema_name: str, policy: dict[str, Any]) -> str:
    text = schema_name.lower()
    if source_name == "DfT_Road_Traffic":
        if "region" in text:
            return "dft_road_traffic_regions"
        if "localauthority" in text:
            return "dft_road_traffic_local_authorities"
        return "dft_road_traffic"
    if source_name == "LondonAir":
        if "healthadvice" in text:
            return "londonair_health_advice"
        if "monitoringsite" in text:
            return "londonair_sites"
        if "species" in text and "reading" not in text:
            return "londonair_species"
        return "londonair_monitoring"
    if source_name == "London_Datastore":
        return "london_boroughs" if "borough" in text else "london_journeys"
    if source_name == "NaPTAN_NPTG":
        if "locality" in text:
            return "nptg_localities"
        if "adminarea" in text:
            return "nptg_admin_areas"
        return "naptan_stops"
    if source_name == "NCEI":
        if "climaterecord" in text:
            return "ncei_cdo_climate"
        if "station" in text:
            return "ncei_stations"
        if "dataset" in text:
            return "ncei_datasets"
        if "datatype" in text:
            return "ncei_datatypes"
        if "location" in text:
            return "ncei_locations"
        return "ncei_reference"
    if source_name == "OpenAQ":
        if "sensor" in text:
            return "openaq_sensors"
        if "parameter" in text:
            return "openaq_parameters"
        if "country" in text:
            return "openaq_countries"
        return "openaq_measurements"
    if source_name == "OpenMeteo":
        if "airquality" in text:
            return "openmeteo_air_quality"
        if "marine" in text:
            return "openmeteo_marine"
        if "climate" in text or "archive" in text:
            return "openmeteo_archive"
        if "search" in text or "geolocation" in text:
            return "openmeteo_geocoding"
        return "openmeteo_forecast"
    if source_name == "OpenWeather":
        if "airpollution" in text or "aqi" in text:
            return "openweather_air_pollution"
        if "forecast" in text:
            return "openweather_forecast"
        if "geolocation" in text:
            return "openweather_geocoding"
        if "solar" in text:
            return "openweather_solar_radiation"
        if "fireindex" in text:
            return "openweather_fire_index"
        if "roadrisk" in text:
            return "openweather_road_risk"
        return "openweather_current"
    if source_name == "STATS19":
        if "vehicle" in text:
            return "stats19_vehicles"
        if "casualty" in text:
            return "stats19_casualties"
        return "stats19_collisions"
    if source_name == "TfL_GIS_Hub":
        if "borough" in text:
            return "tfl_gis_boroughs"
        if "cycle" in text:
            return "tfl_gis_cycle_routes"
        if "road" in text:
            return "tfl_gis_road_network"
        return "tfl_gis_stations"
    if source_name == "TfL_Open_Data":
        if "sequence" in text:
            return "tfl_bus_sequences"
        if "station" in text or "kml" in text:
            return "tfl_station_kml"
        return "tfl_bus_stops"
    if source_name == "TfL Unified API":
        return _tfl_dataset(text)
    if source_name == "WAQI":
        if "forecast" in text:
            return "waqi_forecast"
        if "search" in text or "stationinfo" in text:
            return "waqi_station_search"
        if "mapmarker" in text or "citygeo" in text:
            return "waqi_map_markers"
        return "waqi_air_quality"
    return policy["default_dataset"]


def _tfl_dataset(text: str) -> str:
    if "line" in text and ("status" in text or "disruption" in text or "/line/mode" in text):
        return "tfl_transport_status"
    if "bikepoint" in text or "bike point" in text:
        return "tfl_bike_points"
    if "road" in text or "street" in text:
        return "tfl_roads"
    if "stoppoint" in text or "stop point" in text:
        return "tfl_stop_points"
    if "journey" in text:
        return "tfl_journey_planner"
    if "accidentstats" in text or "accident" in text:
        return "tfl_accident_stats"
    if "airquality" in text or "air quality" in text:
        return "tfl_air_quality"
    if "fare" in text:
        return "tfl_fares"
    if "occupancy" in text or "carpark" in text:
        return "tfl_occupancy"
    if "place" in text:
        return "tfl_places"
    if "search" in text:
        return "tfl_search"
    if "prediction" in text or "arrival" in text or "vehicle" in text:
        return "tfl_arrivals"
    return "tfl_unified_api_reference"


def _schemas_for_dataset(source_name: str, dataset: str, schema_names: Iterable[str]) -> list[str]:
    policy = _source_policy(source_name)
    return [
        schema_name
        for schema_name in schema_names
        if _dataset_for_schema(source_name, schema_name, policy) == dataset
    ]


def _schemas_matching_endpoint(endpoint: dict[str, Any], schema_names: list[str]) -> list[str]:
    text = " ".join(str(endpoint.get(key) or "") for key in ("path", "summary", "operation_id"))
    endpoint_tokens = set(_tokens(text))
    scored: list[tuple[int, str]] = []
    for schema_name in schema_names:
        score = len(endpoint_tokens & set(_tokens(schema_name)))
        if score:
            scored.append((score, schema_name))
    return [schema_name for _, schema_name in sorted(scored, reverse=True)[:20]]


def _connector(policy: dict[str, Any], dataset: str, dataset_catalog: dict[str, Any]) -> dict[str, Any]:
    dataset_config = dataset_catalog.get(dataset, {})
    source_type = dataset_config.get("source_type") or policy["connector_type"]

    if source_type == "api_stream":
        module = "ingestion.streaming.producer"
        mode = "micro_batch_stream"
        status = "implemented"
    elif source_type == "csv_download":
        module = "ingestion.batch.csv_download_ingestion"
        mode = "batch"
        status = "implemented"
    elif source_type in {"rest_api", "openapi_rest", "arcgis_hub"}:
        module = "ingestion.batch.api_ingestion"
        mode = "batch"
        status = "implemented"
    elif source_type == "parquet_batch":
        module = "ingestion.batch.csv_ingestion"
        mode = "batch"
        status = "implemented"
    else:
        module = _planned_module_for(policy["connector_type"])
        mode = "batch"
        status = "planned"

    if dataset not in dataset_catalog and status == "implemented":
        status = "connector_implemented_dataset_planned"

    return {
        "type": source_type,
        "mode": mode,
        "module": module,
        "status": status,
    }


def _planned_module_for(connector_type: str) -> str:
    if connector_type in {"csv_download", "static_file", "data_portal"}:
        return "ingestion.batch.csv_download_ingestion"
    if connector_type in {"rest_api", "openapi_rest", "api_stream"}:
        return "ingestion.batch.api_ingestion"
    return f"planned.{connector_type}_connector"


def _schema_mapping_payload(
    dataset: str,
    schema_names: list[str],
    dataset_catalog: dict[str, Any],
) -> dict[str, Any]:
    dataset_config = dataset_catalog.get(dataset, {})
    target_schema_path = dataset_config.get("schema_path") or f"domains/{_planned_domain(dataset)}/schemas/{dataset}.schema.json"
    return {
        "mode": "source_discovery_json_schema",
        "source_schema_names": schema_names,
        "source_schema_count": len(schema_names),
        "target_schema_path": target_schema_path,
        "target_schema_status": "existing" if dataset_config.get("schema_path") else "planned",
        "conversion": "common.source_discovery.to_nexus_json_schema",
    }


def _quality_for(
    dataset: str,
    schema_names: list[str],
    schema_catalog: dict[str, Any],
    quality_rules: dict[str, Any],
    default_quality_rules: dict[str, Any],
) -> dict[str, Any]:
    if dataset in quality_rules:
        return {
            "status": "configured",
            "required_columns": quality_rules[dataset].get("required_columns", []),
            "freshness_column": quality_rules[dataset].get("freshness_column"),
            "defaults": default_quality_rules,
        }

    required_columns: list[str] = []
    for schema_name in schema_names:
        schema_definition = schema_catalog.get(schema_name, {})
        required_columns.extend(_required_fields(schema_definition))
    return _planned_quality(required_columns, default_quality_rules)


def _quality_from_schema(
    dataset: str,
    schema_definition: dict[str, Any],
    quality_rules: dict[str, Any],
    default_quality_rules: dict[str, Any],
) -> dict[str, Any]:
    if dataset in quality_rules:
        return {
            "status": "configured",
            "required_columns": quality_rules[dataset].get("required_columns", []),
            "freshness_column": quality_rules[dataset].get("freshness_column"),
            "defaults": default_quality_rules,
        }
    return _planned_quality(_required_fields(schema_definition), default_quality_rules)


def _planned_quality(required_columns: list[str], default_quality_rules: dict[str, Any]) -> dict[str, Any]:
    unique_required = list(dict.fromkeys(required_columns))
    return {
        "status": "planned_from_source_schema",
        "required_columns": unique_required[:25],
        "freshness_column": _freshness_column(unique_required),
        "defaults": default_quality_rules,
    }


def _required_fields(schema_definition: dict[str, Any]) -> list[str]:
    required = schema_definition.get("required_fields") or schema_definition.get("required") or []
    if isinstance(required, list):
        return [str(field) for field in required]
    return []


def _freshness_column(columns: list[str]) -> str | None:
    for column in columns:
        lowered = column.lower()
        if any(token in lowered for token in ("time", "date", "timestamp", "observed")):
            return column
    return None


def _airflow_plan(
    source_name: str,
    dataset: str,
    connector: dict[str, Any],
    dataset_catalog: dict[str, Any],
) -> dict[str, Any]:
    task_slug = _slug(dataset)
    if dataset in dataset_catalog:
        source_type = dataset_catalog[dataset].get("source_type")
        if source_type == "api_stream" and source_name in STREAM_SOURCE_KEYS:
            stream_source = STREAM_SOURCE_KEYS[source_name]
            return {
                "dag_id": "nexus_streaming_pipeline",
                "task_id": "produce_transport_events",
                "status": "implemented_parameterized",
                "run_hint": f"NEXUS_STREAM_SOURCE={stream_source}",
            }
        return {
            "dag_id": "nexus_batch_ingestion_pipeline",
            "task_id": f"batch_run_{task_slug}",
            "status": "cli_supported_dag_task_planned",
            "command": f"python -m cli.nexus batch run --dataset {dataset}",
        }

    return {
        "dag_id": "nexus_source_discovery_ingestion_pipeline",
        "task_id": f"ingest_{task_slug}"[:120],
        "status": "planned",
        "command": _planned_command(dataset, connector),
    }


def _planned_command(dataset: str, connector: dict[str, Any]) -> str:
    if connector.get("mode") == "micro_batch_stream":
        return f"python -m cli.nexus quality stream --dataset {dataset}"
    return f"python -m cli.nexus batch run --dataset {dataset}"


def _dataset_status(dataset: str, dataset_catalog: dict[str, Any]) -> str:
    return "existing" if dataset in dataset_catalog else "planned"


def _dataset_domain(dataset: str, dataset_catalog: dict[str, Any], policy: dict[str, Any]) -> str:
    return dataset_catalog.get(dataset, {}).get("domain") or policy["domain"]


def _planned_domain(dataset: str) -> str:
    transport_prefixes = ("tfl_", "dft_", "stats19_", "naptan_", "nptg_", "london_", "os_")
    return "transport" if dataset.startswith(transport_prefixes) else "environment"


def _endpoint_id(source_name: str, endpoint: dict[str, Any]) -> str:
    raw = endpoint.get("operation_id") or endpoint.get("path") or f"unlisted_{endpoint.get('source_endpoint_index')}"
    return f"{_slug(source_name)}.{_slug(str(raw))}"


def _coverage_summary(
    catalog: dict[str, Any],
    sources: list[dict[str, Any]],
    endpoint_entries: list[dict[str, Any]],
    schema_entries: list[dict[str, Any]],
    dataset_catalog: dict[str, Any],
) -> dict[str, Any]:
    discovered_endpoint_count = sum(int(source.get("endpoints_count") or 0) for source in catalog.get("sources", []))
    datasets = {entry["dataset"] for entry in [*endpoint_entries, *schema_entries]}
    existing_datasets = sorted(dataset for dataset in datasets if dataset in dataset_catalog)
    planned_datasets = sorted(dataset for dataset in datasets if dataset not in dataset_catalog)
    return {
        "source_count": len(sources),
        "discovered_endpoint_count": discovered_endpoint_count,
        "mapped_endpoint_count": len(endpoint_entries),
        "schema_count": len(catalog.get("schemas", {})),
        "mapped_schema_count": len(schema_entries),
        "existing_dataset_count": len(existing_datasets),
        "planned_dataset_count": len(planned_datasets),
        "existing_datasets": existing_datasets,
        "planned_datasets": planned_datasets,
        "endpoint_detail_missing_count": sum(
            1
            for entry in endpoint_entries
            if entry.get("endpoint_detail_status") == "missing_from_integrated_artifact"
        ),
        "verified_endpoint_count": sum(
            1
            for entry in endpoint_entries
            if (entry.get("verification") or {}).get("status") == "passed"
        ),
    }


def _dataset_index(
    endpoint_entries: list[dict[str, Any]],
    schema_entries: list[dict[str, Any]],
    dataset_catalog: dict[str, Any],
) -> list[dict[str, Any]]:
    datasets = sorted({entry["dataset"] for entry in [*endpoint_entries, *schema_entries]})
    index: list[dict[str, Any]] = []
    for dataset in datasets:
        endpoint_count = sum(1 for entry in endpoint_entries if entry["dataset"] == dataset)
        schema_count = sum(1 for entry in schema_entries if entry["dataset"] == dataset)
        index.append(
            {
                "dataset": dataset,
                "status": _dataset_status(dataset, dataset_catalog),
                "endpoint_count": endpoint_count,
                "schema_count": schema_count,
            }
        )
    return index


def _tokens(value: str) -> list[str]:
    return [token for token in re.split(r"[^a-z0-9]+", value.lower()) if token]


def _slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").lower()
    return slug or "unknown"
