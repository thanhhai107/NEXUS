"""Regenerate deterministic local CSV sample datasets.

The samples intentionally stay small (10 records each) but are generated from the
current domain JSON Schemas and cross-checked against the generated source
inventory under assets/source_discovery/. This keeps local demos aligned with the
latest catalog while avoiding network calls.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DISCOVERY_CATALOG = PROJECT_ROOT / "assets" / "source_discovery" / "all_schemas.json"
SAMPLES_DIR = PROJECT_ROOT / "assets" / "samples"
BASE_TIME = datetime(2026, 5, 29, 12, 0, tzinfo=timezone.utc)
LONDON_POINTS = [
    (51.5074, -0.1278, "Westminster"),
    (51.5155, -0.0922, "City of London"),
    (51.5416, -0.1430, "Camden"),
    (51.4893, -0.0882, "Southwark"),
    (51.4653, -0.2140, "Wandsworth"),
    (51.5465, -0.1058, "Islington"),
    (51.4746, -0.0357, "Lewisham"),
    (51.4014, -0.1958, "Merton"),
    (51.5761, -0.1458, "Haringey"),
    (51.6029, -0.1940, "Barnet"),
]
LINES = [
    ("bakerloo", "Bakerloo"),
    ("central", "Central"),
    ("district", "District"),
    ("elizabeth", "Elizabeth line"),
    ("jubilee", "Jubilee"),
    ("northern", "Northern"),
    ("piccadilly", "Piccadilly"),
    ("victoria", "Victoria"),
    ("waterloo-city", "Waterloo & City"),
    ("dlr", "DLR"),
]
POLLUTANTS = ["pm25", "pm10", "no2", "o3", "so2", "co", "pm25", "pm10", "no2", "o3"]
WEATHER = ["Clear", "Clouds", "Rain", "Mist", "Drizzle", "Clouds", "Clear", "Rain", "Clouds", "Clear"]


@dataclass(frozen=True)
class SampleSpec:
    dataset: str
    output: str
    schema_path: str
    source_discovery_schema: str | None = None
    row_count: int = 10


SAMPLE_SPECS = [
    SampleSpec("openaq_measurements", "openaq_measurements.csv", "domains/environment/schemas/openaq_measurements.schema.json", "OpenAQ_OpenAQ_Location"),
    SampleSpec("waqi_air_quality", "waqi_air_quality.csv", "domains/environment/schemas/waqi_air_quality.schema.json", "WAQI_WAQI_StationData"),
    SampleSpec("ncei_cdo_climate", "ncei_cdo_climate.csv", "domains/environment/schemas/ncei_cdo_climate.schema.json", "NCEI_NCEI_ClimateRecord"),
    SampleSpec("londonair_monitoring", "londonair_monitoring.csv", "domains/environment/schemas/londonair_monitoring.schema.json", "LondonAir_LondonAir_SpeciesReading"),
    SampleSpec("openmeteo_air_quality", "openmeteo_air_quality.csv", "domains/environment/schemas/openmeteo_air_quality.schema.json", "OpenMeteo_OpenMeteo_AirQualityHourly"),
    SampleSpec("openweather_current", "openweather_current.csv", "domains/environment/schemas/openweather_current.schema.json", "OpenWeather_OpenWeather_Current"),
    SampleSpec("tfl_transport_status", "tfl_transport_status.csv", "domains/transport/schemas/tfl_transport_status.schema.json", "TfL_Unified_API_Tfl.Api.Presentation.Entities.LineStatus"),
    SampleSpec("gtfs_realtime_events", "gtfs_realtime_events.csv", "domains/transport/schemas/gtfs_realtime_events.schema.json"),
    SampleSpec("stats19_collisions", "stats19_collisions.csv", "domains/transport/schemas/stats19_collisions.schema.json", "STATS19_STATS19_Collision"),
    SampleSpec("naptan_stops", "naptan_stops.csv", "domains/transport/schemas/naptan_stops.schema.json", "NaPTAN_NPTG_NaPTAN_AccessNode"),
    SampleSpec("london_journeys", "london_journeys.csv", "domains/transport/schemas/london_journeys.schema.json", "London_Datastore_London_TfLJourneys"),
    SampleSpec("dft_road_traffic", "dft_road_traffic.csv", "domains/transport/schemas/dft_road_traffic.schema.json", "DfT_Road_Traffic_DfT_TrafficCount"),
    SampleSpec("transport_events", "transport_events.csv", "domains/transport/schemas/transport_events.schema.json"),
]


def main() -> int:
    discovery = _load_source_discovery()
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for spec in SAMPLE_SPECS:
        if spec.source_discovery_schema:
            _assert_discovered_schema(discovery, spec.source_discovery_schema)
        schema = _read_json(PROJECT_ROOT / spec.schema_path)
        properties = dict(schema.get("properties", {}))
        if spec.source_discovery_schema:
            properties.update(_discovered_sample_properties(discovery, spec.source_discovery_schema, properties))
        columns = list(properties.keys())
        if not columns:
            raise ValueError(f"Schema has no properties: {spec.schema_path}")
        rows = [_sample_row(spec.dataset, properties, index) for index in range(spec.row_count)]
        output_path = SAMPLES_DIR / spec.output
        _write_csv(output_path, columns, rows)
        written.append(output_path)
    print(f"Regenerated {len(written)} sample dataset files from source discovery/domain schemas:")
    for path in written:
        print(f"- {path.relative_to(PROJECT_ROOT)}")
    return 0


def _load_source_discovery() -> dict[str, Any]:
    catalog = _read_json(SOURCE_DISCOVERY_CATALOG)
    if "schemas" not in catalog:
        raise ValueError(f"Invalid source discovery catalog: {SOURCE_DISCOVERY_CATALOG}")
    return catalog


def _assert_discovered_schema(catalog: dict[str, Any], schema_name: str) -> None:
    if schema_name not in catalog.get("schemas", {}):
        raise KeyError(f"Source discovery schema not found: {schema_name}")


def _discovered_sample_properties(
    catalog: dict[str, Any],
    schema_name: str,
    existing_properties: dict[str, Any],
) -> dict[str, Any]:
    """Return normalized source-discovery fields that are not in the domain schema."""
    discovered = catalog.get("schemas", {}).get(schema_name, {})
    source_properties = discovered.get("properties", {}) if isinstance(discovered, dict) else {}
    existing = set(existing_properties)
    output: dict[str, Any] = {}
    for source_name, source_field in source_properties.items():
        normalized = _normalize_source_field_name(str(source_name))
        if not normalized or normalized in existing or normalized in output:
            continue
        output[normalized] = _source_field_to_sample_schema(source_field)
    return output


def _normalize_source_field_name(name: str) -> str:
    name = name.lstrip("@").replace("(", "_").replace(")", "")
    chars: list[str] = []
    previous = ""
    for char in name:
        if char.isupper() and previous and (previous.islower() or previous.isdigit()):
            chars.append("_")
        chars.append(char.lower() if char.isalnum() else "_")
        previous = char
    normalized = "".join(chars)
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized.strip("_")


def _source_field_to_sample_schema(field: Any) -> dict[str, Any]:
    if not isinstance(field, dict):
        return {"type": "string"}
    field_type = field.get("type")
    if field_type == "any":
        field_type = "object" if field.get("ref") else "string"
    if field_type not in {"string", "integer", "number", "boolean", "array", "object"}:
        field_type = "string"
    schema: dict[str, Any] = {"type": field_type}
    if field.get("format"):
        schema["format"] = field["format"]
    return schema


def _sample_row(dataset: str, properties: dict[str, Any], index: int) -> dict[str, Any]:
    lat, lon, borough = LONDON_POINTS[index % len(LONDON_POINTS)]
    line_id, line_name = LINES[index % len(LINES)]
    pollutant = POLLUTANTS[index % len(POLLUTANTS)]
    timestamp = BASE_TIME - timedelta(hours=index)
    row: dict[str, Any] = {}
    for column, field_schema in properties.items():
        row[column] = _value_for(
            dataset,
            column,
            index,
            timestamp,
            lat,
            lon,
            borough,
            line_id,
            line_name,
            pollutant,
            field_schema,
        )
    return row


def _value_for(
    dataset: str,
    column: str,
    index: int,
    timestamp: datetime,
    lat: float,
    lon: float,
    borough: str,
    line_id: str,
    line_name: str,
    pollutant: str,
    field_schema: dict[str, Any] | None = None,
) -> Any:
    name = column.lower()
    compact = name.replace("_", "")
    iso = timestamp.isoformat().replace("+00:00", "Z")
    date = timestamp.date().isoformat()

    overrides = {
        "source": dataset.replace("_", "-"),
        "event_type": _event_type(dataset),
        "event_time": iso,
        "datetime": iso,
        "observed_at": iso,
        "reading_date_time": iso,
        "date": date,
        "start_time": iso,
        "end_time": iso,
        "period_beginning": date,
        "period_ending": (timestamp + timedelta(days=27)).date().isoformat(),
        "modificationdatetime": iso,
        "creationdatetime": (timestamp - timedelta(days=30)).isoformat().replace("+00:00", "Z"),
        "expected_arrival": (timestamp + timedelta(minutes=3 + index)).isoformat().replace("+00:00", "Z"),
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "start_lat": round(lat, 6),
        "start_lng": round(lon, 6),
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "state": borough,
        "city": borough,
        "county": borough,
        "borough": borough,
        "location": f"London {borough} Monitoring Site",
        "location_id": 1000 + index,
        "station": f"GHCND:UKM0000377{index % 5}",
        "station_uid": f"waqi-london-{index + 1:03d}",
        "station_name": f"London {borough}",
        "site_code": f"LD{index + 1:02d}",
        "species_code": pollutant.upper(),
        "parameter": pollutant,
        "unit": "ug/m3" if pollutant not in {"co"} else "ppm",
        "value": round(8.5 + index * 1.7, 2),
        "aqi": 35 + index * 4,
        "dominant_pollutant": pollutant,
        "line_id": line_id,
        "line_name": line_name,
        "severity": 10 if index % 4 else 6,
        "reason": "No disruptions" if index % 4 else f"Minor delays on the {line_name} line",
        "mode_name": "tube",
        "vehicle_id": f"veh-{100 + index}",
        "route_id": line_id,
        "trip_id": f"trip-{index + 1:03d}",
        "stop_id": f"940GZZLU{index + 1:03d}",
        "event_id": f"{dataset}-{index + 1:03d}",
        "accident_index": f"2026LDN{index + 1:07d}",
        "accident_year": 2026,
        "accident_reference": f"LDN{index + 1:07d}",
        "accident_severity": (index % 3) + 1,
        "number_of_vehicles": 1 + index % 3,
        "number_of_casualties": index % 2,
        "time": timestamp.strftime("%H:%M"),
        "day_of_week": (index % 7) + 1,
        "atcocode": f"49000{index + 1:05d}",
        "naptancode": f"{index + 1:05d}",
        "commonname": f"{borough} Station Stop {index + 1}",
        "shortcommonname": f"{borough} Stop",
        "stoptype": "BCT",
        "status": "active" if dataset == "naptan_stops" else ("Good Service" if index % 4 else "Minor Delays"),
        "status_severity": 10 if index % 4 else 6,
        "status_severity_description": "Good Service" if index % 4 else "Minor Delays",
        "created": iso,
        "modified": iso,
        "reporting_period": f"2026-P{index + 1:02d}",
        "days_in_period": 28,
        "bus_journeys_m": round(120 + index * 1.5, 2),
        "underground_journeys_m": round(90 + index * 1.2, 2),
        "dlr_journeys_m": round(12 + index * 0.3, 2),
        "count_point_id": 900000 + index,
        "aadf_year": 2026,
        "road_name": f"A{index + 1}",
        "road_category": "A Road",
        "all_motor_vehicles": 10000 + index * 250,
        "id": f"{dataset}-{index + 1}",
        "dt": int(timestamp.timestamp()),
        "temp": round(12.5 + index * 0.4, 1),
        "humidity": 65 + index,
        "weather_main": WEATHER[index % len(WEATHER)],
        "pm10": round(18 + index * 0.8, 1),
        "pm2_5": round(10 + index * 0.5, 1),
        "nitrogen_dioxide": round(22 + index * 0.7, 1),
    }
    if column in overrides:
        return overrides[column]
    if name in overrides:
        return overrides[name]
    if compact in overrides:
        return overrides[compact]
    if compact.endswith("id") or compact.endswith("code") or compact.endswith("reference"):
        return f"{compact}-{index + 1:03d}"
    if "time" in compact or "date" in compact:
        return iso
    if "year" in compact:
        return 2026
    if "latitude" in compact:
        return round(lat, 6)
    if "longitude" in compact:
        return round(lon, 6)
    if any(token in compact for token in ("count", "number", "severity", "speed", "pressure", "visibility", "vehicles")):
        return 10 + index
    if any(token in compact for token in ("distance", "length", "temperature", "value", "amount", "easting", "northing")):
        return round(1.5 + index * 0.75, 2)
    if compact.startswith("is") or compact.startswith("has"):
        return "true" if index % 2 == 0 else "false"

    field_type = _schema_type(field_schema or {})
    if field_type == "integer":
        return index + 1
    if field_type == "number":
        return round(1.5 + index * 0.75, 2)
    if field_type == "boolean":
        return "true" if index % 2 == 0 else "false"
    if field_type == "array":
        return "[]"
    if field_type == "object":
        return "{}"
    return f"{column}_{index + 1}"


def _schema_type(field_schema: dict[str, Any]) -> str | None:
    raw_type = field_schema.get("type")
    if isinstance(raw_type, list):
        for preferred in ("integer", "number", "boolean", "array", "object", "string"):
            if preferred in raw_type:
                return preferred
        return next((item for item in raw_type if item != "null"), None)
    if isinstance(raw_type, str):
        return raw_type
    return None


def _event_type(dataset: str) -> str:
    if "air" in dataset or dataset in {"openaq_measurements", "waqi_air_quality"}:
        return "air_quality_observation"
    if "weather" in dataset:
        return "weather_observation"
    if "tfl" in dataset or "transport" in dataset:
        return "transport_status"
    return "sample_record"


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    raise SystemExit(main())
