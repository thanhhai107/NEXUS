from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import requests

TOPICS = {
    "transport": "transport-events",
    "openaq": "environment-openaq",
    "waqi": "environment-waqi",
    "tfl": "transport-tfl",
    "gtfs": "transport-gtfs",
    "singapore": "transport-sg-traffic",
    "londonair": "environment-londonair",
    "openmeteo": "environment-openmeteo",
    "openweather": "environment-openweather",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sim_transport() -> dict[str, object]:
    return {
        "event_id": str(uuid.uuid4()),
        "source": "simulated-transport-producer",
        "event_type": random.choice(["traffic_accident", "road_closure", "slow_traffic"]),
        "event_time": now_iso(),
        "severity": random.randint(1, 4),
        "state": random.choice(["CA", "NY", "IL", "TX", "FL"]),
        "city": random.choice(["Los Angeles", "New York", "Chicago", "Houston", "Miami"]),
        "latitude": round(random.uniform(25.0, 48.0), 6),
        "longitude": round(random.uniform(-124.0, -71.0), 6),
    }


def sim_env() -> dict[str, object]:
    return {
        "event_id": str(uuid.uuid4()),
        "source": "simulated-environment-producer",
        "event_type": "air_quality_measurement",
        "event_time": now_iso(),
        "location": random.choice(["Hanoi", "Singapore", "Bangkok", "Jakarta"]),
        "parameter": random.choice(["pm25", "pm10", "no2", "o3"]),
        "value": round(random.uniform(5.0, 120.0), 2),
        "unit": "ug/m3",
    }





def api_json(url: str, api_key: str | None = None, header: str = "Authorization") -> Any:
    headers = {}
    if api_key:
        headers[header] = api_key if header == "X-API-Key" else f"Bearer {api_key}"
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()
    return response.json()


def list_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("results", "data", "items", "events", "incidents"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        nested = payload.get("data")
        if isinstance(nested, dict):
            for key in ("records", "readings", "items"):
                value = nested.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
    return []


def norm_transport(record: dict[str, Any]) -> dict[str, object]:
    return {
        "event_id": str(record.get("event_id") or record.get("id") or uuid.uuid4()),
        "source": str(record.get("source") or "real-time-transport-api"),
        "event_type": str(record.get("event_type") or record.get("type") or "traffic_incident"),
        "event_time": str(record.get("event_time") or record.get("start_time") or now_iso()),
        "severity": record.get("severity"),
        "state": record.get("state"),
        "city": record.get("city"),
        "latitude": record.get("latitude") or record.get("lat"),
        "longitude": record.get("longitude") or record.get("lng"),
    }


def norm_openaq(record: dict[str, Any]) -> dict[str, object]:
    period = record.get("period") or {}
    coordinates = record.get("coordinates") or {}
    parameter = record.get("parameter") or {}
    observed_at = str(period.get("datetimeFrom") or record.get("datetime") or now_iso())
    return {
        "event_id": str(record.get("id") or uuid.uuid4()),
        "source": "openaq",
        "event_type": "air_quality_measurement",
        "event_time": observed_at,
        "datetime": observed_at,
        "location_id": record.get("locationId") or record.get("location_id"),
        "location": record.get("location") or record.get("locationName"),
        "parameter": parameter.get("name") if isinstance(parameter, dict) else record.get("parameter"),
        "value": record.get("value"),
        "unit": record.get("unit"),
        "latitude": coordinates.get("latitude") if isinstance(coordinates, dict) else None,
        "longitude": coordinates.get("longitude") if isinstance(coordinates, dict) else None,
    }


def norm_waqi(payload: dict[str, Any]) -> dict[str, object]:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    city = data.get("city") if isinstance(data.get("city"), dict) else {}
    time_data = data.get("time") if isinstance(data.get("time"), dict) else {}
    geo = city.get("geo") if isinstance(city.get("geo"), list) else [None, None]
    return {
        "event_id": str(data.get("idx") or uuid.uuid4()),
        "source": "waqi",
        "event_type": "aqi_measurement",
        "event_time": str(time_data.get("iso") or now_iso()),
        "station_uid": data.get("idx"),
        "station_name": city.get("name"),
        "aqi": int(data.get("aqi")) if str(data.get("aqi", "")).isdigit() else None,
        "observed_at": str(time_data.get("iso") or now_iso()),
        "latitude": geo[0],
        "longitude": geo[1],
        "dominant_pollutant": data.get("dominentpol"),
    }


def norm_tfl(record: dict[str, Any]) -> list[dict[str, object]]:
    statuses = record.get("lineStatuses") if isinstance(record.get("lineStatuses"), list) else [{}]
    events = []
    for status in statuses:
        events.append(
            {
                "event_id": str(uuid.uuid4()),
                "source": "tfl",
                "event_type": "line_status",
                "event_time": now_iso(),
                "line_id": record.get("id"),
                "line_name": record.get("name"),
                "status": status.get("statusSeverityDescription"),
                "severity": status.get("statusSeverity"),
                "reason": status.get("reason"),
            }
        )
    return events


def norm_sg(record: dict[str, Any]) -> dict[str, object]:
    location = record.get("location") if isinstance(record.get("location"), dict) else {}
    return {
        "event_id": str(record.get("camera_id") or uuid.uuid4()),
        "source": "data.gov.sg",
        "event_type": "traffic_image",
        "event_time": str(record.get("timestamp") or now_iso()),
        "camera_id": str(record.get("camera_id") or ""),
        "image_url": record.get("image") or record.get("image_url"),
        "latitude": location.get("latitude"),
        "longitude": location.get("longitude"),
    }


def norm_londonair(payload: Any) -> list[dict[str, object]]:
    """Flatten LondonAir nested LocalAuthority > Site > Species structure."""
    authorities = payload if isinstance(payload, list) else [payload]
    events: list[dict[str, object]] = []
    for auth in authorities:
        if not isinstance(auth, dict):
            continue
        borough_code = auth.get("@LocalAuthorityCode", "")
        borough_name = auth.get("@LocalAuthorityName", "")
        sites = auth.get("Site") or []
        if isinstance(sites, dict):
            sites = [sites]
        for site in sites:
            if not isinstance(site, dict):
                continue
            species_list = site.get("Species") or []
            if isinstance(species_list, dict):
                species_list = [species_list]
            for species in species_list:
                if not isinstance(species, dict):
                    continue
                events.append({
                    "event_id": str(uuid.uuid4()),
                    "source": "londonair",
                    "event_type": "air_quality_index",
                    "event_time": str(site.get("@BulletinDate") or now_iso()),
                    "site_code": site.get("@SiteCode", ""),
                    "site_name": site.get("@SiteName", ""),
                    "site_type": site.get("@SiteType", ""),
                    "borough_code": borough_code,
                    "borough_name": borough_name,
                    "species_code": species.get("@SpeciesCode", ""),
                    "species_description": species.get("@SpeciesDescription", ""),
                    "air_quality_index": species.get("@AirQualityIndex"),
                    "air_quality_band": species.get("@AirQualityBand"),
                    "latitude": site.get("@Latitude"),
                    "longitude": site.get("@Longitude"),
                })
    return events


def norm_openmeteo_aq(payload: dict[str, Any]) -> list[dict[str, object]]:
    """Flatten OpenMeteo air quality response into per-hour events."""
    lat = payload.get("latitude")
    lon = payload.get("longitude")
    current = payload.get("current") or {}
    if current:
        return [{
            "event_id": str(uuid.uuid4()),
            "source": "openmeteo",
            "event_type": "air_quality_current",
            "event_time": str(current.get("time") or now_iso()),
            "latitude": lat,
            "longitude": lon,
            "pm10": current.get("pm10"),
            "pm2_5": current.get("pm2_5"),
            "european_aqi": current.get("european_aqi"),
            "us_aqi": current.get("us_aqi"),
        }]
    return [{
        "event_id": str(uuid.uuid4()),
        "source": "openmeteo",
        "event_type": "air_quality_poll",
        "event_time": now_iso(),
        "latitude": lat,
        "longitude": lon,
    }]


def norm_openweather(payload: dict[str, Any]) -> dict[str, object]:
    """Normalize OpenWeather current weather response."""
    coord = payload.get("coord") or {}
    main_data = payload.get("main") or {}
    wind = payload.get("wind") or {}
    clouds_data = payload.get("clouds") or {}
    weather_list = payload.get("weather") or [{}]
    weather = weather_list[0] if weather_list else {}
    return {
        "event_id": str(uuid.uuid4()),
        "source": "openweather",
        "event_type": "current_weather",
        "event_time": now_iso(),
        "city_name": payload.get("name", ""),
        "latitude": coord.get("lat"),
        "longitude": coord.get("lon"),
        "temp": main_data.get("temp"),
        "feels_like": main_data.get("feels_like"),
        "pressure": main_data.get("pressure"),
        "humidity": main_data.get("humidity"),
        "wind_speed": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "clouds": clouds_data.get("all"),
        "visibility": payload.get("visibility"),
        "weather_main": weather.get("main"),
        "weather_description": weather.get("description"),
    }


def sg_events(payload: Any, limit: int) -> list[dict[str, object]]:
    items = list_records(payload)
    events: list[dict[str, object]] = []
    for item in items:
        cameras = item.get("cameras")
        if not isinstance(cameras, list):
            events.append(norm_sg(item))
            continue
        for camera in cameras:
            if isinstance(camera, dict):
                merged = dict(camera)
                merged.setdefault("timestamp", item.get("timestamp"))
                events.append(norm_sg(merged))
                if len(events) >= limit:
                    return events
    return events[:limit]


def gtfs_event(url: str) -> dict[str, object]:
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    return {
        "event_id": str(uuid.uuid4()),
        "source": "gtfs-realtime",
        "event_type": os.getenv("GTFS_REALTIME_TYPE", "gtfs_feed_poll"),
        "event_time": now_iso(),
        "feed_url": url,
        "feed_type": os.getenv("GTFS_REALTIME_TYPE", "unknown"),
        "byte_size": len(response.content),
    }


def api_events(source: str, api_url: str, api_key: str | None, limit: int) -> list[dict[str, object]]:
    if source == "gtfs":
        return [gtfs_event(api_url)]

    header = "X-API-Key" if source == "openaq" else "Authorization"
    payload = api_json(api_url, api_key, header=header)

    if source == "waqi":
        return [norm_waqi(payload)]
    if source == "londonair":
        auth_list = payload if isinstance(payload, list) else payload.get("HourlyAirQualityIndex", {}).get("LocalAuthority", [])
        if isinstance(auth_list, dict):
            auth_list = [auth_list]
        return norm_londonair(auth_list)[:limit]
    if source == "openmeteo":
        return norm_openmeteo_aq(payload)[:limit]
    if source == "openweather":
        return [norm_openweather(payload)]

    records = list_records(payload)
    if source == "openaq":
        return [norm_openaq(record) for record in records[:limit]]
    if source == "tfl":
        events: list[dict[str, object]] = []
        for record in records[:limit]:
            events.extend(norm_tfl(record))
        return events[:limit]
    if source == "singapore":
        return sg_events(payload, limit)
    return [norm_transport(record) for record in records[:limit]]


def event_stream(source: str, api_url: str | None, api_key: str | None, events: int) -> list[dict[str, object]]:
    if api_url:
        try:
            records = api_events(source, api_url, api_key, events)
            if records:
                return records
        except Exception as exc:
            print(f"{source} API unavailable, using fallback: {exc}")


    if source in {"openaq", "waqi", "londonair", "openmeteo", "openweather"}:
        return [sim_env() for _ in range(events)]
    return [sim_transport() for _ in range(events)]


def run_producer(
    source: str,
    topic: str,
    bootstrap_servers: str,
    events: int,
    delay_seconds: float,
    api_url: str | None = None,
    api_key: str | None = None,
) -> None:
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    for event in event_stream(source, api_url, api_key, events):
        producer.send(topic, event)
        print(f"Produced event_id={event['event_id']} source={source} topic={topic}")
        time.sleep(delay_seconds)

    producer.flush()


def default_url(source: str) -> str | None:
    urls = {
        "transport": os.getenv("TRANSPORT_EVENTS_API_URL"),
        "openaq": os.getenv("OPENAQ_API_URL", "https://api.openaq.org/v3/measurements"),
        "waqi": os.getenv("WAQI_API_URL"),
        "tfl": os.getenv("TFL_API_URL", "https://api.tfl.gov.uk/Line/Mode/tube,dlr,overground,elizabeth-line/Status"),
        "gtfs": os.getenv("GTFS_REALTIME_URL"),
        "singapore": os.getenv("SG_TRAFFIC_API_URL", "https://api-open.data.gov.sg/v2/real-time/api/traffic-images"),
        "londonair": os.getenv("LONDONAIR_API_BASE_URL", "https://api.erg.ic.ac.uk/AirQuality") + os.getenv("LONDONAIR_HOURLY_INDEX_ENDPOINT", "/Hourly/MonitoringIndex/GroupName=London/Json"),
        "openmeteo": os.getenv("OPENMETEO_API_URL", "https://air-quality-api.open-meteo.com") + "/v1/air-quality?latitude=51.5074&longitude=-0.1278&current=pm10,pm2_5,european_aqi,us_aqi",
        "openweather": os.getenv("OPENWEATHER_API_URL", "https://api.openweathermap.org") + "/data/2.5/weather?q=London&units=metric&appid=" + (os.getenv("OPENWEATHER_API_KEY") or ""),
    }
    return urls.get(source)


def default_key(source: str) -> str | None:
    keys = {
        "transport": os.getenv("TRANSPORT_EVENTS_API_KEY"),
        "openaq": os.getenv("OPENAQ_API_KEY"),
        "waqi": os.getenv("WAQI_API_TOKEN"),
        "tfl": os.getenv("TFL_API_KEY"),
        "londonair": os.getenv("LONDONAIR_API_KEY"),
        "openmeteo": None,
        "openweather": os.getenv("OPENWEATHER_API_KEY"),
    }
    return keys.get(source)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce domain streaming events into Kafka.")
    parser.add_argument(
        "--source",
        choices=sorted(TOPICS),
        default=os.getenv("NEXUS_STREAM_SOURCE", "transport"),
    )
    parser.add_argument("--topic")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--events", type=int, default=25)
    parser.add_argument("--delay-seconds", type=float, default=0.5)
    parser.add_argument("--api-url")
    parser.add_argument("--api-key")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    source = args.source
    run_producer(
        source=source,
        topic=args.topic or TOPICS[source],
        bootstrap_servers=args.bootstrap_servers,
        events=args.events,
        delay_seconds=args.delay_seconds,
        api_url=args.api_url if args.api_url is not None else default_url(source),
        api_key=args.api_key if args.api_key is not None else default_key(source),
    )
