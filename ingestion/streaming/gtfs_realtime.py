"""
GTFS Realtime Feed Ingestion Module for NEXUS.

Polls GTFS-RT Protocol Buffer feeds (TripUpdate, VehiclePosition, Alert),
parses protobuf messages, and writes to the canonical raw envelope format.

Usage:
    python -m ingestion.streaming.gtfs_realtime --source my_agency --dataset transit \\
        --feed-url https://api.example.com/gtfs-rt/trip-updates.pb --feed-type trip_update

Supported feed types: trip_update, vehicle_position, alert.
"""

from __future__ import annotations
import argparse
import json
import os
import struct
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.canonical.envelope import EnvelopeContext
from ingestion.canonical.writer import write_raw_envelopes
from common.config import BRONZE_DIR

# ═══════════════════════════════════════════════════════════════════════════════
# Data Classes
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class GTFSRealtimeConfig:
    """Configuration for a GTFS Realtime feed polling session."""

    source_key: str
    dataset: str
    feed_url: str
    feed_type: str = "trip_update"
    api_key: str | None = None
    poll_interval_seconds: float = 60.0
    max_iterations: int = 0
    max_entities_per_feed: int = 100


@dataclass
class GTFSRealtimeResult:
    """Result of a GTFS Realtime polling session."""

    iterations: int = 0
    entities_parsed: int = 0
    landed: int = 0
    errors: list[str] = field(default_factory=list)
    raw_paths: list[str] = field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════════
# Enum Lookup Tables
# ═══════════════════════════════════════════════════════════════════════════════

_SCHEDULE_RELATIONSHIP = {
    0: "SCHEDULED",
    1: "ADDED",
    2: "UNSCHEDULED",
    3: "CANCELED",
}

_VEHICLE_STOP_STATUS = {
    0: "INCOMING_AT",
    1: "STOPPED_AT",
    2: "IN_TRANSIT_TO",
}

_CONGESTION_LEVEL = {
    0: "UNKNOWN_CONGESTION_LEVEL",
    1: "RUNNING_SMOOTHLY",
    2: "STOP_AND_GO",
    3: "CONGESTION",
    4: "SEVERE_CONGESTION",
}

_OCCUPANCY_STATUS = {
    0: "EMPTY",
    1: "MANY_SEATS_AVAILABLE",
    2: "FEW_SEATS_AVAILABLE",
    3: "STANDING_ROOM_ONLY",
    4: "CRUSHED_STANDING_ROOM_ONLY",
    5: "FULL",
    6: "NOT_ACCEPTING_PASSENGERS",
}

_ALERT_CAUSE = {
    0: "UNKNOWN_CAUSE",
    1: "OTHER_CAUSE",
    2: "TECHNICAL_PROBLEM",
    3: "STRIKE",
    4: "DEMONSTRATION",
    5: "ACCIDENT",
    6: "HOLIDAY",
    7: "WEATHER",
    8: "MAINTENANCE",
    9: "CONSTRUCTION",
    10: "POLICE_ACTIVITY",
    11: "MEDICAL_EMERGENCY",
}

_ALERT_EFFECT = {
    0: "UNKNOWN_EFFECT",
    1: "NO_SERVICE",
    2: "REDUCED_SERVICE",
    3: "SIGNIFICANT_DELAYS",
    4: "DETOUR",
    5: "ADDITIONAL_SERVICE",
    6: "MODIFIED_SERVICE",
    7: "OTHER_EFFECT",
    8: "STOP_MOVED",
}

_ALERT_SEVERITY = {
    0: "UNKNOWN_SEVERITY",
    1: "INFO",
    2: "WARNING",
    3: "SEVERE",
}


def _enum_name(mapping: dict[int, str], value: Any) -> str | None:
    try:
        return mapping.get(int(value))
    except (TypeError, ValueError):
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Manual Protobuf Parser
# ═══════════════════════════════════════════════════════════════════════════════


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while offset < len(data):
        byte = data[offset]
        result |= (byte & 0x7F) << shift
        offset += 1
        if not (byte & 0x80):
            break
        shift += 7
    return result, offset


def _read_tag(data: bytes, offset: int) -> tuple[int, int, int]:
    tag, offset = _read_varint(data, offset)
    field_number = tag >> 3
    wire_type = tag & 0x07
    return field_number, wire_type, offset


def _decode_sint32(value: int) -> int:
    if value > 0x7FFFFFFF:
        value -= 0x100000000
    return value


def _parse_message(data: bytes) -> dict[int, Any]:
    result: dict[int, Any] = {}
    offset = 0
    while offset < len(data):
        try:
            field_number, wire_type, offset = _read_tag(data, offset)
        except Exception:
            break

        if wire_type == 0:
            value, offset = _read_varint(data, offset)
        elif wire_type == 1:
            if offset + 8 > len(data):
                break
            value = struct.unpack_from("<d", data, offset)[0]
            offset += 8
        elif wire_type == 2:
            length, offset = _read_varint(data, offset)
            if offset + length > len(data):
                break
            value = data[offset : offset + length]
            offset += length
        elif wire_type == 5:
            if offset + 4 > len(data):
                break
            value = struct.unpack_from("<f", data, offset)[0]
            offset += 4
        else:
            break

        if field_number in result:
            existing = result[field_number]
            if isinstance(existing, list):
                existing.append(value)
            else:
                result[field_number] = [existing, value]
        else:
            result[field_number] = value

    return result


def _as_message(value: Any) -> dict[int, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, bytes):
        try:
            return _parse_message(value)
        except Exception:
            return {}
    return {}


def _as_string(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        return value
    return str(value)


def _as_int(value: Any, default: int = 0) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return default


def _as_entry_list(value: Any) -> list[dict[int, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [_as_message(item) for item in value]
    return [_as_message(value)]


def _read_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _read_float_pair(value: Any) -> tuple[float | None, float | None]:
    if isinstance(value, list) and len(value) >= 2:
        return _read_float(value[0]), _read_float(value[1])
    if isinstance(value, (int, float)):
        return float(value), None
    return None, None


# ═══════════════════════════════════════════════════════════════════════════════
# Feed-Specific Parsers (Manual)
# ═══════════════════════════════════════════════════════════════════════════════


def _parse_trip_update_entity(
    tu_msg: dict[int, Any], entity_id: str, feed_timestamp: int
) -> list[dict[str, Any]]:
    trip_desc = _as_message(tu_msg.get(1))

    base = {
        "event_id": entity_id,
        "feed_type": "trip_update",
        "feed_timestamp": feed_timestamp,
        "entity_id": entity_id,
        "trip_id": _as_string(trip_desc.get(1)),
        "route_id": _as_string(trip_desc.get(5)),
        "start_time": _as_string(trip_desc.get(3)),
        "start_date": _as_string(trip_desc.get(4)),
        "schedule_relationship": _enum_name(
            _SCHEDULE_RELATIONSHIP, trip_desc.get(6)
        ),
        "delay_seconds": None,
        "stop_sequence": None,
        "stop_id": None,
    }

    stop_updates = _as_entry_list(tu_msg.get(2))

    if not stop_updates:
        return [base]

    results: list[dict[str, Any]] = []
    for i, stop_update in enumerate(stop_updates):
        entry = dict(base)
        entry["event_id"] = f"{entity_id}_{i}"
        entry["stop_sequence"] = _as_int(stop_update.get(1))
        entry["stop_id"] = _as_string(stop_update.get(4))
        entry["schedule_relationship"] = _enum_name(
            _SCHEDULE_RELATIONSHIP, stop_update.get(5)
        )

        arrival = _as_message(stop_update.get(2))
        departure = _as_message(stop_update.get(3))
        delay = None
        if 2 in arrival:
            delay = _decode_sint32(_as_int(arrival[2]))
        if delay is None and 2 in departure:
            delay = _decode_sint32(_as_int(departure[2]))
        entry["delay_seconds"] = delay

        results.append(entry)

    return results


def _parse_vehicle_position_entity(
    vp_msg: dict[int, Any], entity_id: str, feed_timestamp: int
) -> dict[str, Any]:
    trip_desc = _as_message(vp_msg.get(1))
    vehicle_desc = _as_message(vp_msg.get(3))
    position = _as_message(vp_msg.get(2))

    return {
        "event_id": entity_id,
        "feed_type": "vehicle_position",
        "feed_timestamp": feed_timestamp,
        "entity_id": entity_id,
        "trip_id": _as_string(trip_desc.get(1)),
        "route_id": _as_string(trip_desc.get(5)),
        "vehicle_id": _as_string(vehicle_desc.get(1)),
        "latitude": _read_float(position.get(1)),
        "longitude": _read_float(position.get(2)),
        "bearing": _read_float(position.get(3)),
        "speed": _read_float(position.get(5)),
        "current_stop_sequence": _as_int(vp_msg.get(4)) if vp_msg.get(4) is not None else None,
        "stop_id": _as_string(vp_msg.get(7)),
        "current_status": _enum_name(_VEHICLE_STOP_STATUS, vp_msg.get(5)),
        "congestion_level": _enum_name(_CONGESTION_LEVEL, vp_msg.get(8)),
        "occupancy_status": _enum_name(_OCCUPANCY_STATUS, vp_msg.get(9)),
    }


def _parse_alert_entity(
    alert_msg: dict[int, Any], entity_id: str, feed_timestamp: int
) -> dict[str, Any]:
    header_text = ""
    header_msg = _as_message(alert_msg.get(10))
    if header_msg:
        translations = _as_entry_list(header_msg.get(1))
        if translations:
            header_text = _as_string(translations[0].get(1))

    description_text = ""
    description_msg = _as_message(alert_msg.get(11))
    if description_msg:
        translations = _as_entry_list(description_msg.get(1))
        if translations:
            description_text = _as_string(translations[0].get(1))

    informed_entities = _as_entry_list(alert_msg.get(5))
    affected_types: list[str] = []
    affected_ids: list[str] = []
    for selector in informed_entities:
        if 1 in selector:
            affected_types.append("agency")
            affected_ids.append(_as_string(selector.get(1)))
        if 2 in selector:
            affected_types.append("route")
            affected_ids.append(_as_string(selector.get(2)))
        if 5 in selector:
            affected_types.append("stop")
            affected_ids.append(_as_string(selector.get(5)))

    return {
        "event_id": entity_id,
        "feed_type": "alert",
        "feed_timestamp": feed_timestamp,
        "entity_id": entity_id,
        "cause": _enum_name(_ALERT_CAUSE, alert_msg.get(6)),
        "effect": _enum_name(_ALERT_EFFECT, alert_msg.get(7)),
        "header_text": header_text,
        "description_text": description_text,
        "severity_level": _enum_name(_ALERT_SEVERITY, alert_msg.get(12)),
        "affected_entity_type": ",".join(affected_types) if affected_types else None,
        "affected_entity_id": ",".join(affected_ids) if affected_ids else None,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# GTFS-RT Bindings Parsing (Fallback Path)
# ═══════════════════════════════════════════════════════════════════════════════


def _parse_from_bindings(feed: Any, feed_type: str) -> list[dict[str, Any]]:
    feed_timestamp = getattr(feed.header, "timestamp", 0) or 0
    entities: list[dict[str, Any]] = []

    for entity in feed.entity:
        entity_id = str(entity.id)

        if feed_type == "trip_update" and entity.HasField("trip_update"):
            tu = entity.trip_update
            trip = tu.trip
            base_trip = {
                "trip_id": trip.trip_id or None,
                "route_id": trip.route_id or None,
                "start_time": trip.start_time or None,
                "start_date": trip.start_date or None,
                "schedule_relationship": _SCHEDULE_RELATIONSHIP.get(
                    trip.schedule_relationship
                ),
            }
            su_count = len(tu.stop_time_update) if tu.stop_time_update else 0
            if su_count == 0:
                entities.append(
                    {
                        "event_id": entity_id,
                        "feed_type": "trip_update",
                        "feed_timestamp": feed_timestamp,
                        "entity_id": entity_id,
                        **base_trip,
                        "delay_seconds": None,
                        "stop_sequence": None,
                        "stop_id": None,
                    }
                )
            else:
                for idx, stu in enumerate(tu.stop_time_update):
                    delay = None
                    if stu.HasField("arrival") and stu.arrival.HasField("delay"):
                        delay = stu.arrival.delay
                    elif stu.HasField("departure") and stu.departure.HasField("delay"):
                        delay = stu.departure.delay
                    entities.append(
                        {
                            "event_id": f"{entity_id}_{idx}",
                            "feed_type": "trip_update",
                            "feed_timestamp": feed_timestamp,
                            "entity_id": entity_id,
                            **base_trip,
                            "delay_seconds": delay,
                            "stop_sequence": stu.stop_sequence or None,
                            "stop_id": stu.stop_id or None,
                            "schedule_relationship": _SCHEDULE_RELATIONSHIP.get(
                                stu.schedule_relationship
                            )
                            or base_trip["schedule_relationship"],
                        }
                    )

        elif feed_type == "vehicle_position" and entity.HasField("vehicle"):
            vp = entity.vehicle
            entities.append(
                {
                    "event_id": entity_id,
                    "feed_type": "vehicle_position",
                    "feed_timestamp": feed_timestamp,
                    "entity_id": entity_id,
                    "trip_id": vp.trip.trip_id if vp.HasField("trip") else None,
                    "route_id": vp.trip.route_id if vp.HasField("trip") else None,
                    "vehicle_id": vp.vehicle.id
                    if vp.HasField("vehicle")
                    else None,
                    "latitude": vp.position.latitude
                    if vp.HasField("position")
                    else None,
                    "longitude": vp.position.longitude
                    if vp.HasField("position")
                    else None,
                    "bearing": vp.position.bearing
                    if vp.HasField("position") and vp.position.HasField("bearing")
                    else None,
                    "speed": vp.position.speed
                    if vp.HasField("position") and vp.position.HasField("speed")
                    else None,
                    "current_stop_sequence": vp.current_stop_sequence or None,
                    "stop_id": vp.stop_id or None,
                    "current_status": _VEHICLE_STOP_STATUS.get(vp.current_status),
                    "congestion_level": _CONGESTION_LEVEL.get(vp.congestion_level),
                    "occupancy_status": _OCCUPANCY_STATUS.get(vp.occupancy_status),
                }
            )

        elif feed_type == "alert" and entity.HasField("alert"):
            alert = entity.alert
            header_text = (
                alert.header_text.translation[0].text
                if alert.HasField("header_text") and alert.header_text.translation
                else ""
            )
            description_text = (
                alert.description_text.translation[0].text
                if alert.HasField("description_text")
                and alert.description_text.translation
                else ""
            )
            affected_types: list[str] = []
            affected_ids: list[str] = []
            for selector in alert.informed_entity:
                if selector.HasField("agency_id"):
                    affected_types.append("agency")
                    affected_ids.append(selector.agency_id)
                if selector.HasField("route_id"):
                    affected_types.append("route")
                    affected_ids.append(selector.route_id)
                if selector.HasField("stop_id"):
                    affected_types.append("stop")
                    affected_ids.append(selector.stop_id)

            entities.append(
                {
                    "event_id": entity_id,
                    "feed_type": "alert",
                    "feed_timestamp": feed_timestamp,
                    "entity_id": entity_id,
                    "cause": _ALERT_CAUSE.get(alert.cause),
                    "effect": _ALERT_EFFECT.get(alert.effect),
                    "header_text": header_text,
                    "description_text": description_text,
                    "severity_level": _ALERT_SEVERITY.get(alert.severity_level),
                    "affected_entity_type": (
                        ",".join(affected_types) if affected_types else None
                    ),
                    "affected_entity_id": (
                        ",".join(affected_ids) if affected_ids else None
                    ),
                }
            )

    return entities


# ═══════════════════════════════════════════════════════════════════════════════
# Manual Protobuf Parser Entry Point
# ═══════════════════════════════════════════════════════════════════════════════


def _parse_protobuf_manual(data: bytes, feed_type: str) -> list[dict[str, Any]]:
    feed_message = _parse_message(data)
    header = _as_message(feed_message.get(1))
    feed_timestamp = _as_int(header.get(3))

    entities_raw = feed_message.get(2)
    if entities_raw is None:
        return []

    entity_list = _as_entry_list(entities_raw)
    entities: list[dict[str, Any]] = []

    for entity_msg in entity_list:
        entity_id = _as_string(entity_msg.get(1))

        if feed_type == "trip_update" and 2 in entity_msg:
            tu_msg = _as_message(entity_msg[2])
            entities.extend(
                _parse_trip_update_entity(tu_msg, entity_id, feed_timestamp)
            )
        elif feed_type == "vehicle_position" and 3 in entity_msg:
            vp_msg = _as_message(entity_msg[3])
            entities.append(
                _parse_vehicle_position_entity(vp_msg, entity_id, feed_timestamp)
            )
        elif feed_type == "alert" and 4 in entity_msg:
            alert_msg = _as_message(entity_msg[4])
            entities.append(
                _parse_alert_entity(alert_msg, entity_id, feed_timestamp)
            )

    return entities


# ═══════════════════════════════════════════════════════════════════════════════
# Public Parsing API
# ═══════════════════════════════════════════════════════════════════════════════


def parse_gtfs_feed(
    protobuf_data: bytes, feed_type: str
) -> list[dict[str, Any]]:
    """Parse GTFS-RT protobuf binary data into a list of normalized entity dicts.

    Tries to use ``gtfs_realtime_bindings`` (the official Google package), then
    falls back to a manual protobuf parser.
    """
    try:
        from google.transit import gtfs_realtime_pb2

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(protobuf_data)
        return _parse_from_bindings(feed, feed_type)
    except ImportError:
        pass
    except Exception as exc:
        print(f"  bindings parser failed ({exc}), falling back to manual parser")

    return _parse_protobuf_manual(protobuf_data, feed_type)


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP Fetching
# ═══════════════════════════════════════════════════════════════════════════════


def fetch_gtfs_feed(url: str, api_key: str | None = None) -> bytes:
    """Fetch a GTFS-RT Protocol Buffer feed via HTTP GET.

    Args:
        url: The feed endpoint URL.
        api_key: Optional API key passed via ``x-api-key`` header
                 or ``Authorization: Bearer <key>`` depending on the
                 endpoint requirements.

    Returns:
        Raw protobuf bytes from the response body.
    """
    headers: dict[str, str] = {}
    if api_key:
        headers["x-api-key"] = api_key

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.content


# ═══════════════════════════════════════════════════════════════════════════════
# Polling & Landing
# ═══════════════════════════════════════════════════════════════════════════════


def poll_gtfs_stream(config: GTFSRealtimeConfig) -> GTFSRealtimeResult:
    """Main polling loop for a GTFS Realtime feed.

    1. Fetch protobuf data from ``feed_url``.
    2. Parse into entity dicts.
    3. Write to the canonical raw envelope landing zone.
    4. Sleep for ``poll_interval_seconds``.

    Stops after ``max_iterations`` (0 = unlimited) or on ``KeyboardInterrupt``.
    """
    result = GTFSRealtimeResult()
    run_id = f"stream_gtfs_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"

    iteration = 0
    while config.max_iterations == 0 or iteration < config.max_iterations:
        iteration += 1
        try:
            proto_data = fetch_gtfs_feed(config.feed_url, config.api_key)
            entities = parse_gtfs_feed(proto_data, config.feed_type)
            result.entities_parsed += len(entities)

            if entities:
                capped = entities[: config.max_entities_per_feed]

                context = EnvelopeContext(
                    dataset_id=config.dataset,
                    source_id=config.source_key,
                    ingestion_type="stream_gtfs_realtime",
                    source_key=config.source_key,
                    run_id=run_id,
                )

                raw_path = write_raw_envelopes(
                    capped,
                    context,
                    normalize_payload=True,
                )
                result.landed += len(capped)
                result.raw_paths.append(str(raw_path))
                print(
                    f"[{iteration}] {config.feed_type} — "
                    f"parsed={len(entities)} landed={len(capped)} "
                    f"-> {raw_path}"
                )
            else:
                print(
                    f"[{iteration}] {config.feed_type} — "
                    f"no entities found in feed"
                )

            result.iterations += 1
        except KeyboardInterrupt:
            print("\nPolling interrupted by user.")
            break
        except requests.RequestException as exc:
            msg = f"iteration {iteration}: HTTP error — {exc}"
            print(f"  {msg}")
            result.errors.append(msg)
        except Exception as exc:
            msg = f"iteration {iteration}: {type(exc).__name__} — {exc}"
            print(f"  {msg}")
            result.errors.append(msg)

        if config.max_iterations == 0 or iteration < config.max_iterations:
            time.sleep(config.poll_interval_seconds)

    return result


def run_gtfs_stream(
    source_key: str,
    dataset: str,
    feed_url: str,
    feed_type: str = "trip_update",
    api_key: str | None = None,
    poll_interval: float = 60.0,
    max_iterations: int = 0,
    max_entities_per_feed: int = 100,
) -> GTFSRealtimeResult:
    """Convenience function that builds a config and runs the polling loop."""
    config = GTFSRealtimeConfig(
        source_key=source_key,
        dataset=dataset,
        feed_url=feed_url,
        feed_type=feed_type,
        api_key=api_key,
        poll_interval_seconds=poll_interval,
        max_iterations=max_iterations,
        max_entities_per_feed=max_entities_per_feed,
    )
    return poll_gtfs_stream(config)


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Poll a GTFS Realtime feed and land entities to the raw layer."
    )
    parser.add_argument("--source", required=True, help="Source key identifier.")
    parser.add_argument("--dataset", required=True, help="Dataset name for raw layer.")
    parser.add_argument("--feed-url", required=True, help="GTFS-RT protobuf feed URL.")
    parser.add_argument(
        "--feed-type",
        choices=["trip_update", "vehicle_position", "alert"],
        default="trip_update",
        help="GTFS-RT feed type (default: trip_update).",
    )
    parser.add_argument("--api-key", default=None, help="Optional API key for auth header.")
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=60.0,
        help="Polling interval in seconds (default: 60.0).",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=0,
        help="Max poll iterations (0 = unlimited).",
    )
    parser.add_argument(
        "--max-entities",
        type=int,
        default=100,
        help="Max entities to land per feed (default: 100).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    print(f"GTFS Realtime Ingestion: source={args.source} dataset={args.dataset}")
    print(f"  feed_url: {args.feed_url}")
    print(f"  feed_type: {args.feed_type}")
    if args.api_key:
        print(f"  api_key: <set>")
    print(f"  poll_interval: {args.poll_interval}s")
    print(f"  max_iterations: {args.max_iterations or 'unlimited'}")
    print(f"  max_entities: {args.max_entities}")

    result = run_gtfs_stream(
        source_key=args.source,
        dataset=args.dataset,
        feed_url=args.feed_url,
        feed_type=args.feed_type,
        api_key=args.api_key,
        poll_interval=args.poll_interval,
        max_iterations=args.max_iterations,
        max_entities_per_feed=args.max_entities,
    )

    print()
    print(f"Iterations:      {result.iterations}")
    print(f"Entities parsed: {result.entities_parsed}")
    print(f"Landed:          {result.landed}")
    if result.errors:
        print(f"Errors:          {len(result.errors)}")
        for error in result.errors:
            print(f"  - {error}")
    if result.raw_paths:
        encoded = str(result.raw_paths[-1]).encode("ascii", "replace").decode("ascii")
        print(f"Last raw path:   {encoded}")

    return 0 if result.landed > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
