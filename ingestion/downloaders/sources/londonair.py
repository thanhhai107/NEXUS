from __future__ import annotations

import json
import os
from datetime import date
from typing import Any, Iterable
from urllib.parse import quote

from ingestion.downloaders.core import DownloadContext, SourceFailure, SourceRun
from ingestion.downloaders.http import request_json
from ingestion.downloaders.utils import (
    estimate_record_count,
    iter_dicts,
    limit_items,
    month_ranges,
    parse_ymd,
    poll_time_slug,
    sanitize_segment,
    source_options,
)


def download_londonair(run: SourceRun, context: DownloadContext) -> None:
    opts = source_options(context, "londonair")
    base = (os.environ.get("LONDONAIR_API_BASE_URL") or opts.get("base_url") or "").rstrip("/")
    if not base:
        raise SourceFailure("LondonAir base URL is not configured.")

    poll_time = poll_time_slug(context)
    species_codes = context.mode.get("londonair_species") or ["NO2", "PM25", "PM10", "O3"]

    sites_payload = download_londonair_json_chunk(
        run,
        base,
        londonair_endpoint(
            opts,
            "LONDONAIR_MONITORING_SITES_ENDPOINT",
            "monitoring_sites",
            "/Information/MonitoringSites/GroupName=London/Json",
        ),
        chunk_id="londonair:metadata:monitoring_sites",
        relative_path="metadata/monitoring_sites.json",
        critical=True,
    )
    site_species_payload = download_londonair_json_chunk(
        run,
        base,
        londonair_endpoint(
            opts,
            "LONDONAIR_SITE_SPECIES_ENDPOINT",
            "site_species",
            "/Information/MonitoringSiteSpecies/GroupName=London/Json",
        ),
        chunk_id="londonair:metadata:site_species",
        relative_path="metadata/site_species_mapping.json",
        critical=True,
    )
    download_londonair_json_chunk(
        run,
        base,
        londonair_endpoint(
            opts,
            "LONDONAIR_LOCAL_AUTHORITIES_ENDPOINT",
            "local_authorities",
            "/Information/MonitoringLocalAuthority/GroupName=London/Json",
        ),
        chunk_id="londonair:metadata:local_authorities",
        relative_path="metadata/local_authorities.json",
    )

    for species_code in species_codes:
        download_londonair_json_chunk(
            run,
            base,
            londonair_endpoint(
                opts,
                "LONDONAIR_SPECIES_METADATA_ENDPOINT",
                "species_metadata",
                "/Information/Species/SpeciesCode={SpeciesCode}/Json",
            ),
            path_values={"SpeciesCode": species_code},
            chunk_id=f"londonair:metadata:species:{species_code}",
            relative_path=f"metadata/species/species={sanitize_segment(species_code)}.json",
        )

    download_londonair_json_chunk(
        run,
        base,
        londonair_endpoint(
            opts,
            "LONDONAIR_HOURLY_INDEX_ENDPOINT",
            "hourly_index",
            "/Hourly/MonitoringIndex/GroupName=London/Json",
        ),
        chunk_id=f"londonair:realtime:hourly_index:{poll_time['stamp']}",
        relative_path=f"realtime/date={poll_time['date']}/hour={poll_time['hour']}/hourly_monitoring_index.json",
    )
    download_londonair_json_chunk(
        run,
        base,
        londonair_endpoint(
            opts,
            "LONDONAIR_DAILY_INDEX_ENDPOINT",
            "daily_index",
            "/Daily/MonitoringIndex/Latest/GroupName=London/Json",
        ),
        chunk_id=f"londonair:realtime:daily_index:{poll_time['date']}",
        relative_path=f"realtime/date={poll_time['date']}/daily_monitoring_index.json",
    )

    health_advice_payloads: list[Any] = []
    for index in opts.get("health_advice_indexes", list(range(1, 11))):
        payload = download_londonair_json_chunk(
            run,
            base,
            londonair_endpoint(
                opts,
                "LONDONAIR_HEALTH_ADVICE_BY_INDEX_ENDPOINT",
                "health_advice_by_index",
                "/Information/IndexHealthAdvice/AirQualityIndex={AirQualityIndex}/Json",
            ),
            path_values={"AirQualityIndex": index},
            chunk_id=f"londonair:metadata:health_advice:index={index}",
            relative_path=f"metadata/health_advice/index={int(index):02d}.json",
        )
        if payload is not None:
            health_advice_payloads.append(payload)
    health_advice_chunk_id = "londonair:metadata:health_advice:combined"
    if health_advice_payloads and not run.should_skip(health_advice_chunk_id):
        path = run.write_json(
            "metadata/health_advice.json",
            {"IndexHealthAdvice": health_advice_payloads},
            record_count=len(health_advice_payloads),
        )
        run.mark_complete(health_advice_chunk_id, {"record_count": len(health_advice_payloads), "path": str(path)})

    download_londonair_json_chunk(
        run,
        base,
        londonair_endpoint(
            opts,
            "LONDONAIR_AIR_POLLUTION_GUIDE_ENDPOINT",
            "air_pollution_guide",
            "/Information/AirPollutionGuide/Json",
        ),
        chunk_id="londonair:metadata:air_pollution_guide",
        relative_path="metadata/air_pollution_guide.json",
    )

    active_on = parse_ymd(context.mode["core_end"])
    site_species_pairs = extract_londonair_site_species_pairs(site_species_payload, species_codes, active_on=active_on)
    site_codes = unique_preserve_order(site_code for site_code, _ in site_species_pairs)
    if not site_codes:
        site_codes = extract_londonair_site_codes(sites_payload)
    if not site_codes:
        site_codes = list(opts.get("fallback_site_codes", []))
    site_limit = context.mode.get("londonair_site_limit")
    site_codes = limit_items(site_codes, int(site_limit) if site_limit is not None else None)
    selected_site_codes = set(site_codes)
    selected_pairs = [(site, species) for site, species in site_species_pairs if site in selected_site_codes]
    if not selected_pairs:
        selected_pairs = [(site, species) for site in site_codes for species in species_codes]

    for site_code in site_codes:
        download_londonair_json_chunk(
            run,
            base,
            londonair_endpoint(
                opts,
                "LONDONAIR_SITE_HOURLY_INDEX_ENDPOINT",
                "site_hourly_index",
                "/Hourly/MonitoringIndex/SiteCode={SiteCode}/Json",
            ),
            path_values={"SiteCode": site_code},
            chunk_id=f"londonair:realtime:site_hourly_index:{site_code}:{poll_time['stamp']}",
            relative_path=(
                f"realtime/date={poll_time['date']}/hour={poll_time['hour']}"
                f"/site={sanitize_segment(site_code)}/hourly_monitoring_index.json"
            ),
        )
        for start, end in month_ranges(context.mode["core_start"], context.mode["core_end"]):
            download_londonair_json_chunk(
                run,
                base,
                londonair_endpoint(
                    opts,
                    "LONDONAIR_WIDE_SITE_DATA_ENDPOINT",
                    "wide_site_data",
                    "/Data/Wide/Site/SiteCode={SiteCode}/StartDate={StartDate}/EndDate={EndDate}/Json",
                ),
                path_values={"SiteCode": site_code, "StartDate": start.isoformat(), "EndDate": end.isoformat()},
                chunk_id=f"londonair:wide:{site_code}:{start:%Y-%m}",
                relative_path=(
                    f"historical_wide/site={sanitize_segment(site_code)}"
                    f"/year={start.year}/month={start.month:02d}.json"
                ),
            )

    for site_code, species_code in selected_pairs:
        for period in opts.get("index_days_periods", ["Year"]):
            download_londonair_json_chunk(
                run,
                base,
                londonair_endpoint(
                    opts,
                    "LONDONAIR_SITE_SPECIES_INDEX_DAYS_ENDPOINT",
                    "site_species_index_days",
                    "/Data/SiteSpeciesIndexDays/SiteCode={SiteCode}/SpeciesCode={SpeciesCode}/Period={Period}/Json",
                ),
                path_values={"SiteCode": site_code, "SpeciesCode": species_code, "Period": period},
                chunk_id=f"londonair:index_days:{site_code}:{species_code}:{period}",
                relative_path=(
                    f"index_days/site={sanitize_segment(site_code)}"
                    f"/species={sanitize_segment(species_code)}/period={sanitize_segment(period)}.json"
                ),
            )

def londonair_endpoint(opts: dict[str, Any], env_name: str, option_key: str, default: str) -> str:
    endpoints = opts.get("endpoints", {})
    configured = os.environ.get(env_name) or (endpoints.get(option_key) if isinstance(endpoints, dict) else None)
    return str(configured or default)

def build_londonair_url(base: str, endpoint: str, path_values: dict[str, Any] | None = None) -> str:
    resolved = endpoint
    for key, value in (path_values or {}).items():
        encoded = quote(str(value), safe="")
        for token in {key, key.upper(), key.lower()}:
            resolved = resolved.replace(f"{{{token}}}", encoded)
    if resolved.startswith(("http://", "https://")):
        return resolved
    return f"{base.rstrip('/')}/{resolved.lstrip('/')}"

def download_londonair_json_chunk(
    run: SourceRun,
    base: str,
    endpoint: str,
    *,
    chunk_id: str,
    relative_path: str,
    path_values: dict[str, Any] | None = None,
    critical: bool = False,
) -> Any | None:
    if run.should_skip(chunk_id):
        existing_path = run.raw_dir / relative_path
        if existing_path.exists():
            try:
                return json.loads(existing_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                return None
        return None
    url = build_londonair_url(base, endpoint, path_values)
    try:
        payload = request_json(run, url)
        record_count = estimate_record_count(payload)
        path = run.write_json(relative_path, payload, record_count=record_count)
        run.mark_complete(chunk_id, {"record_count": record_count, "path": str(path)})
        return payload
    except Exception as exc:
        run.mark_failed(chunk_id, str(exc))
        if critical:
            raise
        return None

def extract_londonair_site_codes(payload: Any) -> list[str]:
    codes: list[str] = []
    for record in iter_dicts(payload):
        for key in ("@SiteCode", "SiteCode", "site_code", "code"):
            value = record.get(key)
            if isinstance(value, str) and value and value not in codes:
                codes.append(value)
    return codes

def extract_londonair_site_species_pairs(
    payload: Any,
    species_codes: list[str],
    *,
    active_on: date | None = None,
) -> list[tuple[str, str]]:
    wanted_species = {str(species_code) for species_code in species_codes}
    pairs: list[tuple[str, str]] = []
    sites = payload.get("Sites", {}).get("Site") if isinstance(payload, dict) else None
    if isinstance(sites, dict):
        sites = [sites]
    if not isinstance(sites, list):
        return pairs
    for site in sites:
        if not isinstance(site, dict):
            continue
        site_code = site.get("@SiteCode")
        if not isinstance(site_code, str) or not site_code:
            continue
        species_records = site.get("Species")
        if isinstance(species_records, dict):
            species_records = [species_records]
        if not isinstance(species_records, list):
            continue
        for species_record in species_records:
            if not isinstance(species_record, dict):
                continue
            species_code = species_record.get("@SpeciesCode")
            if not isinstance(species_code, str) or species_code not in wanted_species:
                continue
            if active_on and not londonair_species_active_on(species_record, active_on):
                continue
            pair = (site_code, species_code)
            if pair not in pairs:
                pairs.append(pair)
    return pairs

def londonair_species_active_on(species_record: dict[str, Any], active_on: date) -> bool:
    started = parse_londonair_date(species_record.get("@DateMeasurementStarted"))
    finished = parse_londonair_date(species_record.get("@DateMeasurementFinished"))
    if started and started > active_on:
        return False
    return not (finished and finished < active_on)

def parse_londonair_date(value: Any) -> date | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None

def unique_preserve_order(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        if value not in result:
            result.append(value)
    return result
