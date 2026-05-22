from __future__ import annotations

from typing import Any

from ingestion.downloaders.core import DownloadContext, SourceFailure, SourceRun
from ingestion.downloaders.http import request_json, require_env
from ingestion.downloaders.utils import (
    extract_records,
    limit_items,
    month_ranges,
    sanitize_segment,
    source_options,
)


NCEI_REFERENCE_ENDPOINTS = {
    "datasets",
    "datacategories",
    "datatypes",
    "locationcategories",
    "locations",
}


def download_ncei(run: SourceRun, context: DownloadContext) -> None:
    env = require_env(run, "NCEI_API_TOKEN")
    opts = source_options(context, "ncei")
    base = str(opts.get("base_url", "https://www.ncei.noaa.gov/cdo-web/api/v2")).rstrip("/")
    headers = {"token": env["NCEI_API_TOKEN"]}
    dataset_id = str(opts.get("dataset_id", "GHCND"))
    datatypes = [str(value) for value in opts.get("datatypes", ["TMAX", "TMIN", "TAVG", "PRCP", "AWND"])]
    page_limit = min(int(opts.get("page_limit", 1000)), 1000)
    units = str(opts.get("units", "metric"))

    download_ncei_reference_metadata(run, context, base, headers, dataset_id, datatypes, page_limit, opts)

    station_ids = discover_ncei_station_ids(run, context, base, headers, dataset_id, datatypes, page_limit)
    if not station_ids:
        station_ids = list(opts.get("fallback_station_ids", []))
    station_limit = context.mode.get("ncei_station_limit")
    station_ids = limit_items(station_ids, int(station_limit) if station_limit is not None else None)
    if not station_ids:
        raise SourceFailure("No NCEI station ids discovered or configured.")

    for station_id in station_ids:
        download_ncei_json_chunk(
            run,
            base,
            headers,
            f"stations/{station_id}",
            {},
            chunk_id=f"ncei:station_detail:{station_id}",
            relative_path=f"discovery/station_details/station={sanitize_segment(station_id)}.json",
        )
        for start, end in month_ranges(context.mode["core_start"], context.mode["core_end"]):
            download_ncei_data_window(
                run,
                base,
                headers,
                dataset_id=dataset_id,
                station_id=station_id,
                datatypes=datatypes,
                startdate=start.isoformat(),
                enddate=end.isoformat(),
                units=units,
                page_limit=page_limit,
            )


def download_ncei_reference_metadata(
    run: SourceRun,
    context: DownloadContext,
    base: str,
    headers: dict[str, str],
    dataset_id: str,
    datatypes: list[str],
    page_limit: int,
    opts: dict[str, Any],
) -> None:
    configured = opts.get("reference_endpoints", list(NCEI_REFERENCE_ENDPOINTS))
    endpoints = [str(endpoint) for endpoint in configured if str(endpoint) in NCEI_REFERENCE_ENDPOINTS]
    common_params = {
        "datasetid": dataset_id,
        "startdate": context.mode["core_start"],
        "enddate": context.mode["core_end"],
    }

    if "datasets" in endpoints:
        download_ncei_json_chunk(
            run,
            base,
            headers,
            f"datasets/{dataset_id}",
            {},
            chunk_id=f"ncei:dataset:{dataset_id}",
            relative_path=f"reference/datasets/dataset={sanitize_segment(dataset_id)}.json",
        )
        download_ncei_collection(
            run,
            base,
            headers,
            "datasets",
            {
                "datatypeid": datatypes,
                "startdate": context.mode["core_start"],
                "enddate": context.mode["core_end"],
                "sortfield": "name",
            },
            f"reference/datasets/list_dataset={sanitize_segment(dataset_id)}",
            f"ncei:datasets:list:{dataset_id}",
            page_limit,
        )

    if "datacategories" in endpoints:
        download_ncei_collection(
            run,
            base,
            headers,
            "datacategories",
            {**common_params, "sortfield": "name"},
            f"reference/datacategories/dataset={sanitize_segment(dataset_id)}",
            f"ncei:datacategories:{dataset_id}",
            page_limit,
        )

    if "datatypes" in endpoints:
        download_ncei_collection(
            run,
            base,
            headers,
            "datatypes",
            {**common_params, "sortfield": "name"},
            f"reference/datatypes/dataset={sanitize_segment(dataset_id)}",
            f"ncei:datatypes:{dataset_id}",
            page_limit,
        )
        for datatype in datatypes:
            download_ncei_json_chunk(
                run,
                base,
                headers,
                f"datatypes/{datatype}",
                {},
                chunk_id=f"ncei:datatype:{datatype}",
                relative_path=f"reference/datatypes/detail/datatype={sanitize_segment(datatype)}.json",
            )

    if "locationcategories" in endpoints:
        download_ncei_collection(
            run,
            base,
            headers,
            "locationcategories",
            {
                "datasetid": dataset_id,
                "startdate": context.mode["core_start"],
                "enddate": context.mode["core_end"],
                "sortfield": "name",
            },
            f"reference/locationcategories/dataset={sanitize_segment(dataset_id)}",
            f"ncei:locationcategories:{dataset_id}",
            page_limit,
        )

    if "locations" in endpoints:
        download_ncei_collection(
            run,
            base,
            headers,
            "locations",
            {**common_params, "sortfield": "name"},
            f"reference/locations/dataset={sanitize_segment(dataset_id)}",
            f"ncei:locations:{dataset_id}",
            page_limit,
        )


def discover_ncei_station_ids(
    run: SourceRun,
    context: DownloadContext,
    base: str,
    headers: dict[str, str],
    dataset_id: str,
    datatypes: list[str],
    page_limit: int,
) -> list[str]:
    bbox = context.bbox
    params = {
        "datasetid": dataset_id,
        "datatypeid": datatypes,
        "extent": f"{bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']}",
        "startdate": context.mode["core_start"],
        "enddate": context.mode["core_end"],
        "sortfield": "name",
    }
    try:
        stations = download_ncei_collection(
            run,
            base,
            headers,
            "stations",
            params,
            "discovery/stations",
            f"ncei:stations:{dataset_id}:{context.mode['core_start']}:{context.mode['core_end']}",
            page_limit,
        )
    except Exception as exc:
        run.mark_failed("station_discovery", str(exc))
        stations = []
    return [str(station.get("id")) for station in stations if station.get("id")]


def download_ncei_data_window(
    run: SourceRun,
    base: str,
    headers: dict[str, str],
    *,
    dataset_id: str,
    station_id: str,
    datatypes: list[str],
    startdate: str,
    enddate: str,
    units: str,
    page_limit: int,
) -> None:
    chunk_id = f"ncei:data:{station_id}:{startdate}:{enddate}"
    if run.should_skip(chunk_id):
        return
    params = {
        "datasetid": dataset_id,
        "stationid": station_id,
        "datatypeid": datatypes,
        "startdate": startdate,
        "enddate": enddate,
        "limit": page_limit,
        "units": units,
    }
    rel_prefix = f"station={sanitize_segment(station_id)}/year={startdate[:4]}/month={startdate[5:7]}"
    try:
        records = download_ncei_collection(
            run,
            base,
            headers,
            "data",
            params,
            rel_prefix,
            chunk_id,
            page_limit,
        )
        run.mark_complete(chunk_id, {"record_count": len(records)})
    except Exception as exc:
        run.mark_failed(chunk_id, str(exc))


def download_ncei_collection(
    run: SourceRun,
    base: str,
    headers: dict[str, str],
    endpoint: str,
    params: dict[str, Any],
    relative_prefix: str,
    chunk_id: str,
    page_limit: int,
) -> list[dict[str, Any]]:
    if run.should_skip(chunk_id):
        return []
    all_records: list[dict[str, Any]] = []
    offset = 1
    while True:
        payload = request_json(
            run,
            f"{base}/{endpoint}",
            headers=headers,
            params={**params, "limit": page_limit, "offset": offset},
        )
        records = extract_records(payload)
        if records:
            run.write_jsonl(f"{relative_prefix}/offset={offset:06d}.jsonl", records)
            all_records.extend(records)
        resultset = ncei_resultset(payload)
        count = int(resultset.get("count") or 0) if resultset else 0
        current_offset = int(resultset.get("offset") or offset) if resultset else offset
        current_limit = int(resultset.get("limit") or page_limit) if resultset else page_limit
        if not records or len(records) < page_limit:
            break
        if count and current_offset + current_limit > count:
            break
        offset = current_offset + current_limit
    if endpoint != "data":
        run.mark_complete(chunk_id, {"record_count": len(all_records)})
    return all_records


def download_ncei_json_chunk(
    run: SourceRun,
    base: str,
    headers: dict[str, str],
    endpoint: str,
    params: dict[str, Any],
    *,
    chunk_id: str,
    relative_path: str,
) -> Any | None:
    if run.should_skip(chunk_id):
        return None
    try:
        payload = request_json(run, f"{base}/{endpoint}", headers=headers, params=params)
        path = run.write_json(relative_path, payload, record_count=1)
        run.mark_complete(chunk_id, {"record_count": 1, "path": str(path)})
        return payload
    except Exception as exc:
        run.mark_failed(chunk_id, str(exc))
        return None


def ncei_resultset(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        return {}
    resultset = metadata.get("resultset")
    return resultset if isinstance(resultset, dict) else {}
