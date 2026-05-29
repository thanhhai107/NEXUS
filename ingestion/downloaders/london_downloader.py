from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    script_dir = Path(__file__).resolve().parent
    sys.path = [
        entry
        for entry in sys.path
        if Path(entry or ".").resolve() != script_dir
    ]
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

from ingestion.base.contracts import SourceSpec
from ingestion.base.core import DownloadContext, SourceRun
from ingestion.base.utils import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_ENV_PATH,
    load_config,
    resolve_mode,
    resolve_output_dir,
    run_id_now,
)
from ingestion.downloaders.raw_adapter import published_run_to_raw_envelope
from ingestion.downloaders.schema_inference import InferredSchema
from ingestion.downloaders.validation import (
    route_parser_failures_to_dlq,
    route_run_failures_to_dlq,
    route_source_failure_to_dlq,
    validate_raw_envelope_file,
)
from ingestion.sources.londonair import download_londonair
from ingestion.sources.ncei import download_ncei
from ingestion.sources.openmeteo import download_openmeteo
from ingestion.sources.openmeteo_historical_weather import download_openmeteo_historical_weather
from ingestion.sources.ukair import download_ukair_air_quality_archive
from ingestion.sources.openaq import download_openaq
from ingestion.sources.waqi import download_waqi
from ingestion.sources.openweather import download_openweather
from ingestion.sources.tfl import download_tfl, download_tfl_arrivals, download_tfl_line_status
from ingestion.sources.stats19 import download_stats19
from ingestion.sources.naptan import download_naptan
from ingestion.sources.dft import download_dft
from ingestion.sources.london_journeys import download_london_journeys

# Semantic annotation
from ingestion.semantic import (
    SemanticAnnotationPipeline,
    AnnotationResult,
)


def find_latest_run_id(output_dir: Path, source_keys: list[str]) -> str | None:
    candidates: list[tuple[float, str]] = []
    for source in source_keys:
        spec = SOURCE_REGISTRY[normalize_source_key(source)]
        source_dir = output_dir / spec.source_id
        if not source_dir.exists():
            continue
        for run_dir in source_dir.glob("run_id=*"):
            if not run_dir.is_dir():
                continue
            metadata_dir = run_dir / "metadata"
            marker_files = [
                metadata_dir / "checkpoint.json",
                metadata_dir / "profile.json",
                metadata_dir / "source_manifest.json",
            ]
            mtimes = [path.stat().st_mtime for path in marker_files if path.exists()]
            if not mtimes:
                mtimes = [run_dir.stat().st_mtime]
            candidates.append((max(mtimes), run_dir.name.removeprefix("run_id=")))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def resolve_source_keys(
    config: dict[str, Any],
    *,
    source_keys: list[str] | None,
    source_group: str | None,
    default_group: str,
) -> list[str]:
    if source_keys:
        return source_keys
    sources = config.get("sources", {})
    resolved_group = source_group or default_group
    if resolved_group == "default":
        resolved_group = str(config.get("default_source_group") or "core_historical")
    if resolved_group == "polling":
        resolved_group = str(config.get("polling_source_group") or "realtime_polling")
    selected = sources.get(resolved_group)
    if not selected:
        available = ", ".join(sorted(sources)) or "<none>"
        raise ValueError(f"Unknown source group: {resolved_group}. Available groups: {available}")
    return list(selected)


def run_source(spec: SourceSpec, context: DownloadContext) -> dict[str, Any]:
    run = SourceRun(spec.source_id, context, spec.source_key, dataset_name=spec.dataset_name)
    print(f"[{spec.source_key}] start source_id={spec.source_id} run_id={context.run_id}")
    try:
        spec.func(run, context)
    except Exception as exc:
        profile = run.finish("failed", str(exc))
        route_source_failure_to_dlq(
            source_id=run.source_id,
            dataset=run.dataset_name,
            run_id=run.run_id,
            error=exc,
        )
        route_run_failures_to_dlq(run)
        print(f"[{spec.source_key}] failed: {exc}")
        return profile
    status = "partial" if run.failed_requests else "success"
    profile = run.finish(status)
    if status == "partial":
        routed = route_run_failures_to_dlq(run)
        if routed:
            print(f"[{spec.source_key}] dlq: routed_failed_chunks={routed}")
    maybe_publish_raw_envelope(run, context)

    # Run schema inference
    inferred_schema = maybe_infer_schema(run, context)
    if inferred_schema:
        profile["schema_inferred"] = True
        profile["inferred_fields"] = len(inferred_schema.fields)

        # Run semantic annotation
        annotation_result = maybe_annotate_semantic(run, context, inferred_schema)
        if annotation_result:
            profile["semantic_annotated"] = True
            profile["annotated_fields"] = len(annotation_result.annotations)
            profile["llm_calls"] = annotation_result.llm_calls

        print(
            f"[{spec.source_key}] {status}: rows={profile.get('row_count', 0)} "
            f"files={profile.get('file_count', 0)} size_mb={profile.get('size_mb', 0)}"
        )
    return profile


def maybe_infer_schema(run: SourceRun, context: DownloadContext) -> InferredSchema | None:
    """Infer schema from downloaded data if enabled.

    Args:
        run: SourceRun instance with downloaded data
        context: Download context

    Returns:
        InferredSchema if inference ran successfully, None otherwise
    """
    # Check if schema inference is enabled
    inference_config = context.config.get("schema_inference", {})
    if not inference_config.get("enabled", False):
        return None

    # Find JSONL files in raw_dir (recursive, excluding metadata)
    jsonl_files = list(run.raw_dir.rglob("*.jsonl"))
    jsonl_files = [f for f in jsonl_files if "metadata" not in str(f)]
    
    if not jsonl_files:
        return None

    # Sort by size (largest first = most data)
    jsonl_files.sort(key=lambda p: p.stat().st_size, reverse=True)
    main_file = jsonl_files[0]

    try:
        from ingestion.downloaders.schema_inference import SchemaInference

        inference = SchemaInference(
            sample_size=inference_config.get("sample_size", 10000)
        )
        schema = inference.infer_from_jsonl(
            file_path=main_file,
            source_id=run.source_id,
            source_key=run.source_key,
            run_id=run.run_id,
        )

        # Save schema to metadata directory
        schema_path = run.metadata_dir / "inferred_schema.json"
        schema.save(schema_path)

        # Save summary
        summary_path = run.metadata_dir / "inferred_schema_summary.json"
        schema.save_summary(summary_path)

        print(
            f"[{run.source_key}] schema inferred: fields={len(schema.fields)} "
            f"records={schema.record_count} path={schema_path}"
        )

        return schema

    except Exception as exc:
        print(f"[{run.source_key}] schema inference failed: {exc}")
        return None


def maybe_annotate_semantic(
    run: SourceRun,
    context: DownloadContext,
    inferred_schema: InferredSchema,
) -> AnnotationResult | None:
    """
    Run semantic annotation on inferred schema if enabled.

    Args:
        run: SourceRun instance with downloaded data
        context: Download context
        inferred_schema: Schema from maybe_infer_schema

    Returns:
        AnnotationResult if annotation ran successfully, None otherwise
    """
    # Check if semantic annotation is enabled
    semantic_config = context.config.get("semantic_annotation", {})
    if not semantic_config.get("enabled", False):
        return None

    try:
        # Get config values
        # semantic_cache goes to runtime/semantic_cache (same level as lake)
        runtime_dir = context.output_dir.parent
        cache_dir = runtime_dir / "semantic_cache"
        llm_model = semantic_config.get("llm_model", "qwen2.5:0.5b")
        llm_url = semantic_config.get("ollama_url", "http://localhost:11434")
        llm_timeout = semantic_config.get("llm_timeout_seconds", 180)
        min_new_fields = semantic_config.get("trigger", {}).get("min_new_fields", 3)
        reannotate_threshold = semantic_config.get("trigger", {}).get("reannotate_threshold", 10)

        # Get docs URL for this source
        docs_urls = semantic_config.get("docs_urls", {})
        docs_url = docs_urls.get(run.source_key)

        # Create pipeline
        pipeline = SemanticAnnotationPipeline(
            cache_dir=cache_dir,
            llm_model=llm_model,
            llm_base_url=llm_url,
            llm_timeout=llm_timeout,
            min_new_fields=min_new_fields,
            reannotate_threshold=reannotate_threshold,
        )

        # Get domain from source
        domain = _get_source_domain(run.source_key, context.config)

        # Run annotation
        result = pipeline.process(
            source_id=run.source_id,
            source_key=run.source_key,
            inferred_schema=inferred_schema,
            docs_url=docs_url,
            domain=domain,
        )

        # Log result with path
        if result.llm_calls > 0:
            print(
                f"[{run.source_key}] semantic annotation: "
                f"fields={result.total_annotations} "
                f"new={result.new_fields_count} "
                f"llm_calls={result.llm_calls}"
            )
        else:
            print(
                f"[{run.source_key}] semantic annotation: "
                f"fields={result.total_annotations} "
                f"from_cache=True llm_calls=0"
            )

        # Log path to annotations file
        if result.annotations:
            cache_path = cache_dir / run.source_key / f"v{result.schema_hash[:4]}" / "annotations.json"
            print(f"[{run.source_key}] semantic metadata: path={cache_path}")

        return result

    except Exception as exc:
        print(f"[{run.source_key}] semantic annotation failed: {exc}")
        return None


def _get_source_domain(source_key: str, config: dict) -> str:
    """Get domain name for a source."""
    # Check source-specific config
    source_config = config.get(source_key, {})
    if isinstance(source_config, dict):
        domain = source_config.get("domain")
        if domain:
            return domain
    
    # Default domains based on source patterns
    if source_key.startswith("tfl"):
        return "transport"
    elif source_key in {"waqi", "openaq", "openmeteo", "londonair", "ncei", "ukair"}:
        return "environment"
    elif source_key in {"stats19", "dft", "naptan", "london_journeys"}:
        return "transport"
    
    return "unknown"


def maybe_publish_raw_envelope(run: SourceRun, context: DownloadContext) -> dict[str, Any] | None:
    publish_policy = dict((context.config.get("resilient_runtime") or {}).get("publish_policy") or {})
    if not bool(publish_policy.get("raw_envelope_enabled", False)):
        return None
    if not run.published_manifest_path.exists():
        return None
    try:
        result = published_run_to_raw_envelope(run.published_manifest_path)
    except Exception as exc:
        route_source_failure_to_dlq(
            source_id=run.source_id,
            dataset=run.dataset_name,
            run_id=run.run_id,
            error=exc,
        )
        raise

    parser_failures = route_parser_failures_to_dlq(
        result.get("parser_failure_details") or [],
        dataset=run.dataset_name,
        source=run.source_id,
        run_id=run.run_id,
    )
    validation_policy = dict((context.config.get("resilient_runtime") or {}).get("validation_policy") or {})
    validation_result = validate_raw_envelope_file(
        dataset=run.dataset_name,
        raw_path=result["raw_path"],
        run_id=run.run_id,
        source=run.source_id,
        schema_validation_enabled=bool(validation_policy.get("schema_validation_enabled", False)),
        quarantine_invalid_records=bool(validation_policy.get("quarantine_invalid_records", True)),
    )
    raw_path_for_log = str(result["raw_path"]).encode("ascii", "replace").decode("ascii")
    print(
        f"[{run.source_key}] raw envelope: path={raw_path_for_log} "
        f"records={result['record_count']} "
        f"parser_failures={result['parser_failures']} "
        f"dlq_parser_failures={parser_failures} "
        f"schema_invalid={validation_result['schema_invalid_records']}"
    )
    result["validation"] = validation_result
    result["dlq_parser_failures"] = parser_failures
    return result


def _date_from_cli(value: str) -> str:
    """Normalize an ISO date/datetime CLI value to YYYY-MM-DD for downloader modes."""
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).date().isoformat()
    except ValueError:
        return datetime.strptime(value[:10], "%Y-%m-%d").date().isoformat()


def _apply_time_window(
    mode: dict[str, Any],
    *,
    start_time: str | None = None,
    end_time: str | None = None,
) -> dict[str, Any]:
    """Override downloader mode date windows from optional backfill CLI args."""
    if not start_time and not end_time:
        return mode

    updated = dict(mode)
    if start_time:
        start_date = _date_from_cli(start_time)
        updated["core_start"] = start_date
        updated["transport_start"] = start_date
        updated["transport_start_year"] = int(start_date[:4])
    if end_time:
        end_date = _date_from_cli(end_time)
        updated["core_end"] = end_date
        updated["transport_end"] = end_date
        updated["transport_end_year"] = int(end_date[:4])
    return updated


def run_once(
    *,
    source_keys: list[str] | None = None,
    source_group: str | None = None,
    mode_name: str | None = None,
    run_id: str | None = None,
    output_dir: Path | None = None,
    config_path: Path | None = None,
    resume: bool = True,
    resume_latest: bool = False,
    overwrite: bool = False,
    poll_time: datetime | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    parallel: bool = False,
    max_workers: int = 4,
) -> list[dict[str, Any]]:
    load_dotenv(DEFAULT_ENV_PATH, override=True)
    config = load_config(config_path)
    resolved_mode_name, mode = resolve_mode(config, mode_name)
    mode = _apply_time_window(mode, start_time=start_time, end_time=end_time)
    if source_group == "small_demo" and resolved_mode_name != "small_demo":
        print(
            "[config] warning: --source-group small_demo is intended to run with "
            "--mode small_demo. Current mode may request full date ranges and hit "
            "external API rate limits."
        )
    resolved_output_dir = resolve_output_dir(config, output_dir)
    source_groups = config.get("sources", {})
    if not isinstance(source_groups, dict):
        source_groups = {}
    default_group = source_group or (
        resolved_mode_name if resolved_mode_name in source_groups else str(config.get("default_source_group") or "core_historical")
    )
    selected = resolve_source_keys(
        config,
        source_keys=source_keys,
        source_group=source_group,
        default_group=default_group,
    )
    resolved_run_id = run_id
    if not resolved_run_id and resume and resume_latest:
        resolved_run_id = find_latest_run_id(resolved_output_dir, selected)
        if resolved_run_id:
            print(f"[resume] using latest run_id={resolved_run_id}")
    resolved_run_id = resolved_run_id or run_id_now()
    context = DownloadContext(
        config=config,
        mode_name=resolved_mode_name,
        mode=mode,
        output_dir=resolved_output_dir,
        run_id=resolved_run_id,
        resume=resume,
        overwrite=overwrite,
        poll_time=poll_time,
    )
    specs = [SOURCE_REGISTRY[normalize_source_key(source)] for source in selected]

    if parallel and len(specs) > 1:
        return _run_parallel(specs, context, max_workers)
    else:
        return [run_source(spec, context) for spec in specs]


def _run_parallel(
    specs: list[SourceSpec],
    context: DownloadContext,
    max_workers: int,
) -> list[dict[str, Any]]:
    """Run sources in parallel using ThreadPoolExecutor.

    Args:
        specs: List of source specs to run
        context: Download context
        max_workers: Maximum number of concurrent workers

    Returns:
        List of profiles from all sources
    """
    results: list[dict[str, Any]] = []
    start_time = datetime.now(timezone.utc)

    print(f"[parallel] starting {len(specs)} sources with {max_workers} workers at {start_time.isoformat()}")

    # Wrap specs with worker info
    wrapped_specs = [
        (spec, (idx % max_workers) + 1) for idx, spec in enumerate(specs)
    ]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_spec = {
            executor.submit(run_source, spec, context): (spec, worker_id)
            for spec, worker_id in wrapped_specs
        }

        # Collect results as they complete
        for future in as_completed(future_to_spec):
            spec, worker_id = future_to_spec[future]
            try:
                profile = future.result()
                print(f"[worker-{worker_id}] [{spec.source_key}] completed")
                results.append(profile)
            except Exception as exc:
                print(f"[{spec.source_key}] parallel execution failed: {exc}")
                results.append({
                    "source_id": spec.source_id,
                    "source_key": spec.source_key,
                    "status": "failed",
                    "error": str(exc),
                    "row_count": 0,
                    "file_count": 0,
                    "size_mb": 0.0,
                })

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    total_rows = sum(r.get("row_count", 0) for r in results)
    total_size = sum(r.get("size_mb", 0) for r in results)
    failed = sum(1 for r in results if r.get("status") == "failed")

    print(f"[parallel] completed in {elapsed:.1f}s - total: rows={total_rows} size_mb={total_size:.2f} failed={failed}")

    return results


def run_polling(
    *,
    source_keys: list[str] | None = None,
    source_group: str | None = None,
    mode_name: str | None = None,
    run_id: str | None = None,
    output_dir: Path | None = None,
    config_path: Path | None = None,
    duration_days: float = 7,
    interval_minutes: float = 15,
    resume: bool = True,
    resume_latest: bool = False,
    overwrite: bool = False,
    start_time: str | None = None,
    end_time: str | None = None,
) -> list[dict[str, Any]]:
    config = load_config(config_path)
    selected = resolve_source_keys(
        config,
        source_keys=source_keys,
        source_group=source_group,
        default_group=str(config.get("polling_source_group") or "realtime_polling"),
    )
    end_at = datetime.now(timezone.utc) + timedelta(days=duration_days)
    resolved_output_dir = resolve_output_dir(config, output_dir)
    resolved_run_id = run_id
    if not resolved_run_id and resume and resume_latest:
        resolved_run_id = find_latest_run_id(resolved_output_dir, selected)
        if resolved_run_id:
            print(f"[resume] using latest run_id={resolved_run_id}")
    resolved_run_id = resolved_run_id or run_id_now()
    all_profiles: list[dict[str, Any]] = []
    while datetime.now(timezone.utc) <= end_at:
        poll_time = datetime.now(timezone.utc)
        print(f"[poll] snapshot at {poll_time.isoformat()} sources={','.join(selected)}")
        all_profiles.extend(
            run_once(
                source_keys=selected,
                source_group=None,
                mode_name=mode_name,
                run_id=resolved_run_id,
                output_dir=output_dir,
                config_path=config_path,
                resume=resume,
                resume_latest=False,
                overwrite=overwrite,
                poll_time=poll_time,
                start_time=start_time,
                end_time=end_time,
            )
        )
        if datetime.now(timezone.utc) >= end_at:
            break
        time.sleep(max(interval_minutes, 0.01) * 60)
    return all_profiles


def normalize_source_key(source: str) -> str:
    aliases = {
        "openmeteo_air_quality": "openmeteo",
        "openmeteo_historical_weather": "openmeteo_historical_weather",
        "ukair_air_quality_archive": "ukair_air_quality_archive",
        "ukair": "ukair_air_quality_archive",
        "uk_air": "ukair_air_quality_archive",
        "londonair_monitoring": "londonair",
        "openaq_measurements": "openaq",
        "ncei_cdo_climate": "ncei",
        "waqi_air_quality": "waqi",
        "openweather_current": "openweather",
        "stats19_collisions": "stats19",
        "naptan_stops": "naptan",
        "dft_road_traffic": "dft",
        "tfl_status": "tfl_line_status",
        "tfl_transport_status": "tfl_line_status",
        "tfl_transport": "tfl",
    }
    key = aliases.get(source, source)
    if key not in SOURCE_REGISTRY:
        raise ValueError(f"Unknown source: {source}")
    return key


def source_choices() -> list[str]:
    return sorted(SOURCE_REGISTRY)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download Greater London NEXUS demo data locally.")
    parser.add_argument("--source", action="append", help="Source key to download. Repeat for multiple sources.")
    parser.add_argument(
        "--source-group",
        help="Named source group from config/download_defaults.yml, e.g. core_historical, latest_update, realtime_snapshot, realtime_polling.",
    )
    parser.add_argument("--list", action="store_true", help="List available sources.")
    parser.add_argument("--mode", help="Download mode from config. Defaults to config default_mode.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="Downloader YAML config.")
    parser.add_argument("--output-dir", type=Path, help="Output directory. Defaults to config output_dir.")
    parser.add_argument("--run-id", help="Run id. Use the same value to resume a partial run.")
    parser.add_argument("--start-time", help="Optional backfill window start as ISO date/datetime.")
    parser.add_argument("--end-time", help="Optional backfill window end as ISO date/datetime.")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting files for the same run_id.")
    parser.add_argument("--no-resume", action="store_true", help="Ignore existing checkpoint for this run_id.")
    parser.add_argument(
        "--resume-latest",
        action="store_true",
        help="When --run-id is omitted, reuse the newest existing run_id in the output directory.",
    )
    parser.add_argument("--poll", action="store_true", help="Run repeated snapshots for realtime sources.")
    parser.add_argument("--duration-days", type=float, default=7, help="Polling duration when --poll is set.")
    parser.add_argument("--interval-minutes", type=float, default=15, help="Polling interval when --poll is set.")
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Run sources in parallel instead of sequentially. Use with --max-workers to control concurrency.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of concurrent workers when --parallel is set. Default: 4",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.list:
        print("Available sources:")
        for key in source_choices():
            spec = SOURCE_REGISTRY[key]
            realtime = " realtime" if spec.realtime else ""
            print(f"  {key:18s} {spec.source_id:28s} {spec.description}{realtime}")
        return 0
    try:
        if args.poll:
            run_polling(
                source_keys=args.source,
                source_group=args.source_group,
                mode_name=args.mode,
                run_id=args.run_id,
                output_dir=args.output_dir,
                config_path=args.config,
                duration_days=args.duration_days,
                interval_minutes=args.interval_minutes,
                resume=not args.no_resume,
                resume_latest=args.resume_latest,
                overwrite=args.overwrite,
                start_time=args.start_time,
                end_time=args.end_time,
            )
        else:
            run_once(
                source_keys=args.source,
                source_group=args.source_group,
                mode_name=args.mode,
                run_id=args.run_id,
                output_dir=args.output_dir,
                config_path=args.config,
                resume=not args.no_resume,
                resume_latest=args.resume_latest,
                overwrite=args.overwrite,
                start_time=args.start_time,
                end_time=args.end_time,
                parallel=args.parallel,
                max_workers=args.max_workers,
            )
    except KeyboardInterrupt:
        print("Interrupted.")
        return 130
    return 0


SOURCE_REGISTRY: dict[str, SourceSpec] = {
    "openmeteo": SourceSpec(
        source_key="openmeteo",
        source_id="openmeteo_air_quality",
        source_name="Open-Meteo",
        dataset_name="openmeteo_air_quality",
        description="Open-Meteo historical air quality and weather for borough centroids",
        func=download_openmeteo,
    ),
    "openmeteo_historical_weather": SourceSpec(
        source_key="openmeteo_historical_weather",
        source_id="openmeteo_historical_weather",
        source_name="Open-Meteo Historical Weather",
        dataset_name="openmeteo_historical_weather",
        description="Open-Meteo historical weather CSV for generated bbox grid points",
        func=download_openmeteo_historical_weather,
    ),
    "ukair_air_quality_archive": SourceSpec(
        source_key="ukair_air_quality_archive",
        source_id="ukair_air_quality_archive",
        source_name="UK-AIR Air Quality Archive",
        dataset_name="ukair_air_quality_archive",
        description="UK-AIR archive CSV files discovered from site flat-file pages",
        func=download_ukair_air_quality_archive,
    ),
    "londonair": SourceSpec(
        source_key="londonair",
        source_id="londonair_monitoring",
        source_name="LondonAir",
        dataset_name="londonair_monitoring",
        description="LondonAir metadata, realtime indexes, wide historical data, and AQI summaries",
        func=download_londonair,
    ),
    "openaq": SourceSpec(
        source_key="openaq",
        source_id="openaq_measurements",
        source_name="OpenAQ",
        dataset_name="openaq_measurements",
        description="OpenAQ discovered London sensors and hourly measurements",
        func=download_openaq,
        required_env=("OPENAQ_API_KEY",),
    ),
    "ncei": SourceSpec(
        source_key="ncei",
        source_id="ncei_cdo_climate",
        source_name="NCEI",
        dataset_name="ncei_cdo_climate",
        description="NCEI daily climate data for discovered London stations",
        func=download_ncei,
        required_env=("NCEI_API_TOKEN",),
    ),
    "waqi": SourceSpec(
        source_key="waqi",
        source_id="waqi_air_quality",
        source_name="WAQI",
        dataset_name="waqi_air_quality",
        description="WAQI London station snapshot/feed",
        func=download_waqi,
        required_env=("WAQI_API_TOKEN",),
        realtime=True,
    ),
    "openweather": SourceSpec(
        source_key="openweather",
        source_id="openweather_current",
        source_name="OpenWeather",
        dataset_name="openweather_current",
        description="OpenWeather current weather and air-pollution snapshot",
        func=download_openweather,
        required_env=("OPENWEATHER_API_KEY",),
        realtime=True,
    ),
    "stats19": SourceSpec(
        source_key="stats19",
        source_id="stats19_collisions",
        source_name="STATS19",
        dataset_name="stats19_collisions",
        description="STATS19 collisions, vehicles, and casualties last-5-years files",
        func=download_stats19,
    ),
    "naptan": SourceSpec(
        source_key="naptan",
        source_id="naptan_stops",
        source_name="NaPTAN",
        dataset_name="naptan_stops",
        description="NaPTAN London ATCO 490 access nodes snapshot",
        func=download_naptan,
    ),
    "london_journeys": SourceSpec(
        source_key="london_journeys",
        source_id="london_journeys",
        source_name="London Journeys",
        dataset_name="london_journeys",
        description="London Datastore public transport journeys CSV",
        func=download_london_journeys,
    ),
    "dft": SourceSpec(
        source_key="dft",
        source_id="dft_road_traffic",
        source_name="DfT Road Traffic",
        dataset_name="dft_road_traffic",
        description="DfT road traffic London count points and traffic counts",
        func=download_dft,
    ),
    "tfl": SourceSpec(
        source_key="tfl",
        source_id="tfl_transport",
        source_name="TfL",
        dataset_name="tfl_transport",
        description="TfL line status, routes, disruptions, and arrivals",
        func=download_tfl,
        realtime=True,
    ),
    "tfl_line_status": SourceSpec(
        source_key="tfl_line_status",
        source_id="tfl_line_status",
        source_name="TfL Line Status",
        dataset_name="tfl_line_status",
        description="TfL line status, routes, and disruptions realtime snapshot",
        func=download_tfl_line_status,
        realtime=True,
    ),
    "tfl_arrivals": SourceSpec(
        source_key="tfl_arrivals",
        source_id="tfl_arrivals",
        source_name="TfL Arrivals",
        dataset_name="tfl_arrivals",
        description="TfL stop arrivals realtime snapshot",
        func=download_tfl_arrivals,
        realtime=True,
    ),
}


if __name__ == "__main__":
    sys.exit(main())
