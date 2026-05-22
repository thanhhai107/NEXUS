"""Consolidate downloader runs into one canonical run per source.

The downloader keeps immutable runs under:

    runtime/downloads/<source_id>/run_id=<run_id>/

This script creates a derived run, by default:

    runtime/downloads/<source_id>/run_id=consolidated/

It does not mutate input runs. Duplicate chunk paths are resolved to one file,
then metadata is written so profiling/bronze ingestion can target the
consolidated run.
"""

from __future__ import annotations

import argparse
import codecs
import csv
import hashlib
import json
import os
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DOWNLOADS_DIR = PROJECT_ROOT / "runtime" / "downloads"
DEFAULT_OUTPUT_RUN_ID = "consolidated"
DEFAULT_STATUSES = {"success", "partial", "running"}
RECORD_COUNT_FORMATS = {".csv", ".json", ".jsonl", ".geojson"}

if sys.stdout.encoding and sys.stdout.encoding.lower().replace("-", "") != "utf8":
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, errors="replace")
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, errors="replace")


@dataclass
class InputRun:
    source_id: str
    run_id: str
    run_dir: Path
    raw_dir: Path
    metadata_dir: Path
    profile_path: Path
    profile: dict[str, Any]
    status: str
    mode: str | None
    updated_at: str | None
    mtime: float


@dataclass
class CandidateFile:
    source_id: str
    run_id: str
    run_status: str
    run_mtime: float
    source_path: Path
    relative_path: str
    size_bytes: int
    mtime: float
    sha256: str | None = None
    record_count: int | None = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as file:
        for row in rows:
            file.write(json.dumps(row, ensure_ascii=False) + "\n")


def safe_relative(path: Path, base: Path) -> str:
    return path.relative_to(base).as_posix()


def path_from_posix(relative_path: str) -> Path:
    return Path(*PurePosixPath(relative_path).parts)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iter_source_dirs(downloads_dir: Path, selected_sources: set[str] | None) -> list[Path]:
    source_dirs = [path for path in downloads_dir.iterdir() if path.is_dir()]
    if selected_sources:
        source_dirs = [path for path in source_dirs if path.name in selected_sources]
    return sorted(source_dirs, key=lambda path: path.name)


def discover_input_runs(
    source_dir: Path,
    *,
    output_run_id: str,
    allowed_statuses: set[str],
    include_run_ids: set[str] | None,
    exclude_run_ids: set[str],
) -> tuple[list[InputRun], list[dict[str, Any]]]:
    runs: list[InputRun] = []
    skipped: list[dict[str, Any]] = []
    for run_dir in sorted(source_dir.glob("run_id=*"), key=lambda path: path.name):
        run_id = run_dir.name.removeprefix("run_id=")
        if run_id == output_run_id:
            skipped.append({"run_id": run_id, "reason": "output_run"})
            continue
        if include_run_ids and run_id not in include_run_ids:
            skipped.append({"run_id": run_id, "reason": "not_in_include_run_ids"})
            continue
        if run_id in exclude_run_ids:
            skipped.append({"run_id": run_id, "reason": "excluded_run_id"})
            continue

        metadata_dir = run_dir / "metadata"
        raw_dir = run_dir / "raw"
        profile_path = metadata_dir / "profile.json"
        profile = load_json(profile_path, {}) or {}
        status = str(profile.get("status") or "unknown").lower()
        if status not in allowed_statuses:
            skipped.append({"run_id": run_id, "reason": f"status={status}"})
            continue
        if not raw_dir.exists():
            skipped.append({"run_id": run_id, "reason": "missing_raw_dir"})
            continue

        raw_files = [path for path in raw_dir.rglob("*") if path.is_file() and not path.name.endswith(".part")]
        if not raw_files:
            skipped.append({"run_id": run_id, "reason": "no_raw_files"})
            continue

        runs.append(
            InputRun(
                source_id=source_dir.name,
                run_id=run_id,
                run_dir=run_dir,
                raw_dir=raw_dir,
                metadata_dir=metadata_dir,
                profile_path=profile_path,
                profile=profile,
                status=status,
                mode=profile.get("mode"),
                updated_at=profile.get("updated_at"),
                mtime=max(path.stat().st_mtime for path in [run_dir, profile_path] if path.exists()),
            )
        )
    return runs, skipped


def collect_candidates(runs: list[InputRun]) -> list[CandidateFile]:
    candidates: list[CandidateFile] = []
    for run in runs:
        for source_path in sorted(run.raw_dir.rglob("*")):
            if not source_path.is_file() or source_path.name.endswith(".part"):
                continue
            stat = source_path.stat()
            candidates.append(
                CandidateFile(
                    source_id=run.source_id,
                    run_id=run.run_id,
                    run_status=run.status,
                    run_mtime=run.mtime,
                    source_path=source_path,
                    relative_path=safe_relative(source_path, run.raw_dir),
                    size_bytes=stat.st_size,
                    mtime=stat.st_mtime,
                )
            )
    return candidates


def candidate_score(candidate: CandidateFile, strategy: str) -> tuple[Any, ...]:
    status_rank = {"success": 3, "partial": 2, "running": 1, "unknown": 0, "failed": -1}.get(
        candidate.run_status,
        0,
    )
    if strategy == "largest":
        return (candidate.size_bytes, status_rank, candidate.run_mtime, candidate.mtime)
    if strategy == "largest-then-newest":
        return (candidate.size_bytes, candidate.run_mtime, candidate.mtime, status_rank)
    return (candidate.run_mtime, candidate.mtime, status_rank, candidate.size_bytes)


def choose_files(
    candidates: list[CandidateFile],
    *,
    conflict_strategy: str,
) -> tuple[list[CandidateFile], list[dict[str, Any]]]:
    by_relative_path: dict[str, list[CandidateFile]] = {}
    for candidate in candidates:
        by_relative_path.setdefault(candidate.relative_path, []).append(candidate)

    selected: list[CandidateFile] = []
    duplicate_groups: list[dict[str, Any]] = []

    for relative_path, group in sorted(by_relative_path.items()):
        if len(group) == 1:
            selected.append(group[0])
            continue

        sizes = {candidate.size_bytes for candidate in group}
        hashes: set[str] = set()
        if len(sizes) == 1:
            for candidate in group:
                candidate.sha256 = candidate.sha256 or file_sha256(candidate.source_path)
                hashes.add(candidate.sha256)

        same_content = len(sizes) == 1 and len(hashes) == 1
        winner = max(group, key=lambda item: candidate_score(item, conflict_strategy))
        selected.append(winner)
        duplicate_groups.append(
            {
                "relative_path": relative_path,
                "candidate_count": len(group),
                "same_content": same_content,
                "conflict": not same_content,
                "selected_run_id": winner.run_id,
                "selected_size_bytes": winner.size_bytes,
                "candidates": [
                    {
                        "run_id": candidate.run_id,
                        "status": candidate.run_status,
                        "size_bytes": candidate.size_bytes,
                        "mtime": candidate.mtime,
                        "sha256": candidate.sha256,
                    }
                    for candidate in group
                ],
            }
        )

    return selected, duplicate_groups


def estimate_record_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if not isinstance(payload, dict):
        return 1

    hourly = payload.get("hourly")
    if isinstance(hourly, dict) and isinstance(hourly.get("time"), list):
        return len(hourly["time"])

    record_keys = {"results", "data", "items", "records", "features", "rows", "readings", "feed"}
    for key, value in payload.items():
        if key.lower() in record_keys:
            if isinstance(value, list):
                return len(value)
            if isinstance(value, dict):
                count = estimate_record_count(value)
                if count:
                    return count

    nested_counts = [estimate_record_count(value) for value in payload.values() if isinstance(value, (dict, list))]
    return max(nested_counts) if nested_counts else 1


def count_csv_records(path: Path) -> int:
    count = 0
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as file:
        reader = csv.reader(file)
        for count, _row in enumerate(reader, start=1):
            pass
    return max(count - 1, 0)


def count_jsonl_records(path: Path) -> int:
    count = 0
    with path.open("r", encoding="utf-8", errors="replace") as file:
        for line in file:
            if line.strip():
                count += 1
    return count


def count_json_records(path: Path) -> int:
    payload = load_json(path, None)
    if payload is None:
        return 0
    return estimate_record_count(payload)


def count_records(path: Path) -> int | None:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return count_csv_records(path)
    if suffix == ".jsonl":
        return count_jsonl_records(path)
    if suffix in {".json", ".geojson"}:
        return count_json_records(path)
    return None


def combine_min(values: Iterable[Any]) -> Any:
    clean = [value for value in values if value not in (None, "")]
    return min(clean) if clean else None


def combine_max(values: Iterable[Any]) -> Any:
    clean = [value for value in values if value not in (None, "")]
    return max(clean) if clean else None


def link_or_copy(source: Path, destination: Path, method: str) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if method == "copy":
        shutil.copy2(source, destination)
        return "copy"
    try:
        os.link(source, destination)
        return "hardlink"
    except OSError:
        shutil.copy2(source, destination)
        return "copy_fallback"


def safe_remove_consolidated(target_run_dir: Path, downloads_dir: Path, output_run_id: str) -> None:
    resolved_target = target_run_dir.resolve()
    resolved_downloads = downloads_dir.resolve()
    if not resolved_target.is_relative_to(resolved_downloads):
        raise RuntimeError(f"Refusing to remove path outside downloads dir: {target_run_dir}")
    if target_run_dir.name != f"run_id={output_run_id}":
        raise RuntimeError(f"Refusing to remove unexpected target path: {target_run_dir}")
    shutil.rmtree(target_run_dir)


def materialize_files(
    selected: list[CandidateFile],
    target_raw_dir: Path,
    *,
    method: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in selected:
        destination = target_raw_dir / path_from_posix(candidate.relative_path)
        materialized_as = link_or_copy(candidate.source_path, destination, method)
        rows.append(
            {
                "relative_path": candidate.relative_path,
                "input_run_id": candidate.run_id,
                "input_status": candidate.run_status,
                "size_bytes": candidate.size_bytes,
                "record_count": candidate.record_count,
                "sha256": candidate.sha256,
                "materialized_as": materialized_as,
                "source_path": str(candidate.source_path),
            }
        )
    return rows


def merge_request_logs(runs: list[InputRun], target_metadata_dir: Path) -> int:
    output_path = target_metadata_dir / "request_log.jsonl"
    count = 0
    with output_path.open("w", encoding="utf-8", newline="\n") as output:
        for run in runs:
            log_path = run.metadata_dir / "request_log.jsonl"
            if not log_path.exists():
                continue
            with log_path.open("r", encoding="utf-8", errors="replace") as input_file:
                for line in input_file:
                    if not line.strip():
                        continue
                    try:
                        event = json.loads(line)
                        if isinstance(event, dict):
                            event.setdefault("input_run_id", run.run_id)
                            event.setdefault("input_run_status", run.status)
                            output.write(json.dumps(event, ensure_ascii=False) + "\n")
                            count += 1
                            continue
                    except json.JSONDecodeError:
                        pass
                    output.write(
                        json.dumps(
                            {
                                "input_run_id": run.run_id,
                                "input_run_status": run.status,
                                "raw_log_line": line.rstrip("\n"),
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    count += 1
    if count == 0:
        output_path.unlink(missing_ok=True)
    return count


def status_counts(runs: list[InputRun]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for run in runs:
        counts[run.status] = counts.get(run.status, 0) + 1
    return counts


def build_profile(
    source_id: str,
    output_run_id: str,
    runs: list[InputRun],
    selected: list[CandidateFile],
    duplicate_groups: list[dict[str, Any]],
    generated_at: str,
) -> dict[str, Any]:
    row_counts = [candidate.record_count for candidate in selected if candidate.record_count is not None]
    input_statuses = status_counts(runs)
    consolidated_status = "success"
    if not selected:
        consolidated_status = "failed"
    elif any(status != "success" for status in input_statuses):
        consolidated_status = "partial"

    profiles = [run.profile for run in runs]
    return {
        "source_id": source_id,
        "run_id": output_run_id,
        "mode": "consolidated",
        "date_from": combine_min(profile.get("date_from") for profile in profiles),
        "date_to": combine_max(profile.get("date_to") for profile in profiles),
        "transport_date_from": combine_min(profile.get("transport_date_from") for profile in profiles),
        "transport_date_to": combine_max(profile.get("transport_date_to") for profile in profiles),
        "spatial_scope": combine_max(profile.get("spatial_scope") for profile in profiles),
        "row_count": sum(row_counts) if len(row_counts) == len(selected) else None,
        "row_count_method": "file_scan" if len(row_counts) == len(selected) else "not_counted",
        "file_count": len(selected),
        "size_mb": round(sum(candidate.size_bytes for candidate in selected) / (1024 * 1024), 3),
        "first_timestamp": combine_min(profile.get("first_timestamp") for profile in profiles),
        "last_timestamp": combine_max(profile.get("last_timestamp") for profile in profiles),
        "failed_requests": sum(int(profile.get("failed_requests") or 0) for profile in profiles),
        "status": consolidated_status,
        "started_at": generated_at,
        "updated_at": generated_at,
        "finished_at": generated_at,
        "input_run_count": len(runs),
        "input_status_counts": input_statuses,
        "duplicate_relative_path_groups": len(duplicate_groups),
        "conflict_relative_path_groups": sum(1 for group in duplicate_groups if group.get("conflict")),
    }


def build_checkpoint(
    source_id: str,
    output_run_id: str,
    selected: list[CandidateFile],
    generated_at: str,
) -> dict[str, Any]:
    completed_chunks = {
        candidate.relative_path: {
            "completed_at": generated_at,
            "input_run_id": candidate.run_id,
            "input_status": candidate.run_status,
            "size_bytes": candidate.size_bytes,
            "record_count": candidate.record_count,
            "sha256": candidate.sha256,
        }
        for candidate in selected
    }
    return {
        "source_id": source_id,
        "run_id": output_run_id,
        "completed_chunks": completed_chunks,
        "failed_chunks": {},
        "last_run_at": generated_at,
    }


def build_source_manifest(
    source_id: str,
    output_run_id: str,
    runs: list[InputRun],
    generated_at: str,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "source_key": source_id,
        "run_id": output_run_id,
        "mode": "consolidated",
        "started_at": generated_at,
        "input_runs": [
            {
                "run_id": run.run_id,
                "status": run.status,
                "mode": run.mode,
                "updated_at": run.updated_at,
                "profile_path": str(run.profile_path),
            }
            for run in runs
        ],
    }


def consolidate_source(
    source_dir: Path,
    *,
    downloads_dir: Path,
    output_run_id: str,
    allowed_statuses: set[str],
    include_run_ids: set[str] | None,
    exclude_run_ids: set[str],
    conflict_strategy: str,
    method: str,
    replace: bool,
    dry_run: bool,
    skip_row_count: bool,
) -> dict[str, Any]:
    source_id = source_dir.name
    target_run_dir = source_dir / f"run_id={output_run_id}"
    target_raw_dir = target_run_dir / "raw"
    target_metadata_dir = target_run_dir / "metadata"
    generated_at = utc_now()

    runs, skipped_runs = discover_input_runs(
        source_dir,
        output_run_id=output_run_id,
        allowed_statuses=allowed_statuses,
        include_run_ids=include_run_ids,
        exclude_run_ids=exclude_run_ids,
    )
    candidates = collect_candidates(runs)
    selected, duplicate_groups = choose_files(candidates, conflict_strategy=conflict_strategy)

    if not skip_row_count:
        for candidate in selected:
            if candidate.source_path.suffix.lower() in RECORD_COUNT_FORMATS:
                candidate.record_count = count_records(candidate.source_path)

    summary = {
        "source_id": source_id,
        "output_run_id": output_run_id,
        "dry_run": dry_run,
        "input_run_count": len(runs),
        "skipped_run_count": len(skipped_runs),
        "input_file_count": len(candidates),
        "selected_file_count": len(selected),
        "duplicate_relative_path_groups": len(duplicate_groups),
        "conflict_relative_path_groups": sum(1 for group in duplicate_groups if group.get("conflict")),
        "selected_size_mb": round(sum(candidate.size_bytes for candidate in selected) / (1024 * 1024), 3),
        "selected_row_count": (
            sum(candidate.record_count or 0 for candidate in selected)
            if selected and all(candidate.record_count is not None for candidate in selected)
            else None
        ),
        "target_run_dir": str(target_run_dir),
    }

    if dry_run:
        return summary

    if target_run_dir.exists():
        if not replace:
            raise FileExistsError(
                f"{target_run_dir} already exists. Re-run with --replace to rebuild this derived run."
            )
        safe_remove_consolidated(target_run_dir, downloads_dir, output_run_id)

    target_raw_dir.mkdir(parents=True, exist_ok=True)
    target_metadata_dir.mkdir(parents=True, exist_ok=True)

    selected_rows = materialize_files(selected, target_raw_dir, method=method)
    request_log_count = merge_request_logs(runs, target_metadata_dir)

    manifest = {
        "source_id": source_id,
        "output_run_id": output_run_id,
        "generated_at": generated_at,
        "downloads_dir": str(downloads_dir),
        "source_dir": str(source_dir),
        "target_run_dir": str(target_run_dir),
        "conflict_strategy": conflict_strategy,
        "materialization_method": method,
        "allowed_statuses": sorted(allowed_statuses),
        "skip_row_count": skip_row_count,
        "summary": summary,
        "input_runs": [
            {
                "run_id": run.run_id,
                "status": run.status,
                "mode": run.mode,
                "updated_at": run.updated_at,
                "raw_dir": str(run.raw_dir),
                "profile_path": str(run.profile_path),
                "profile": run.profile,
            }
            for run in runs
        ],
        "skipped_runs": skipped_runs,
        "request_log_events_merged": request_log_count,
    }

    write_json(target_metadata_dir / "merge_manifest.json", manifest)
    write_jsonl(target_metadata_dir / "dedupe_index.jsonl", selected_rows)
    write_jsonl(target_metadata_dir / "duplicate_groups.jsonl", duplicate_groups)
    write_jsonl(target_metadata_dir / "skipped_runs.jsonl", skipped_runs)
    write_json(target_metadata_dir / "profile.json", build_profile(source_id, output_run_id, runs, selected, duplicate_groups, generated_at))
    write_json(target_metadata_dir / "checkpoint.json", build_checkpoint(source_id, output_run_id, selected, generated_at))
    write_json(target_metadata_dir / "source_manifest.json", build_source_manifest(source_id, output_run_id, runs, generated_at))

    return summary


def parse_csv_set(values: list[str] | None) -> set[str] | None:
    if not values:
        return None
    result: set[str] = set()
    for value in values:
        result.update(item.strip() for item in value.split(",") if item.strip())
    return result


def parse_required_csv_set(values: list[str] | None) -> set[str]:
    return parse_csv_set(values) or set()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consolidate runtime/downloads run_id folders per source.")
    parser.add_argument("--downloads-dir", type=Path, default=DEFAULT_DOWNLOADS_DIR)
    parser.add_argument("--source", action="append", help="Source id to consolidate. Can be repeated or comma-separated.")
    parser.add_argument("--output-run-id", default=DEFAULT_OUTPUT_RUN_ID)
    parser.add_argument("--input-run-id", action="append", help="Only include these input run ids. Can be repeated or comma-separated.")
    parser.add_argument("--exclude-run-id", action="append", help="Exclude these input run ids. Can be repeated or comma-separated.")
    parser.add_argument(
        "--status",
        action="append",
        help="Allowed input profile statuses. Default: success,partial,running. Can be repeated or comma-separated.",
    )
    parser.add_argument(
        "--conflict-strategy",
        choices=["newest", "largest", "largest-then-newest"],
        default="newest",
        help="How to choose a winner when multiple runs contain the same raw relative path.",
    )
    parser.add_argument(
        "--method",
        choices=["hardlink", "copy"],
        default="hardlink",
        help="Use hardlinks by default to avoid duplicating local disk blocks; falls back to copy if unsupported.",
    )
    parser.add_argument("--replace", action="store_true", help="Delete and rebuild the output run if it already exists.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned consolidation without writing files.")
    parser.add_argument("--skip-row-count", action="store_true", help="Skip per-file row counting for faster metadata generation.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    downloads_dir = args.downloads_dir
    if not downloads_dir.is_absolute():
        downloads_dir = PROJECT_ROOT / downloads_dir
    downloads_dir = downloads_dir.resolve()
    if not downloads_dir.exists():
        raise FileNotFoundError(f"Downloads dir not found: {downloads_dir}")

    selected_sources = parse_csv_set(args.source)
    include_run_ids = parse_csv_set(args.input_run_id)
    exclude_run_ids = parse_required_csv_set(args.exclude_run_id)
    allowed_statuses = parse_csv_set(args.status) or set(DEFAULT_STATUSES)
    allowed_statuses = {status.lower() for status in allowed_statuses}

    source_dirs = iter_source_dirs(downloads_dir, selected_sources)
    if selected_sources:
        found = {path.name for path in source_dirs}
        missing = sorted(selected_sources - found)
        if missing:
            raise FileNotFoundError(f"Source folder(s) not found under {downloads_dir}: {', '.join(missing)}")

    print(f"Downloads dir: {downloads_dir}")
    print(f"Output run id: {args.output_run_id}")
    print(f"Sources: {', '.join(path.name for path in source_dirs) if source_dirs else '(none)'}")
    print(f"Mode: {'dry-run' if args.dry_run else 'write'}")

    summaries: list[dict[str, Any]] = []
    for source_dir in source_dirs:
        print(f"[{source_dir.name}] consolidating...")
        summary = consolidate_source(
            source_dir,
            downloads_dir=downloads_dir,
            output_run_id=args.output_run_id,
            allowed_statuses=allowed_statuses,
            include_run_ids=include_run_ids,
            exclude_run_ids=exclude_run_ids,
            conflict_strategy=args.conflict_strategy,
            method=args.method,
            replace=args.replace,
            dry_run=args.dry_run,
            skip_row_count=args.skip_row_count,
        )
        summaries.append(summary)
        print(
            "[{source}] input_runs={runs} selected_files={files} size_mb={size} "
            "duplicate_paths={dupes} conflicts={conflicts} rows={rows}".format(
                source=summary["source_id"],
                runs=summary["input_run_count"],
                files=summary["selected_file_count"],
                size=summary["selected_size_mb"],
                dupes=summary["duplicate_relative_path_groups"],
                conflicts=summary["conflict_relative_path_groups"],
                rows=summary["selected_row_count"],
            )
        )

    total = {
        "sources": len(summaries),
        "selected_files": sum(int(summary["selected_file_count"]) for summary in summaries),
        "selected_size_mb": round(sum(float(summary["selected_size_mb"]) for summary in summaries), 3),
        "selected_row_count": (
            sum(int(summary["selected_row_count"]) for summary in summaries)
            if summaries and all(summary["selected_row_count"] is not None for summary in summaries)
            else None
        ),
    }
    print("Summary:", json.dumps(total, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
