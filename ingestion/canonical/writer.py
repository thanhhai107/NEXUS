"""Canonical Writer for Raw Data.

Writes raw data envelopes to the landing zone.
Supports both local filesystem and S3/MinIO storage with:
- Streaming writes (memory efficient)
- Partitioned writes (for parallel processing)
- Worker-aware distribution

Usage:
    # Simple write
    write_raw_envelopes(records, context)
    
    # Streaming write (memory efficient)
    stream_raw_envelopes(records, context)
    
    # Partitioned write (for distributed processing)
    write_partitioned_envelopes(records, context, partition_key="source_id")
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Iterable, Iterator, Mapping

from common.config import RAW_DIR, is_vm_mode
from common.storage import get_storage
from common.worker import (
    get_worker_count,
    get_worker_index,
    hash_partition,
    is_coordinator,
    partition_iterator,
)
from ingestion.canonical.envelope import EnvelopeContext, build_raw_envelope


logger = logging.getLogger(__name__)

LOCAL_RAW_DIR = RAW_DIR


# =============================================================================
# PATH UTILITIES
# =============================================================================

def _get_raw_storage_path(dataset_id: str, filename: str) -> str:
    """Get S3 storage path for bronze data."""
    return f"bronze/{dataset_id}/{filename}"


def raw_dataset_dir(dataset_id: str, output_dir: Path | None = None) -> Path:
    """Get raw dataset directory (local only)."""
    root = output_dir or LOCAL_RAW_DIR
    path = root / dataset_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def default_raw_path(
    dataset_id: str,
    output_dir: Path | None = None,
    prefix: str | None = None,
    partition: int | None = None,
) -> Path:
    """Get default raw path for dataset (local only)."""
    output_dir_for_dataset = raw_dataset_dir(dataset_id, output_dir)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    
    if partition is not None:
        filename = f"partition={partition}/{prefix}_{stamp}.jsonl" if prefix else f"partition={partition}/{stamp}.jsonl"
    else:
        filename = f"{prefix}_{stamp}.jsonl" if prefix else f"{stamp}.jsonl"
    
    return output_dir_for_dataset / filename


# =============================================================================
# CORE WRITE FUNCTIONS
# =============================================================================

def write_raw_envelopes(
    records: Iterable[Mapping[str, object]],
    context: EnvelopeContext,
    *,
    output_path: Path | None = None,
    output_dir: Path | None = None,
    normalize_payload: bool = False,
) -> Path | str:
    """Write raw envelopes to storage (local or S3).
    
    This is the standard write function - use stream_raw_envelopes() for
    memory-efficient streaming writes.
    
    Args:
        records: Records to write
        context: Envelope context
        output_path: Explicit output path (for local mode)
        output_dir: Output directory override
        normalize_payload: Whether to normalize payload
        
    Returns:
        Path (local) or S3 URL
    """
    if is_vm_mode():
        return _write_s3_envelopes(
            records, context, normalize_payload=normalize_payload
        )
    else:
        return _write_local_envelopes(
            records, context, output_path, output_dir, normalize_payload
        )


def _write_s3_envelopes(
    records: Iterable[Mapping[str, object]],
    context: EnvelopeContext,
    normalize_payload: bool = False,
) -> str:
    """Write envelopes to S3 using streaming (memory efficient)."""
    storage = get_storage()
    dataset_id = context.dataset_id
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    storage_path = f"bronze/{dataset_id}/{stamp}.jsonl"
    
    # Stream envelopes directly without loading all into memory
    def envelope_generator():
        for index, record in enumerate(records):
            yield build_raw_envelope(
                record,
                context,
                record_index=index,
                normalize_payload=normalize_payload,
            )
    
    # Use streaming write
    result = storage.write_jsonl(storage_path, envelope_generator())
    return result


def _write_local_envelopes(
    records: Iterable[Mapping[str, object]],
    context: EnvelopeContext,
    output_path: Path | None,
    output_dir: Path | None,
    normalize_payload: bool,
) -> Path:
    """Write envelopes to local filesystem."""
    target = output_path or default_raw_path(context.dataset_id, output_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_suffix(target.suffix + ".part")
    
    try:
        with tmp_path.open("w", encoding="utf-8", newline="\n") as file:
            for index, record in enumerate(records):
                envelope = build_raw_envelope(
                    record,
                    context,
                    record_index=index,
                    normalize_payload=normalize_payload,
                )
                file.write(json.dumps(envelope, ensure_ascii=False) + "\n")
        tmp_path.replace(target)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    
    return target


# =============================================================================
# STREAMING WRITE (Memory Efficient)
# =============================================================================

def stream_raw_envelopes(
    records: Iterable[Mapping[str, object]],
    context: EnvelopeContext,
    *,
    normalize_payload: bool = False,
) -> Generator[tuple[str, int], None, None]:
    """Stream raw envelopes as they are written.
    
    Memory-efficient alternative that yields (path, record_count) as data is written.
    Useful for large datasets where you don't want to load all records into memory.
    
    Args:
        records: Records to write
        context: Envelope context
        normalize_payload: Whether to normalize payload
        
    Yields:
        Tuples of (written_path, total_records)
    """
    if is_vm_mode():
        yield from _stream_s3_envelopes(
            records, context, normalize_payload=normalize_payload
        )
    else:
        yield from _stream_local_envelopes(
            records, context, normalize_payload=normalize_payload
        )


def _stream_s3_envelopes(
    records: Iterable[Mapping[str, object]],
    context: EnvelopeContext,
    normalize_payload: bool = False,
) -> Generator[tuple[str, int], None, None]:
    """Stream envelopes to S3."""
    storage = get_storage()
    dataset_id = context.dataset_id
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    storage_path = f"bronze/{dataset_id}/{stamp}.jsonl"
    
    record_count = 0
    lines = []
    
    for index, record in enumerate(records):
        envelope = build_raw_envelope(
            record,
            context,
            record_index=index,
            normalize_payload=normalize_payload,
        )
        lines.append(json.dumps(envelope, ensure_ascii=False))
        record_count += 1
        
        # Flush every 10000 records to avoid memory issues
        if record_count % 10000 == 0:
            content = "\n".join(lines).encode("utf-8") + b"\n"
            if record_count == 10000:
                storage.write_bytes(storage_path, content)
            else:
                # Append
                existing = storage.read_bytes(storage_path) if storage.exists(storage_path) else b""
                storage.write_bytes(storage_path, existing + content)
            lines = []
    
    # Write remaining
    if lines:
        content = "\n".join(lines).encode("utf-8") + b"\n"
        if record_count <= 10000:
            storage.write_bytes(storage_path, content)
        else:
            existing = storage.read_bytes(storage_path)
            storage.write_bytes(storage_path, existing + content)
    
    yield storage_path, record_count


def _stream_local_envelopes(
    records: Iterable[Mapping[str, object]],
    context: EnvelopeContext,
    normalize_payload: bool = False,
) -> Generator[tuple[Path, int], None, None]:
    """Stream envelopes to local filesystem."""
    target = default_raw_path(context.dataset_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_suffix(target.suffix + ".part")
    
    record_count = 0
    
    try:
        with tmp_path.open("w", encoding="utf-8", newline="\n") as file:
            for index, record in enumerate(records):
                envelope = build_raw_envelope(
                    record,
                    context,
                    record_index=index,
                    normalize_payload=normalize_payload,
                )
                file.write(json.dumps(envelope, ensure_ascii=False) + "\n")
                record_count += 1
                
                # Yield periodically
                if record_count % 10000 == 0:
                    yield target, record_count
        
        tmp_path.replace(target)
        yield target, record_count
        
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


# =============================================================================
# PARTITIONED WRITE (For Distributed Processing)
# =============================================================================

def write_partitioned_envelopes(
    records: Iterable[Mapping[str, object]],
    context: EnvelopeContext,
    partition_key: str = "source_id",
    *,
    normalize_payload: bool = False,
    num_partitions: int | None = None,
    output_dir: Path | None = None,
) -> dict[int, Path | str]:
    """Write envelopes with partitioning for parallel processing.
    
    Partitions data by a key field, creating separate files per partition.
    Each partition can be processed by a different worker.
    
    Args:
        records: Records to write
        context: Envelope context
        partition_key: Field to partition on (e.g., "source_id", "timestamp")
        normalize_payload: Whether to normalize payload
        num_partitions: Number of partitions (defaults to worker count)
        output_dir: Output directory override
        
    Returns:
        Dict mapping partition index to written path
        
    Example:
        # 4 workers processing data
        paths = write_partitioned_envelopes(
            records,
            context,
            partition_key="station_id",
            num_partitions=4
        )
        # paths = {0: "bronze/.../partition=0/...", 1: "...", ...}
    """
    if num_partitions is None:
        num_partitions = get_worker_count()
    
    if is_vm_mode():
        return _write_partitioned_s3(
            records, context, partition_key, normalize_payload, num_partitions
        )
    else:
        return _write_partitioned_local(
            records, context, partition_key, normalize_payload, num_partitions, output_dir
        )


def _write_partitioned_s3(
    records: Iterable[Mapping[str, object]],
    context: EnvelopeContext,
    partition_key: str,
    normalize_payload: bool,
    num_partitions: int,
) -> dict[int, str]:
    """Write partitioned envelopes to S3."""
    storage = get_storage()
    dataset_id = context.dataset_id
    
    # Group records by partition
    by_partition: dict[int, list[dict[str, Any]]] = {i: [] for i in range(num_partitions)}
    record_counts: dict[int, int] = {i: 0 for i in range(num_partitions)}
    
    for index, record in enumerate(records):
        # Determine partition
        key_value = str(record.get(partition_key, str(index)))
        partition = hash_partition(key_value, num_partitions)
        
        # Build envelope
        envelope = build_raw_envelope(
            record,
            context,
            record_index=index,
            normalize_payload=normalize_payload,
        )
        
        by_partition[partition].append(envelope)
        record_counts[partition] += 1
    
    # Write each partition
    paths = {}
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    
    for partition in range(num_partitions):
        if by_partition[partition]:
            storage_path = f"bronze/{dataset_id}/partition={partition}/{stamp}.jsonl"
            
            # Use streaming write
            storage.write_jsonl(storage_path, by_partition[partition])
            paths[partition] = f"s3://{storage._bucket}/{storage_path}"
            
            logger.debug(
                f"Written partition {partition}: {record_counts[partition]} records"
            )
    
    return paths


def _write_partitioned_local(
    records: Iterable[Mapping[str, object]],
    context: EnvelopeContext,
    partition_key: str,
    normalize_payload: bool,
    num_partitions: int,
    output_dir: Path | None,
) -> dict[int, Path]:
    """Write partitioned envelopes to local filesystem."""
    dataset_id = context.dataset_id
    
    # Ensure directories exist
    for partition in range(num_partitions):
        partition_dir = raw_dataset_dir(dataset_id, output_dir) / f"partition={partition}"
        partition_dir.mkdir(parents=True, exist_ok=True)
    
    # Open file handles for each partition
    handles: dict[int, Any] = {}
    paths: dict[int, Path] = {}
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    
    try:
        for partition in range(num_partitions):
            path = raw_dataset_dir(dataset_id, output_dir) / f"partition={partition}" / f"{stamp}.jsonl"
            handles[partition] = path.open("w", encoding="utf-8", newline="\n")
            paths[partition] = path
        
        # Write records
        for index, record in enumerate(records):
            key_value = str(record.get(partition_key, str(index)))
            partition = hash_partition(key_value, num_partitions)
            
            envelope = build_raw_envelope(
                record,
                context,
                record_index=index,
                normalize_payload=normalize_payload,
            )
            
            handles[partition].write(json.dumps(envelope, ensure_ascii=False) + "\n")
    
    finally:
        for handle in handles.values():
            handle.close()
    
    return paths


# =============================================================================
# WORKER-AWARE DISTRIBUTED WRITE
# =============================================================================

def distributed_write_envelopes(
    records: Iterable[Mapping[str, object]],
    context: EnvelopeContext,
    partition_key: str = "source_id",
    *,
    normalize_payload: bool = False,
) -> dict[int, Path | str]:
    """Write envelopes using worker-aware distributed processing.
    
    Automatically detects worker count and partitions data accordingly.
    Each worker writes only its assigned partition.
    
    This is the main entry point for distributed writes.
    
    Args:
        records: Records to write
        context: Envelope context
        partition_key: Field to partition on
        normalize_payload: Whether to normalize payload
        
    Returns:
        Dict mapping partition index to written path
    """
    worker_info = {
        "index": get_worker_index(),
        "count": get_worker_count(),
        "is_coordinator": is_coordinator(),
    }
    
    logger.info(
        f"Distributed write: worker {worker_info['index']}/{worker_info['count']}, "
        f"coordinator={worker_info['is_coordinator']}"
    )
    
    # Partition records for this worker
    num_partitions = get_worker_count()
    my_partition = get_worker_index()
    
    # Filter records for this partition
    def partition_filter():
        for index, record in enumerate(partition_iterator(
            records, num_partitions=num_partitions
        )):
            yield record
    
    # Write only this worker's partition
    return write_partitioned_envelopes(
        partition_filter(),
        context,
        partition_key=partition_key,
        normalize_payload=normalize_payload,
        num_partitions=num_partitions,
    )


# =============================================================================
# READ UTILITIES
# =============================================================================

def read_partitioned_envelopes(
    base_path: str,
    partition_key: str = "partition",
) -> Iterator[tuple[int, dict[str, Any]]]:
    """Read partitioned envelopes from S3 or local storage.
    
    Args:
        base_path: Base path containing partitions
        partition_key: Partition key name (default: "partition")
        
    Yields:
        Tuples of (partition_index, envelope)
    """
    storage = get_storage()
    
    # List partitions
    if is_vm_mode():
        prefix = f"{base_path}/{partition_key}="
        paths = storage.list(prefix)
    else:
        base = Path(base_path)
        paths = []
        for p in base.rglob("*.jsonl"):
            if partition_key in str(p):
                paths.append(str(p))
    
    # Read each partition
    for path in sorted(paths):
        # Extract partition number
        try:
            part = path.split(f"{partition_key}=")[1].split("/")[0]
            partition = int(part)
        except (IndexError, ValueError):
            partition = 0
        
        # Read records
        for record in storage.read_jsonl(path):
            yield partition, record


__all__ = [
    "write_raw_envelopes",
    "stream_raw_envelopes",
    "write_partitioned_envelopes",
    "distributed_write_envelopes",
    "read_partitioned_envelopes",
]
