"""Checkpoint Management for Orchestration.

Provides checkpoint tracking for resumable pipeline runs.
Extracted from ingestion/base/core.py for orchestration use.
Supports both local filesystem and S3/MinIO storage.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common.config import RUNTIME_DIR, is_vm_mode
from common.storage import get_storage


CHECKPOINTS_DIR = RUNTIME_DIR / "checkpoints"


def _get_checkpoint_storage_path(run_id: str, dataset: str) -> str:
    """Get storage path for checkpoint in S3."""
    return f"checkpoints/{dataset}/{run_id}.checkpoint.json"


def ensure_checkpoints_dir(dataset: str | None = None) -> Path:
    """Ensure checkpoints directory exists.
    
    Args:
        dataset: Optional dataset name for nested structure
    
    Returns:
        Path to checkpoints directory
    """
    if dataset:
        path = CHECKPOINTS_DIR / dataset
    else:
        path = CHECKPOINTS_DIR
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_checkpoint_path(run_id: str, dataset: str) -> Path:
    """Get path to checkpoint file.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
    
    Returns:
        Path to checkpoint.json
    """
    return ensure_checkpoints_dir(dataset) / f"{run_id}.checkpoint.json"


def load_checkpoint(run_id: str, dataset: str) -> dict[str, Any]:
    """Load checkpoint data for a run.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
    
    Returns:
        Checkpoint dict with completed_chunks, failed_chunks, etc.
    """
    if is_vm_mode():
        # Use S3 storage
        storage = get_storage()
        storage_path = _get_checkpoint_storage_path(run_id, dataset)
        
        if not storage.exists(storage_path):
            return {
                "run_id": run_id,
                "dataset": dataset,
                "completed_chunks": {},
                "failed_chunks": {},
                "skipped_chunks": {},
                "chunk_outputs": {},
                "last_run_at": None,
            }
        
        try:
            return storage.read(storage_path)
        except Exception:
            return {
                "run_id": run_id,
                "dataset": dataset,
                "completed_chunks": {},
                "failed_chunks": {},
                "skipped_chunks": {},
                "chunk_outputs": {},
                "last_run_at": None,
            }
    else:
        # Use local filesystem
        path = get_checkpoint_path(run_id, dataset)
        
        if not path.exists():
            return {
                "run_id": run_id,
                "dataset": dataset,
                "completed_chunks": {},
                "failed_chunks": {},
                "skipped_chunks": {},
                "chunk_outputs": {},
                "last_run_at": None,
            }
        
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {
                "run_id": run_id,
                "dataset": dataset,
                "completed_chunks": {},
                "failed_chunks": {},
                "skipped_chunks": {},
                "chunk_outputs": {},
                "last_run_at": None,
            }


def save_checkpoint(run_id: str, dataset: str, data: dict[str, Any]) -> None:
    """Save checkpoint data for a run.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
        data: Checkpoint data to save
    """
    data["run_id"] = run_id
    data["dataset"] = dataset
    data["last_run_at"] = datetime.now(timezone.utc).isoformat()
    
    if is_vm_mode():
        # Use S3 storage
        storage = get_storage()
        storage_path = _get_checkpoint_storage_path(run_id, dataset)
        storage.write(storage_path, data, is_json=True)
    else:
        # Use local filesystem
        path = get_checkpoint_path(run_id, dataset)
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def is_chunk_completed(run_id: str, dataset: str, chunk_id: str) -> bool:
    """Check if a chunk has been completed.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
        chunk_id: Chunk identifier
    
    Returns:
        True if chunk is completed
    """
    checkpoint = load_checkpoint(run_id, dataset)
    completed = checkpoint.get("completed_chunks", {})
    return chunk_id in completed


def mark_chunk_complete(
    run_id: str,
    dataset: str,
    chunk_id: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Mark a chunk as completed.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
        chunk_id: Chunk identifier
        metadata: Optional metadata about the completion
    """
    checkpoint = load_checkpoint(run_id, dataset)
    
    completed = checkpoint.setdefault("completed_chunks", {})
    completed[chunk_id] = {
        "completed_at": datetime.now(timezone.utc).isoformat(),
        **(metadata or {}),
    }
    
    # Remove from failed/skipped if it was there
    checkpoint.get("failed_chunks", {}).pop(chunk_id, None)
    checkpoint.get("skipped_chunks", {}).pop(chunk_id, None)
    
    save_checkpoint(run_id, dataset, checkpoint)


def mark_chunk_failed(
    run_id: str,
    dataset: str,
    chunk_id: str,
    error: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Mark a chunk as failed.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
        chunk_id: Chunk identifier
        error: Error message
        metadata: Optional metadata
    """
    checkpoint = load_checkpoint(run_id, dataset)
    
    failed = checkpoint.setdefault("failed_chunks", {})
    failed[chunk_id] = {
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "error": error,
        **(metadata or {}),
    }
    
    save_checkpoint(run_id, dataset, checkpoint)


def mark_chunk_skipped(
    run_id: str,
    dataset: str,
    chunk_id: str,
    reason: str,
) -> None:
    """Mark a chunk as skipped.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
        chunk_id: Chunk identifier
        reason: Reason for skipping
    """
    checkpoint = load_checkpoint(run_id, dataset)
    
    skipped = checkpoint.setdefault("skipped_chunks", {})
    skipped[chunk_id] = {
        "skipped_at": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
    }
    
    save_checkpoint(run_id, dataset, checkpoint)


def get_completed_chunks(run_id: str, dataset: str) -> list[str]:
    """Get list of completed chunk IDs.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
    
    Returns:
        List of completed chunk IDs
    """
    checkpoint = load_checkpoint(run_id, dataset)
    return list(checkpoint.get("completed_chunks", {}).keys())


def get_failed_chunks(run_id: str, dataset: str) -> list[str]:
    """Get list of failed chunk IDs.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
    
    Returns:
        List of failed chunk IDs
    """
    checkpoint = load_checkpoint(run_id, dataset)
    return list(checkpoint.get("failed_chunks", {}).keys())


def clear_checkpoint(run_id: str, dataset: str) -> bool:
    """Clear a checkpoint file.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
    
    Returns:
        True if checkpoint was deleted
    """
    path = get_checkpoint_path(run_id, dataset)
    if path.exists():
        path.unlink()
        return True
    return False
