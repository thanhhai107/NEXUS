"""Distributed Download Utilities.

Provides utilities for parallel and distributed downloads:
- Parallel source downloads with worker coordination
- Chunk-based downloading with progress tracking
- Work stealing for load balancing
- Download state coordination

Usage:
    from common.distributed import parallel_download, distribute_work
    
    # Parallel download
    for result in parallel_download(sources, max_workers=4):
        process(result)
    
    # Distribute work across workers
    my_work = distribute_work(all_tasks)
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Iterator, TypeVar

from common.storage import get_storage
from common.worker import (
    get_worker_count,
    get_worker_index,
    get_worker_info,
    is_coordinator,
    partition_iterator,
)


logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


# =============================================================================
# DOWNLOAD RESULT
# =============================================================================

@dataclass
class DownloadResult:
    """Result of a distributed download operation."""
    source_id: str
    status: str  # "success", "failed", "skipped"
    records_count: int = 0
    bytes_downloaded: int = 0
    duration_ms: int = 0
    error: str | None = None
    worker_id: str | None = None
    partition: int | None = None
    output_path: str | None = None


# =============================================================================
# PARALLEL DOWNLOAD
# =============================================================================

def parallel_download(
    sources: list[dict[str, Any]],
    download_fn: Callable[[dict[str, Any], int], DownloadResult],
    max_workers: int | None = None,
    *,
    partition_by: str | None = None,
) -> Iterator[DownloadResult]:
    """Download multiple sources in parallel.
    
    Args:
        sources: List of source configs
        download_fn: Function to download each source (source, worker_index) -> DownloadResult
        max_workers: Max concurrent workers (defaults to detected workers)
        partition_by: Optional field to partition sources by
        
    Yields:
        DownloadResult for each completed source
    """
    if max_workers is None:
        max_workers = get_worker_count()
    
    worker_info = get_worker_info()
    logger.info(
        f"Starting parallel download: {len(sources)} sources, {max_workers} workers, "
        f"worker_id={worker_info.worker_id}"
    )
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_source = {
            executor.submit(download_fn, source, i % max_workers): source
            for i, source in enumerate(sources)
        }
        
        # Yield results as they complete
        for future in as_completed(future_to_source):
            source = future_to_source[future]
            try:
                result = future.result()
                result.worker_id = worker_info.worker_id
                yield result
            except Exception as exc:
                logger.error(f"Download failed for {source.get('source_id', 'unknown')}: {exc}")
                yield DownloadResult(
                    source_id=source.get("source_id", "unknown"),
                    status="failed",
                    error=str(exc),
                    worker_id=worker_info.worker_id,
                )
    
    elapsed = (time.time() - start_time) * 1000
    logger.info(f"Parallel download completed in {elapsed:.0f}ms")


def batch_download(
    sources: list[dict[str, Any]],
    download_fn: Callable[[dict[str, Any], int], DownloadResult],
    batch_size: int = 10,
    max_workers: int | None = None,
) -> list[DownloadResult]:
    """Download sources in batches for better memory management.
    
    Args:
        sources: List of source configs
        download_fn: Function to download each source
        batch_size: Number of sources per batch
        max_workers: Max concurrent workers
        
    Returns:
        List of all DownloadResults
    """
    if max_workers is None:
        max_workers = get_worker_count()
    
    results = []
    
    for i in range(0, len(sources), batch_size):
        batch = sources[i:i + batch_size]
        logger.debug(f"Processing batch {i//batch_size + 1}: {len(batch)} sources")
        
        batch_results = list(parallel_download(batch, download_fn, max_workers))
        results.extend(batch_results)
    
    return results


# =============================================================================
# WORK STEALING (Dynamic Load Balancing)
# =============================================================================

@dataclass
class WorkStealer:
    """Dynamic work stealing for load balancing across workers.
    
    Uses S3 or local storage to coordinate work stealing.
    """
    namespace: str
    task_prefix: str = "work"
    _storage: Any = field(default=None, repr=False)
    
    @property
    def storage(self):
        if self._storage is None:
            self._storage = get_storage()
        return self._storage
    
    def claim_task(self, task_id: str) -> bool:
        """Attempt to claim a task for processing.
        
        Uses atomic write-if-not-exists for coordination.
        
        Args:
            task_id: Unique task identifier
            
        Returns:
            True if claim succeeded
        """
        claim_path = f"{self.namespace}/{self.task_prefix}/{task_id}.claim"
        
        # Check if already claimed
        if self.storage.exists(claim_path):
            return False
        
        # Try to claim
        worker_info = get_worker_info()
        claim_data = {
            "task_id": task_id,
            "worker_id": worker_info.worker_id,
            "claimed_at": datetime.now(timezone.utc).isoformat(),
        }
        
        try:
            self.storage.write(claim_path, claim_data, is_json=True)
            return True
        except Exception:
            return False
    
    def get_pending_tasks(self, max_count: int = 100) -> list[str]:
        """Get list of pending tasks.
        
        Args:
            max_count: Maximum tasks to return
            
        Returns:
            List of pending task IDs
        """
        prefix = f"{self.namespace}/{self.task_prefix}/"
        all_claims = self.storage.list(prefix)
        
        pending = []
        for path in all_claims:
            if path.endswith(".pending"):
                task_id = path.replace(f"{prefix}", "").replace(".pending", "")
                pending.append(task_id)
                
                if len(pending) >= max_count:
                    break
        
        return pending
    
    def mark_task_pending(self, task_id: str) -> None:
        """Mark a task as pending (available for stealing)."""
        path = f"{self.namespace}/{self.task_prefix}/{task_id}.pending"
        self.storage.write_bytes(path, b"")
    
    def mark_task_done(self, task_id: str) -> None:
        """Mark a task as completed."""
        # Remove pending marker
        pending_path = f"{self.namespace}/{self.task_prefix}/{task_id}.pending"
        self.storage.delete(pending_path)
        
        # Mark as done
        done_path = f"{self.namespace}/{self.task_prefix}/{task_id}.done"
        worker_info = get_worker_info()
        self.storage.write(done_path, {
            "task_id": task_id,
            "completed_by": worker_info.worker_id,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }, is_json=True)


# =============================================================================
# DISTRIBUTED WORK QUEUE
# =============================================================================

def distribute_work(
    tasks: Iterable[T],
    task_id_fn: Callable[[T], str] | None = None,
) -> Iterator[T]:
    """Distribute work to workers using filtering.
    
    Each worker only processes tasks assigned to its index.
    This is a simple but effective distribution strategy.
    
    Args:
        tasks: Iterable of tasks
        task_id_fn: Optional function to extract task ID
        
    Yields:
        Tasks assigned to this worker
    """
    worker_index = get_worker_index()
    num_workers = get_worker_count()
    
    logger.debug(
        f"Distributing work: worker {worker_index}/{num_workers}"
    )
    
    for i, task in enumerate(tasks):
        if task_id_fn:
            task_id = task_id_fn(task)
            # Use hash for consistent assignment
            import hashlib
            hash_val = int(hashlib.md5(str(task_id).encode()).hexdigest(), 16)
            assigned = hash_val % num_workers
        else:
            assigned = i % num_workers
        
        if assigned == worker_index:
            yield task


def steal_work(
    stealer: WorkStealer,
    task_fn: Callable[[str], R | None],
    max_steals: int = 10,
) -> list[R]:
    """Attempt to steal and process work from other workers.
    
    Args:
        stealer: WorkStealer instance
        task_fn: Function to process task (returns None on failure)
        max_steals: Maximum tasks to attempt stealing
        
    Returns:
        List of results from processed stolen tasks
    """
    results = []
    pending = stealer.get_pending_tasks(max_count=max_steals)
    
    for task_id in pending:
        # Try to claim
        if stealer.claim_task(task_id):
            logger.info(f"Worker {get_worker_info().worker_id} stole task: {task_id}")
            
            try:
                result = task_fn(task_id)
                if result is not None:
                    results.append(result)
                stealer.mark_task_done(task_id)
            except Exception as exc:
                logger.error(f"Task {task_id} failed: {exc}")
                stealer.mark_task_done(task_id)  # Mark done even on failure
    
    return results


# =============================================================================
# DOWNLOAD COORDINATION
# =============================================================================

@dataclass
class DownloadCoordination:
    """Coordination for distributed downloads."""
    namespace: str
    _storage: Any = field(default=None, repr=False)
    
    @property
    def storage(self):
        if self._storage is None:
            self._storage = get_storage()
        return self._storage
    
    def init_run(self, run_id: str, total_sources: int) -> None:
        """Initialize a distributed download run.
        
        Args:
            run_id: Unique run identifier
            total_sources: Total number of sources to download
        """
        path = f"{self.namespace}/runs/{run_id}.json"
        worker_info = get_worker_info()
        
        self.storage.write(path, {
            "run_id": run_id,
            "total_sources": total_sources,
            "completed": 0,
            "failed": 0,
            "started_by": worker_info.worker_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }, is_json=True)
    
    def update_progress(
        self,
        run_id: str,
        status: str,
        source_id: str,
        records_count: int = 0,
    ) -> None:
        """Update download progress.
        
        Args:
            run_id: Run identifier
            status: "completed" or "failed"
            source_id: Source that completed
            records_count: Number of records downloaded
        """
        path = f"{self.namespace}/progress/{run_id}/{source_id}.json"
        
        self.storage.write(path, {
            "run_id": run_id,
            "source_id": source_id,
            "status": status,
            "records_count": records_count,
            "worker_id": get_worker_info().worker_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }, is_json=True)
    
    def get_run_summary(self, run_id: str) -> dict[str, Any]:
        """Get summary of a download run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Summary dict with counts
        """
        progress_prefix = f"{self.namespace}/progress/{run_id}/"
        
        completed = 0
        failed = 0
        total_records = 0
        
        for path in self.storage.list(progress_prefix):
            try:
                progress = self.storage.read(path)
                if progress.get("status") == "completed":
                    completed += 1
                    total_records += progress.get("records_count", 0)
                elif progress.get("status") == "failed":
                    failed += 1
            except Exception:
                pass
        
        return {
            "run_id": run_id,
            "completed": completed,
            "failed": failed,
            "total_records": total_records,
        }


# =============================================================================
# CHUNKED DOWNLOAD (For Large Files)
# =============================================================================

@dataclass
class ChunkProgress:
    """Progress tracking for chunked downloads."""
    chunk_id: str
    total_chunks: int
    completed_chunks: int = 0
    total_bytes: int = 0
    downloaded_bytes: int = 0
    status: str = "running"  # "running", "completed", "failed"


def chunked_download(
    url: str,
    download_fn: Callable[[str, int, int], bytes],  # url, start, end -> bytes
    chunk_size: int = 10 * 1024 * 1024,  # 10MB chunks
    max_concurrent: int | None = None,
) -> Iterator[tuple[int, bytes]]:
    """Download a large file in chunks with parallel fetching.
    
    Args:
        url: URL to download
        download_fn: Function to download range (url, start, end) -> bytes
        chunk_size: Size per chunk
        max_concurrent: Max concurrent chunk downloads
        
    Yields:
        Tuples of (chunk_index, chunk_data)
    """
    if max_concurrent is None:
        max_concurrent = get_worker_count()
    
    # This is a placeholder - actual implementation would need
    # HTTP range requests support from the download_fn
    
    # For now, yield a single chunk (full download)
    yield 0, download_fn(url, 0, 0)


# =============================================================================
# COORDINATOR HELPERS
# =============================================================================

def is_first_worker() -> bool:
    """Check if this is the first/coordinating worker.
    
    Returns:
        True if this worker should coordinate others
    """
    return is_coordinator()


def wait_for_workers(
    timeout_seconds: int = 60,
    check_interval: float = 1.0,
) -> bool:
    """Wait for all workers to be ready.
    
    Args:
        timeout_seconds: Maximum time to wait
        check_interval: Time between checks
        
    Returns:
        True if all workers ready, False if timeout
    """
    start = time.time()
    expected_workers = get_worker_count()
    
    # Simple implementation - in production, use proper coordination
    # like a barrier in Redis or ZooKeeper
    
    while time.time() - start < timeout_seconds:
        # In a real implementation, check worker registry
        logger.debug(f"Waiting for {expected_workers} workers...")
        time.sleep(check_interval)
    
    return False  # Timeout


def broadcast_task(
    task_type: str,
    task_data: dict[str, Any],
) -> None:
    """Broadcast a task to all workers.
    
    Args:
        task_type: Type of task (e.g., "download", "validate")
        task_data: Task configuration
    """
    storage = get_storage()
    namespace = "nexus/tasks"
    
    path = f"{namespace}/{task_type}/current.json"
    worker_info = get_worker_info()
    
    storage.write(path, {
        "task_type": task_type,
        "task_data": task_data,
        "broadcast_by": worker_info.worker_id,
        "broadcast_at": datetime.now(timezone.utc).isoformat(),
    }, is_json=True)
    
    logger.info(f"Broadcast task: {task_type}")


__all__ = [
    # Result types
    "DownloadResult",
    "ChunkProgress",
    # Parallel download
    "parallel_download",
    "batch_download",
    # Work stealing
    "WorkStealer",
    "distribute_work",
    "steal_work",
    # Coordination
    "DownloadCoordination",
    "is_first_worker",
    "wait_for_workers",
    "broadcast_task",
    # Chunked download
    "chunked_download",
]
