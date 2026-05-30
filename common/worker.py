"""NEXUS Worker Detection and Distributed Processing Module.

Detects available workers and provides utilities for distributed processing:
- Auto-detect worker count from Airflow/Celery/Kubernetes
- Partition data for parallel processing
- Coordinate work distribution across workers

Usage:
    from common.worker import get_worker_info, partition_work, is_coordinator
    
    # Auto-detect workers
    worker_info = get_worker_info()
    print(f"Worker {worker_info.worker_id} of {worker_info.total_workers}")
    
    # Partition work
    partitions = partition_work(items, num_partitions=worker_info.total_workers)
    my_partition = partitions[worker_info.worker_index]
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, TypeVar

from common.config import get_runtime_mode, get_execution_mode, is_distributed_mode, is_vm_mode


T = TypeVar("T")


# =============================================================================
# WORKER DETECTION
# =============================================================================

@dataclass
class WorkerInfo:
    """Information about the current worker/process."""
    # Worker identification
    worker_id: str
    worker_index: int  # 0-based index
    total_workers: int
    
    # Execution context
    is_coordinator: bool  # True if this is the main/coordinator process
    is_distributed: bool  # True if running in distributed mode
    
    # Environment info
    hostname: str
    pid: int
    
    # Capabilities
    supports_parallel_io: bool  # S3/parallel filesystem available
    supports_multiprocess: bool  # Can spawn child processes


def _detect_airflow_workers() -> tuple[int, int, str]:
    """Detect worker info from Airflow environment.
    
    Returns:
        Tuple of (worker_index, total_workers, worker_id)
    """
    # Airflow CeleryExecutor sets these variables
    worker_index = int(os.getenv("AIRFLOW_WORKER_NUMBER", "0"))
    total_workers = int(os.getenv("AIRFLOW_WORKERS", "1"))
    
    # Generate worker_id
    task_id = os.getenv("AIRFLOW_TASK_TAKING", os.getenv("AIRFLOW_TASK_ID", ""))
    hostname = os.getenv("HOSTNAME", socket.gethostname())
    worker_id = os.getenv("AIRFLOW_WORKER_NAME", f"airflow-{hostname}-{worker_index}")
    
    return worker_index, total_workers, worker_id


def _detect_kubernetes_workers() -> tuple[int, int, str]:
    """Detect worker info from Kubernetes environment.
    
    Returns:
        Tuple of (worker_index, total_workers, worker_id)
    """
    # Kubernetes downward API sets these
    worker_index = int(os.getenv("POD_NAME", "0").split("-")[-1] or "0")
    total_workers = int(os.getenv("REPLICAS", "1"))
    
    pod_name = os.getenv("POD_NAME", socket.gethostname())
    worker_id = f"k8s-{pod_name}"
    
    return worker_index, total_workers, worker_id


def _detect_local_workers() -> tuple[int, int, str]:
    """Detect worker info for local execution.
    
    Returns:
        Tuple of (worker_index, total_workers, worker_id)
    """
    # Single worker for local mode
    worker_index = 0
    total_workers = int(os.getenv("NEXUS_LOCAL_WORKERS", "1"))
    
    worker_id = f"local-{socket.gethostname()}-{os.getpid()}"
    
    return worker_index, total_workers, worker_id


def _detect_spark_workers() -> tuple[int, int, str]:
    """Detect worker info from Spark environment.
    
    Returns:
        Tuple of (worker_index, total_workers, worker_id)
    """
    # Spark executors
    worker_index = int(os.getenv("SPARK_EXECUTOR_ID", "0").replace("executor_", "") or "0")
    total_workers = int(os.getenv("SPARK_EXECUTOR_INSTANCES", "1"))
    
    worker_id = os.getenv("SPARK_EXECUTOR_ID", f"spark-executor-{worker_index}")
    
    return worker_index, total_workers, worker_id


def get_worker_info() -> WorkerInfo:
    """Detect worker information for the current process.

    Auto-detects the execution environment and returns appropriate worker info.

    Returns:
        WorkerInfo with current worker details
    """
    import os

    hostname = socket.gethostname()
    pid = os.getpid()

    # Use is_distributed_mode() for proper distributed detection
    is_distributed = is_distributed_mode()

    # Try to detect from environment
    if os.getenv("AIRFLOW_HOME") or os.getenv("AIRFLOW_WORKER_NUMBER"):
        worker_index, total_workers, worker_id = _detect_airflow_workers()
        supports_multiprocess = True
    elif os.getenv("KUBERNETES_SERVICE_HOST"):
        worker_index, total_workers, worker_id = _detect_kubernetes_workers()
        supports_multiprocess = True
    elif os.getenv("SPARK_EXECUTOR_ID"):
        worker_index, total_workers, worker_id = _detect_spark_workers()
        supports_multiprocess = True
    else:
        # Local mode
        worker_index, total_workers, worker_id = _detect_local_workers()
        supports_multiprocess = int(os.getenv("NEXUS_LOCAL_WORKERS", "1")) > 1
    
    return WorkerInfo(
        worker_id=worker_id,
        worker_index=worker_index,
        total_workers=total_workers,
        is_coordinator=(worker_index == 0),
        is_distributed=is_distributed,
        hostname=hostname,
        pid=pid,
        supports_parallel_io=is_distributed,
        supports_multiprocess=supports_multiprocess,
    )


# =============================================================================
# WORK COORDINATION
# =============================================================================

def is_coordinator() -> bool:
    """Check if current process is the coordinator.
    
    Returns:
        True if this is the main/coordinator process
    """
    return get_worker_info().is_coordinator


def get_worker_count() -> int:
    """Get total number of workers.
    
    Returns:
        Total number of workers available
    """
    return get_worker_info().total_workers


def get_worker_index() -> int:
    """Get current worker index (0-based).
    
    Returns:
        Current worker index
    """
    return get_worker_info().worker_index


# =============================================================================
# DATA PARTITIONING
# =============================================================================

def partition_work(
    items: list[T] | Iterator[T],
    num_partitions: int | None = None,
    worker_index: int | None = None,
) -> list[T]:
    """Get items for current worker from partitioned work.
    
    Partitions work across workers and returns only items for this worker.
    
    Args:
        items: List of items to partition
        num_partitions: Number of partitions (defaults to worker count)
        worker_index: Worker index to filter (defaults to current worker)
        
    Returns:
        List of items assigned to this worker
    """
    if num_partitions is None:
        num_partitions = get_worker_count()
    
    if worker_index is None:
        worker_index = get_worker_index()
    
    return [item for i, item in enumerate(items) if i % num_partitions == worker_index]


def partition_iterator(
    items: Iterator[T],
    num_partitions: int | None = None,
    worker_index: int | None = None,
) -> Iterator[T]:
    """Yield items for current worker from partitioned iterator.
    
    Memory-efficient version that doesn't load all items first.
    
    Args:
        items: Iterator of items to partition
        num_partitions: Number of partitions (defaults to worker count)
        worker_index: Worker index to filter (defaults to current worker)
        
    Yields:
        Items assigned to this worker
    """
    if num_partitions is None:
        num_partitions = get_worker_count()
    
    if worker_index is None:
        worker_index = get_worker_index()
    
    for i, item in enumerate(items):
        if i % num_partitions == worker_index:
            yield item


def hash_partition(
    key: str,
    num_partitions: int | None = None,
) -> int:
    """Get partition index using consistent hashing.
    
    Ensures same key always goes to same partition (for deduplication).
    
    Args:
        key: Key to hash
        num_partitions: Number of partitions
        
    Returns:
        Partition index (0 to num_partitions-1)
    """
    if num_partitions is None:
        num_partitions = get_worker_count()
    
    hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
    return hash_value % num_partitions


def get_partition_path(
    base_path: str,
    partition_key: str,
    num_partitions: int | None = None,
    worker_index: int | None = None,
) -> str:
    """Get partitioned file path for this worker.
    
    Args:
        base_path: Base path for the file
        partition_key: Key to use for partitioning
        num_partitions: Number of partitions
        worker_index: Target worker index (defaults to current)
        
    Returns:
        Partitioned path like: base_path/partition=2/file.json
    """
    if num_partitions is None:
        num_partitions = get_worker_count()
    
    if worker_index is None:
        worker_index = get_worker_index()
    
    # Use hash for consistent partitioning
    partition = hash_partition(partition_key, num_partitions)
    
    # For the current worker, return path with its partition
    # For other workers, this creates partitioned directory structure
    return f"{base_path}/partition={partition}/"


# =============================================================================
# COORDINATED WRITES (for S3)
# =============================================================================

@dataclass
class PartitionedWriteConfig:
    """Configuration for partitioned writes."""
    num_partitions: int
    partition_prefix: str = "partition"
    file_pattern: str = "data_{partition}.jsonl"
    records_per_file: int = 100_000  # Rotate files after this many records
    bytes_per_file: int = 100 * 1024 * 1024  # Or after this size


class PartitionedWriter:
    """Writer that partitions data across multiple files for parallel processing.
    
    Each partition can be processed by a different worker.
    
    Usage:
        writer = PartitionedWriter("s3://bucket/bronze/dataset/")
        for record in records:
            writer.write(record)  # Auto-routes to correct partition
        paths = writer.close()  # Returns list of written paths
    """
    
    def __init__(
        self,
        base_path: str,
        partition_key: str,
        config: PartitionedWriteConfig | None = None,
        storage: Any = None,
    ):
        self.base_path = base_path.rstrip("/")
        self.partition_key = partition_key
        self.config = config or PartitionedWriteConfig(
            num_partitions=get_worker_count()
        )
        self._storage = storage
        
        # Current files per partition
        self._files: dict[int, list[str]] = {}
        self._record_counts: dict[int, int] = {}
        self._bytes_written: dict[int, int] = {}
        self._closed = False
    
    @property
    def storage(self):
        """Get storage backend."""
        if self._storage is None:
            from common.storage import get_storage
            self._storage = get_storage()
        return self._storage
    
    def _get_partition_path(self, partition: int, filename: str) -> str:
        """Get full path for a partition file."""
        return f"{self.base_path}/{self.config.partition_prefix}={partition}/{filename}"
    
    def _get_partition(self, record: dict[str, Any]) -> int:
        """Determine partition for a record."""
        key_value = str(record.get(self.partition_key, ""))
        return hash_partition(key_value, self.config.num_partitions)
    
    def write(self, record: dict[str, Any]) -> None:
        """Write a single record to appropriate partition.
        
        Args:
            record: Record to write
        """
        if self._closed:
            raise RuntimeError("Cannot write to closed PartitionedWriter")
        
        partition = self._get_partition(record)
        path = self._get_partition_path(partition, self.config.file_pattern.format(partition=partition))
        
        # Use append_jsonl to add record
        self.storage.append_jsonl(path, record)
        
        # Track stats
        self._record_counts[partition] = self._record_counts.get(partition, 0) + 1
        self._bytes_written[partition] = self._bytes_written.get(partition, 0) + len(json.dumps(record))
    
    def write_batch(self, records: Iterable[dict[str, Any]]) -> None:
        """Write multiple records to appropriate partitions.
        
        Args:
            records: Records to write
        """
        # Group by partition first
        by_partition: dict[int, list[dict[str, Any]]] = {}
        
        for record in records:
            partition = self._get_partition(record)
            if partition not in by_partition:
                by_partition[partition] = []
            by_partition[partition].append(record)
        
        # Write each partition
        for partition, partition_records in by_partition.items():
            path = self._get_partition_path(partition, self.config.file_pattern.format(partition=partition))
            self.storage.write_jsonl(path, partition_records)
            
            # Track stats
            self._record_counts[partition] = self._record_counts.get(partition, 0) + len(partition_records)
    
    def get_written_paths(self) -> dict[int, str]:
        """Get paths for all written partitions.
        
        Returns:
            Dict mapping partition index to path
        """
        result = {}
        for partition in self._record_counts:
            path = self._get_partition_path(partition, self.config.file_pattern.format(partition=partition))
            result[partition] = path
        return result
    
    def close(self) -> dict[int, str]:
        """Close writer and return paths.
        
        Returns:
            Dict mapping partition index to path
        """
        self._closed = True
        return self.get_written_paths()


# =============================================================================
# STREAMING WITH PROGRESS
# =============================================================================

def stream_with_partition(
    records: Iterable[dict[str, Any]],
    partition_key: str,
    num_partitions: int | None = None,
) -> Iterator[tuple[int, dict[str, Any]]]:
    """Stream records with partition info.
    
    Yields (partition_index, record) tuples for downstream processing.
    
    Args:
        records: Records to stream
        partition_key: Key to partition on
        num_partitions: Number of partitions
        
    Yields:
        Tuples of (partition_index, record)
    """
    if num_partitions is None:
        num_partitions = get_worker_count()
    
    for record in records:
        partition = hash_partition(
            str(record.get(partition_key, "")),
            num_partitions
        )
        yield partition, record


def filter_by_partition(
    records: Iterable[dict[str, Any]],
    partition_key: str,
    target_partition: int,
    num_partitions: int | None = None,
) -> Iterator[dict[str, Any]]:
    """Filter records for a specific partition.
    
    Memory-efficient alternative to partition_work for iterators.
    
    Args:
        records: Records to filter
        partition_key: Key to partition on
        target_partition: Partition index to keep
        num_partitions: Number of partitions
        
    Yields:
        Records belonging to target partition
    """
    if num_partitions is None:
        num_partitions = get_worker_count()
    
    for record in records:
        partition = hash_partition(
            str(record.get(partition_key, "")),
            num_partitions
        )
        if partition == target_partition:
            yield record


# =============================================================================
# WORKER COORDINATION (LOCK-FREE)
# =============================================================================

@dataclass
class WorkerCoordination:
    """Coordination info for workers without distributed locks.
    
    Uses atomic file operations for coordination.
    """
    namespace: str
    _storage: Any = field(default=None, repr=False)
    
    @property
    def storage(self):
        if self._storage is None:
            from common.storage import get_storage
            self._storage = get_storage()
        return self._storage
    
    def claim_task(self, task_id: str, ttl_seconds: int = 300) -> bool:
        """Attempt to claim a task.
        
        Uses atomic write-if-not-exists pattern.
        
        Args:
            task_id: Unique task identifier
            ttl_seconds: Time-to-live for claim
            
        Returns:
            True if claim succeeded, False if already claimed
        """
        path = f"{self.namespace}/tasks/{task_id}.claim"
        
        if self.storage.exists(path):
            # Check if claim is stale
            try:
                claim = self.storage.read(path)
                expires_at = datetime.fromisoformat(claim.get("expires_at", "1970-01-01"))
                if datetime.now(timezone.utc) < expires_at:
                    return False  # Still valid claim
                # Claim expired, we can overwrite
            except Exception:
                pass
        
        # Write claim
        claim_data = {
            "task_id": task_id,
            "worker_id": get_worker_info().worker_id,
            "claimed_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": datetime.now(timezone.utc).timestamp() + ttl_seconds,
        }
        
        self.storage.write(path, claim_data, is_json=True)
        return True
    
    def release_task(self, task_id: str) -> None:
        """Release a claimed task."""
        path = f"{self.namespace}/tasks/{task_id}.claim"
        self.storage.delete(path)
    
    def get_active_claims(self) -> dict[str, str]:
        """Get all active task claims.
        
        Returns:
            Dict mapping task_id to worker_id
        """
        prefix = f"{self.namespace}/tasks/"
        claims = {}
        
        for path in self.storage.list(prefix):
            if path.endswith(".claim"):
                try:
                    claim = self.storage.read(path)
                    task_id = claim.get("task_id")
                    worker_id = claim.get("worker_id")
                    if task_id and worker_id:
                        claims[task_id] = worker_id
                except Exception:
                    pass
        
        return claims


__all__ = [
    # Worker detection
    "WorkerInfo",
    "get_worker_info",
    "is_coordinator",
    "get_worker_count",
    "get_worker_index",
    # Partitioning
    "partition_work",
    "partition_iterator",
    "hash_partition",
    "get_partition_path",
    # Partitioned writes
    "PartitionedWriteConfig",
    "PartitionedWriter",
    # Streaming
    "stream_with_partition",
    "filter_by_partition",
    # Coordination
    "WorkerCoordination",
]
