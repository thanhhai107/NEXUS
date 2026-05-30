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


# =============================================================================
# WORKER HEALTH CHECK & HEARTBEAT
# =============================================================================

import subprocess

from common.storage import get_storage as _get_storage


@dataclass
class ServiceStatus:
    name: str
    role: str  # "master" | "worker" | "both"
    running: bool
    container_id: str | None = None
    uptime_seconds: int | None = None
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "role": self.role,
            "running": self.running,
            "container_id": self.container_id,
            "uptime_seconds": self.uptime_seconds,
            "detail": self.detail,
        }


@dataclass
class WorkerHealth:
    worker_id: str
    hostname: str
    role: str  # "master" | "worker"
    is_reachable: bool
    checked_at: str
    services: list[ServiceStatus]
    healthy_count: int
    total_count: int
    is_healthy: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "hostname": self.hostname,
            "role": self.role,
            "is_reachable": self.is_reachable,
            "checked_at": self.checked_at,
            "services": [s.to_dict() for s in self.services],
            "healthy_count": self.healthy_count,
            "total_count": self.total_count,
            "is_healthy": self.is_healthy,
        }


def _docker_ps() -> list[dict[str, str]]:
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.ID}}\t{{.Names}}\t{{.Status}}\t{{.RunningFor}}"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0:
            return []
        containers = []
        for line in result.stdout.strip().split("\n"):
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) >= 4:
                containers.append({
                    "id": parts[0],
                    "name": parts[1],
                    "status": parts[2],
                    "running_for": parts[3],
                })
        containers.sort(key=lambda c: len(c["name"]))  # shorter names first → exact matches win
        return containers
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return []
        containers = []
        for line in result.stdout.strip().split("\n"):
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) >= 4:
                containers.append({
                    "id": parts[0],
                    "name": parts[1],
                    "status": parts[2],
                    "running_for": parts[3],
                })
        return containers
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return []


def _parse_uptime(running_for: str) -> int | None:
    cleaned = running_for.lower()
    cleaned = cleaned.replace("about ", "").replace(" ago", "").strip()
    cleaned = cleaned.replace("a ", "1 ").replace("an ", "1 ")
    parts = cleaned.split()
    if not parts:
        return None
    total = 0
    i = 0
    while i < len(parts) - 1:
        try:
            val = int(parts[i])
            unit = parts[i + 1]
            if "hour" in unit:
                total += val * 3600
            elif "minute" in unit or "min" in unit:
                total += val * 60
            elif "second" in unit or "sec" in unit:
                total += val
            elif "day" in unit:
                total += val * 86400
            elif "week" in unit:
                total += val * 604800
            elif "month" in unit:
                total += val * 2592000
            i += 2
        except (ValueError, IndexError):
            i += 1
    return total if total > 0 else None


MASTER_SERVICES = {
    "zookeeper":            ("zookeeper-1", "master"),
    "kafka":                ("kafka-1", "master"),
    "minio":                ("minio-1", "master"),
    "spark":                ("spark", "master"),                    # nexus-spark (not spark-worker-*)
    "trino-coordinator":    ("trino-coordinator", "master"),
    "airflow-db":           ("airflow-db", "master"),
    "redis":                ("redis", "master"),
    "hive-metastore-db":    ("hive-metastore-db", "master"),
    "hive-metastore":       ("hive-metastore", "master"),          # Not hive-metastore-db
    "airflow-webserver":    ("airflow", "master"),                  # nexus-airflow (before scheduler)
    "airflow-scheduler":    ("airflow-scheduler", "master"),
    "superset":             ("superset", "master"),
    "api":                  ("api-1", "master"),                    # nexus-api-1
    "api-lb":               ("api-lb", "master"),
}

WORKER_SERVICES = {
    "zookeeper": ("zookeeper-2|zookeeper-3", "worker"),
    "kafka": ("kafka-2|kafka-3", "worker"),
    "minio": ("minio-2|minio-3|minio-4", "worker"),
    "spark-worker": ("spark-worker", "worker"),
    "trino-worker": ("trino-worker", "worker"),
    "airflow-worker": ("airflow-worker", "worker"),
}

OPTIONAL_SERVICES = {
    "openmetadata":          ("openmetadata", "master"),
    "marquez":               ("marquez", "master"),
}


def _match_container(container_name: str, patterns: str) -> bool:
    for pattern in patterns.split("|"):
        if pattern in container_name:
            return True
    return False


def check_local_services(role: str = "auto") -> list[ServiceStatus]:
    if role == "auto":
        role = "master" if is_coordinator() else "worker"

    containers = _docker_ps()

    services: list[ServiceStatus] = []
    service_defs = MASTER_SERVICES if role == "master" else WORKER_SERVICES
    used_containers: set[str] = set()

    for svc_name, (pattern, svc_role) in service_defs.items():
        matched = None
        for c in containers:
            if c["id"] in used_containers:
                continue
            if _match_container(c["name"], pattern):
                matched = c
                used_containers.add(c["id"])
                break

        is_running = matched is not None and ("up" in matched["status"].lower() or matched["status"].lower().startswith("up"))
        services.append(ServiceStatus(
            name=svc_name,
            role=svc_role,
            running=bool(matched) and is_running,
            container_id=matched["name"] if matched else None,
            uptime_seconds=_parse_uptime(matched["running_for"]) if matched and is_running else None,
            detail=matched["status"] if matched else "container not found",
        ))

    for svc_name, (pattern, svc_role) in OPTIONAL_SERVICES.items():
        matched = None
        for c in containers:
            if c["id"] in used_containers:
                continue
            if _match_container(c["name"], pattern):
                matched = c
                used_containers.add(c["id"])
                break
        services.append(ServiceStatus(
            name=svc_name,
            role=svc_role,
            running=matched is not None,
            container_id=matched["name"] if matched else None,
            uptime_seconds=_parse_uptime(matched["running_for"]) if matched else None,
            detail=matched["status"] if matched else "not present",
        ))

    return services


def check_worker_health(
    hostname: str | None = None,
    role: str = "auto",
    remote_ssh: str | None = None,
    timeout: int = 30,
) -> WorkerHealth:
    worker_info = get_worker_info()

    if hostname is None:
        hostname = socket.gethostname()
    if role == "auto":
        role = "master" if is_coordinator() else "worker"

    checked_at = datetime.now(timezone.utc).isoformat()

    if remote_ssh:
        is_reachable, services = _remote_docker_check(remote_ssh, role, timeout)
    else:
        is_reachable = True
        services = check_local_services(role)

    healthy = sum(1 for s in services if s.running)

    return WorkerHealth(
        worker_id=worker_info.worker_id if not remote_ssh else remote_ssh,
        hostname=hostname,
        role=role,
        is_reachable=is_reachable,
        checked_at=checked_at,
        services=services,
        healthy_count=healthy,
        total_count=len(services),
        is_healthy=is_reachable and healthy >= max(len(services) - 1, 1),
    )


def _remote_docker_check(ssh_host: str, role: str, timeout: int) -> tuple[bool, list[ServiceStatus]]:
    script = (
        "from common.worker import check_local_services; "
        "import json; "
        "svcs = check_local_services('{role}'); "
        "print(json.dumps([s.__dict__ for s in svcs]))"
    ).format(role=role)

    cmd = [
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        "-o", "BatchMode=yes",
        f"ConnectTimeout={min(timeout // 2, 15)}",
        ssh_host,
        f"cd /opt/nexus/NEXUS && source .venv/bin/activate && python -c \"{script}\"",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            return False, [ServiceStatus(
                name="ssh", role=role, running=False,
                detail=f"SSH failed: {result.stderr.strip() or 'exit code ' + str(result.returncode)}",
            )]
        try:
            raw = json.loads(result.stdout.strip().split("\n")[-1])
            services = [ServiceStatus(**s) for s in raw]
            return True, services
        except (json.JSONDecodeError, TypeError):
            return False, [ServiceStatus(
                name="parse", role=role, running=False,
                detail=f"Failed to parse remote output: {result.stdout[:200]}",
            )]
    except subprocess.TimeoutExpired:
        return False, [ServiceStatus(name="ssh", role=role, running=False, detail="SSH timeout")]
    except FileNotFoundError:
        return False, [ServiceStatus(name="ssh", role=role, running=False, detail="ssh command not found")]


def write_heartbeat(worker_id: str | None = None, ttl_seconds: int = 120) -> dict[str, Any]:
    storage = _get_storage(force_refresh=True)
    info = get_worker_info()
    wid = worker_id or info.worker_id
    path = f"nexus/heartbeats/{wid}.json"

    payload = {
        "worker_id": wid,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "worker_index": info.worker_index,
        "total_workers": info.total_workers,
        "is_coordinator": info.is_coordinator,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ttl_seconds": ttl_seconds,
    }
    storage.write(path, payload, is_json=True)
    return payload


def read_heartbeats(max_age_seconds: int = 300) -> list[dict[str, Any]]:
    storage = _get_storage(force_refresh=True)
    prefix = "nexus/heartbeats/"
    now = datetime.now(timezone.utc)
    heartbeats: list[dict[str, Any]] = []

    for path in storage.list(prefix):
        try:
            data = storage.read(path)
            ts_str = data.get("timestamp", "")
            if ts_str:
                ts = datetime.fromisoformat(ts_str)
                age = (now - ts).total_seconds()
                if age <= max_age_seconds:
                    data["age_seconds"] = int(age)
                    data["alive"] = True
                    heartbeats.append(data)
                else:
                    data["age_seconds"] = int(age)
                    data["alive"] = False
                    heartbeats.append(data)
        except Exception:
            pass

    heartbeats.sort(key=lambda h: h.get("timestamp", ""), reverse=True)
    return heartbeats


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
    # Health check
    "ServiceStatus",
    "WorkerHealth",
    "check_local_services",
    "check_worker_health",
    "write_heartbeat",
    "read_heartbeats",
]
