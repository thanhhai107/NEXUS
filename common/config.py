"""
NEXUS Configuration Module.

Unified config cho cả local (runtime/) và VM (/data/).
Cấu trúc thư mục theo Medallion Architecture với metadata và governance:

    runtime/
    ├── lake/                    # Data Layer (Bronze → Silver → Gold)
    │   ├── bronze/             # Raw data gốc (source format)
    │   ├── silver/             # Validated + standardized data
    │   └── gold/               # Business aggregates
    │
    ├── catalog/                 # Metadata Layer
    │   ├── schemas/           # Versioned schemas
    │   └── datasets/           # Dataset registry
    │
    ├── governance/              # Governance Layer
    │   ├── semantic/          # Semantic annotations
    │   └── quality/           # Quality reports
    │
    ├── checkpoints/             # Orchestration state
    │
    ├── raw/                    # Raw envelope landing zone
    ├── dlq/                    # Dead Letter Queue
    ├── quarantine/              # Invalid records
    ├── staging/                # Temporary files
    ├── tmp/                    # Temp files
    ├── logs/                    # Application logs
    └── metrics/                # Prometheus metrics
"""

from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOMAINS_DIR = PROJECT_ROOT / "domains"

try:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env", override=False)
except ImportError:
    pass


def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


# =============================================================================
# RUNTIME DIRECTORY RESOLUTION
# =============================================================================
# Resolution order:
#   1. NEXUS_RUNTIME_DIR environment variable (highest priority)
#   2. NEXUS_RUNTIME_MODE + default paths:
#      - "local": PROJECT_ROOT / "runtime"
#      - "vm": "/data" (AWS EC2 or GCP VM)
#   3. PROJECT_ROOT / "runtime" (default fallback)

def _resolve_runtime_dir() -> Path:
    # Check environment variable first
    env_runtime = os.getenv("NEXUS_RUNTIME_DIR")
    if env_runtime:
        return Path(env_runtime).resolve()

    # Check runtime mode
    runtime_mode = os.getenv("NEXUS_RUNTIME_MODE", "local").lower()
    if runtime_mode == "vm":
        return Path("/data").resolve()
    
    # Default: PROJECT_ROOT / "runtime"
    return PROJECT_ROOT / "runtime"


RUNTIME_DIR = _resolve_runtime_dir()


def get_runtime_mode() -> str:
    """Get the current runtime mode.
    
    Returns:
        "local" or "vm" depending on NEXUS_RUNTIME_MODE setting
    """
    return os.getenv("NEXUS_RUNTIME_MODE", "local").lower()


def is_vm_mode() -> bool:
    """Check if running in VM mode (using /data/).

    Returns:
        True if NEXUS_RUNTIME_MODE=vm or NEXUS_FORCE_VM=true
        (NEXUS_FORCE_GCP is still accepted for backward compatibility)
    """
    if os.getenv("NEXUS_FORCE_VM") or os.getenv("NEXUS_FORCE_GCP"):
        return True
    return get_runtime_mode() == "vm"


def is_distributed_mode() -> bool:
    """Check if running in distributed execution mode (worker cluster).

    Unlike is_vm_mode() which only checks data storage location,
    this checks if work is being distributed across multiple workers.

    Returns:
        True if:
        - NEXUS_DISTRIBUTED_MODE=true|1|yes|on
        - AIRFLOW_WORKER_NUMBER is set (Airflow CeleryExecutor)
        - KUBERNETES_SERVICE_HOST is set (K8s)
        - SPARK_EXECUTOR_ID is set (Spark)
    """
    # Explicit config
    val = os.getenv("NEXUS_DISTRIBUTED_MODE", "").lower()
    if val in {"1", "true", "yes", "on"}:
        return True
    if val in {"0", "false", "no", "off"}:
        return False

    # Auto-detect from orchestration environment
    if os.getenv("AIRFLOW_WORKER_NUMBER") is not None:
        return True
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        return True
    if os.getenv("SPARK_EXECUTOR_ID") is not None:
        return True

    return False


def get_worker_count() -> int:
    """Get total number of workers available.

    Returns:
        Number of workers (from environment or local config)
    """
    # Try Airflow
    if os.getenv("AIRFLOW_WORKERS"):
        return int(os.getenv("AIRFLOW_WORKERS", "1"))
    # Try Kubernetes
    if os.getenv("REPLICAS"):
        return int(os.getenv("REPLICAS", "1"))
    # Try Spark
    if os.getenv("SPARK_EXECUTOR_INSTANCES"):
        return int(os.getenv("SPARK_EXECUTOR_INSTANCES", "1"))
    # Default from config
    return int(os.getenv("NEXUS_LOCAL_WORKERS", "1"))


def get_execution_mode() -> str:
    """Get detailed execution mode description.

    Returns:
        One of:
        - "local_single" - Single process, local data
        - "local_multi" - Multiple workers, local data
        - "distributed_single" - Single worker in distributed cluster
        - "distributed_multi" - Multiple workers in distributed cluster
    """
    import os as _os

    distributed = is_distributed_mode()

    # Get worker count
    if _os.getenv("AIRFLOW_WORKERS"):
        workers = int(_os.getenv("AIRFLOW_WORKERS", "1"))
    elif _os.getenv("REPLICAS"):
        workers = int(_os.getenv("REPLICAS", "1"))
    elif _os.getenv("SPARK_EXECUTOR_INSTANCES"):
        workers = int(_os.getenv("SPARK_EXECUTOR_INSTANCES", "1"))
    else:
        workers = int(_os.getenv("NEXUS_LOCAL_WORKERS", "1"))

    if distributed:
        if workers > 1:
            return "distributed_multi"
        return "distributed_single"
    else:
        if workers > 1:
            return "local_multi"
        return "local_single"


# ============================================================================
# SPARK CLUSTER CONFIG
# ============================================================================
# Configuration for connecting to Spark cluster (standalone or YARN/K8s)

def get_spark_master_url() -> str:
    """Get Spark master URL for cluster mode.

    Returns:
        Spark master URL (e.g., 'spark://host:7077' for standalone,
        'yarn', 'k8s://https://...', or 'local[*]' for local)
    """
    return os.getenv("SPARK_MASTER_URL", "local[*]")


def get_spark_config() -> dict[str, str]:
    """Get Spark configuration for cluster mode.

    Returns:
        Dict of Spark config key-value pairs
    """
    config = {}

    # MinIO/S3 configuration
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    if minio_endpoint:
        config["spark.hadoop.fs.s3a.endpoint"] = minio_endpoint
        config["spark.hadoop.fs.s3a.access.key"] = os.getenv("MINIO_ROOT_USER", "minioadmin")
        config["spark.hadoop.fs.s3a.secret.key"] = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        config["spark.hadoop.fs.s3a.path.style.access"] = "true"
        config["spark.hadoop.fs.s3a.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"

    # Iceberg configuration
    config["spark.sql.extensions"] = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    config["spark.sql.catalog.spark_catalog"] = "org.apache.iceberg.spark.SparkCatalog"
    config["spark.sql.catalog.spark_catalog.type"] = "hive"

    return config


# ============================================================================
# LAKE STRUCTURE (Medallion Architecture)
# ============================================================================
# Data lake với 3 tier: Bronze → Silver → Gold
#
# Directory structure:
#   lake/bronze/{dataset}/run_id={run_id}/
#       ├── metadata/       # Checkpoint, profile, request log
#       ├── published/      # Published manifest
#       ├── raw/            # Downloaded raw files
#       └── staging/        # Temporary files during download
#
#   lake/silver/{dataset}/                   # Validated, standardized data
#   lake/gold/{dataset}/                     # Business aggregates

LAKE_DIR = RUNTIME_DIR / "lake"
BRONZE_DIR = LAKE_DIR / "bronze"  # Raw data gốc (source format)
SILVER_DIR = LAKE_DIR / "silver"  # Validated + standardized data
GOLD_DIR = LAKE_DIR / "gold"      # Business aggregates (optional)


# ============================================================================
# CATALOG (Metadata Service)
# ============================================================================
# Versioned schemas và dataset registry
#
# Structure:
#   catalog/
#       ├── schemas/
#       │   └── {dataset}/
#       │       ├── v1/
#       │       │   └── schema.json
#       │       └── v2/
#       │           └── schema.json
#       └── datasets/
#           └── {dataset}.json

CATALOG_DIR = RUNTIME_DIR / "catalog"
SCHEMAS_DIR = CATALOG_DIR / "schemas"  # Versioned schemas
DATASETS_DIR = CATALOG_DIR / "datasets"  # Dataset registry


# ============================================================================
# GOVERNANCE (Semantic + Quality)
# ============================================================================
# Semantic annotations và quality reports
#
# Structure:
#   governance/
#       ├── semantic/
#       │   └── {dataset}/
#       │       └── v1/
#       │           └── annotations.json
#       └── quality/
#           └── {dataset}/
#               └── run_id={run_id}/
#                   └── quality_report.json

GOVERNANCE_DIR = RUNTIME_DIR / "governance"
SEMANTIC_DIR = GOVERNANCE_DIR / "semantic"  # Semantic annotations
QUALITY_DIR = GOVERNANCE_DIR / "quality"     # Quality reports


# ============================================================================
# ORCHESTRATION STATE
# ============================================================================
# Checkpoints và pipeline state
#
# Structure:
#   checkpoints/
#       └── {dataset}/
#           └── {run_id}.checkpoint.json

CHECKPOINTS_DIR = RUNTIME_DIR / "checkpoints"


# ============================================================================
# RUNTIME WORKSPACE (Temporary)
# ============================================================================

STAGING_DIR = RUNTIME_DIR / "staging"  # Files đang download
TMP_DIR = RUNTIME_DIR / "tmp"          # Temp files
LOGS_DIR = RUNTIME_DIR / "logs"       # Application logs
METRICS_DIR = RUNTIME_DIR / "metrics"  # Prometheus metrics


# ============================================================================
# ERROR HANDLING
# ============================================================================

DLQ_DIR = RUNTIME_DIR / "dlq"          # Dead Letter Queue
QUARANTINE_DIR = RUNTIME_DIR / "quarantine"  # Invalid records


# ============================================================================
# BRONZE ENVELOPE LANDING ZONE
# ============================================================================
# Canonical raw JSONL envelopes after ingestion. Spark bronze→Silver jobs
# consume RAW_DIR. Source artifacts remain under BRONZE_DIR per run.

RAW_DIR = RUNTIME_DIR / "bronze"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_runtime_path(*parts: str) -> Path:
    """Get path within runtime directory, creating it if needed."""
    path = RUNTIME_DIR.joinpath(*parts)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_bronze_path(
    dataset: str,
    run_id: str,
    subdir: str = "raw",
) -> Path:
    """Get bronze path for a dataset run.
    
    Args:
        dataset: Dataset name (e.g., 'tpcds_store_sales', 'tpcds_customer')
        run_id: Run ID (e.g., '20260527T033320Z')
        subdir: Subdirectory ('raw', 'staging', 'metadata', 'published')
    
    Returns:
        Path to the bronze subdirectory for this run
    """
    path = BRONZE_DIR / dataset / f"run_id={run_id}" / subdir
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_silver_path(
    dataset: str,
    run_id: str,
    filename: str | None = None,
) -> Path:
    """Get silver path for a dataset.
    
    Args:
        dataset: Dataset name
        run_id: Run ID
        filename: Optional filename (e.g., '{dataset}_{run_id}.jsonl')
    
    Returns:
        Path to the silver directory or file for this dataset run
    """
    if filename:
        path = SILVER_DIR / dataset / filename
    else:
        path = SILVER_DIR / dataset
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def get_schema_path(dataset: str) -> Path:
    """Get schema path for a dataset."""
    return SCHEMAS_DIR / f"{dataset}.schema.json"


def is_aws_ec2() -> bool:
    """Check if running on AWS EC2 by checking IMDS (Instance Metadata Service).

    Tries IMDSv2 first (token-based), falls back to IMDSv1.

    Note: This checks actual AWS metadata service. For mode-based detection,
    use is_vm_mode() instead.
    """
    try:
        socket.setdefaulttimeout(2)
        sock = socket.create_connection(("169.254.169.254", 80), timeout=2)
        sock.close()
        return True
    except (socket.timeout, socket.error, OSError):
        return False


def is_gcp_vm() -> bool:
    """Check if running on GCP VM by checking metadata service.
    
    Deprecated: Use is_aws_ec2() for AWS deployments. This function is kept
    for backward compatibility with existing GCP deployments.

    Note: This checks actual GCP metadata service. For mode-based detection,
    use is_vm_mode() instead.
    """
    try:
        socket.setdefaulttimeout(2)
        sock = socket.create_connection(("metadata.google.internal", 80), timeout=2)
        sock.close()
        return True
    except (socket.timeout, socket.error, OSError):
        return False


def get_effective_runtime_dir() -> tuple[Path, str]:
    """Get effective runtime dir and where it's mounted.

    Returns:
        Tuple of (runtime_dir, location) where location is 'vm' or 'local'
    """
    if is_vm_mode() or is_aws_ec2() or is_gcp_vm() or os.getenv("NEXUS_FORCE_GCP"):
        return (RUNTIME_DIR, "vm")
    return (RUNTIME_DIR, "local")


def get_config_runtime_dir() -> str:
    """Get runtime_dir from config file (for documentation/UI purposes)."""
    config_yaml = load_yaml(PROJECT_ROOT / "ingestion" / "config" / "download_defaults.yml")
    return config_yaml.get("runtime_dir", "")


def set_config_runtime_dir(value: str) -> None:
    """Set runtime_dir in config file."""
    config_path = PROJECT_ROOT / "ingestion" / "config" / "download_defaults.yml"
    config_yaml = load_yaml(config_path)
    config_yaml["runtime_dir"] = value
    with config_path.open("w", encoding="utf-8") as f:
        yaml.dump(config_yaml, f, default_flow_style=False, sort_keys=False)


# ============================================================================
# MINIO/S3 CONFIGURATION
# ============================================================================
# Configuration for MinIO/S3 distributed storage
# Used when NEXUS_RUNTIME_MODE=vm

def get_minio_config() -> dict[str, Any]:
    """Get MinIO/S3 configuration from environment.
    
    Returns:
        Dict with MinIO/S3 settings
    """
    import os
    
    return {
        "endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        "access_key": os.getenv("MINIO_ROOT_USER", "minioadmin"),
        "secret_key": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        "bucket": os.getenv("NEXUS_BUCKET", "nexus-lakehouse"),
        "region": os.getenv("AWS_REGION", "us-east-1"),
        "secure": os.getenv("MINIO_SECURE", "false").lower() == "true",
        "session_token": os.getenv("AWS_SESSION_TOKEN"),
    }


def is_minio_available() -> bool:
    """Check if MinIO/S3 is available and configured.
    
    Returns:
        True if MinIO/S3 is available
    """
    import os
    
    # Check environment variables
    endpoint = os.getenv("MINIO_ENDPOINT")
    if not endpoint:
        return False
    
    # Check mode
    if get_runtime_mode() != "vm":
        return False
    
    return True


# ============================================================================
# CONFIGURATION LOADERS
# ============================================================================

def load_polling_config(config_dir: Path | None = None) -> dict[str, Any]:
    """Load polling configuration from download_defaults.yml."""
    config_dir = config_dir or (PROJECT_ROOT / "ingestion" / "config")
    config = load_yaml(config_dir / "download_defaults.yml")
    return config.get("polling", {})


def load_backfill_config(config_dir: Path | None = None) -> dict[str, Any]:
    """Load backfill configuration from download_defaults.yml."""
    config_dir = config_dir or (PROJECT_ROOT / "ingestion" / "config")
    config = load_yaml(config_dir / "download_defaults.yml")
    return config.get("backfill", {})


def get_polling_interval(source: str) -> int:
    """Get polling interval in seconds for a source."""
    polling_config = load_polling_config()
    source_config = polling_config.get(source, {})
    return source_config.get("interval_seconds", 0)


def is_polling_enabled(source: str) -> bool:
    """Check if polling is enabled for a source."""
    return get_polling_interval(source) > 0


def _get_active_domains() -> set[str] | None:
    return {"tpc"}


def load_dataset_catalog(domains_dir: Path = DOMAINS_DIR) -> dict[str, Any]:
    """Load dataset catalog from domains/ directory."""
    active = _get_active_domains()
    datasets: dict[str, Any] = {}
    for path in sorted(domains_dir.glob("*/datasets.yml")):
        if path.name.startswith("datasets.yml.bak"):
            continue
        domain = path.parent.name
        if active is not None and domain not in active:
            continue
        datasets.update(load_yaml(path).get("datasets", {}))
    return {"datasets": datasets}


def load_quality_config(
    domains_dir: Path = DOMAINS_DIR,
    config_dir: Path | None = None,
) -> dict[str, Any]:
    """Load quality rules from domains/ and governance/config/ directories."""
    config_dir = config_dir or (PROJECT_ROOT / "governance" / "config")
    default_rules = load_yaml(config_dir / "quality_defaults.yml").get("default_rules", {})
    active = _get_active_domains()
    datasets: dict[str, Any] = {}
    for path in sorted(domains_dir.glob("*/quality_rules.yml")):
        if path.name.startswith("quality_rules.yml.bak"):
            continue
        domain = path.parent.name
        if active is not None and domain not in active:
            continue
        datasets.update(load_yaml(path).get("datasets", {}))
    return {"default_rules": default_rules, "datasets": datasets}


def load_semantic_config(
    domains_dir: Path = DOMAINS_DIR,
    config_dir: Path | None = None,
) -> dict[str, Any]:
    """Load semantic rules from domains/ and governance/config/ directories."""
    config_dir = config_dir or (PROJECT_ROOT / "governance" / "config")
    default_semantic = load_yaml(config_dir / "semantic_defaults.yml").get("default_semantic", {})
    active = _get_active_domains()
    datasets: dict[str, Any] = {}
    for path in sorted(domains_dir.glob("*/semantic_rules.yml")):
        if path.name.startswith("semantic_rules.yml.bak"):
            continue
        domain = path.parent.name
        if active is not None and domain not in active:
            continue
        datasets.update(load_yaml(path).get("datasets", {}))
    return {"default_semantic": default_semantic, "datasets": datasets}

def load_governance_defaults(config_dir: Path | None = None) -> dict[str, Any]:
    """Load governance defaults."""
    config_dir = config_dir or (PROJECT_ROOT / "governance" / "config")
    return load_yaml(config_dir / "governance_config.yml").get("default", {})
