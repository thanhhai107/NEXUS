"""NEXUS Storage Abstraction Layer.

Provides unified storage interface supporting both local filesystem and S3/MinIO.
Based on NEXUS_RUNTIME_MODE:
- "local": Uses local filesystem (runtime/ directory)
- "vm": Uses S3/MinIO for distributed storage

Usage:
    from common.storage import get_storage, write_json, read_json
    
    # Unified API
    storage = get_storage()
    storage.write("bronze/dataset/file.json", data)
    data = storage.read("bronze/dataset/file.json")
    
    # Convenience functions
    write_json("path/to/file.json", data)
    records = list(read_jsonl("path/to/file.jsonl"))
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Callable, Iterator, TypeVar

from common.config import RUNTIME_DIR, get_runtime_mode, is_vm_mode


logger = logging.getLogger(__name__)

T = TypeVar("T")


# =============================================================================
# STORAGE CONFIGURATION
# =============================================================================

@dataclass
class StorageConfig:
    """Configuration for storage backend."""
    # Mode: "local" or "vm"
    mode: str
    # For S3/MinIO
    endpoint: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    bucket: str | None = None
    region: str | None = None
    # For local
    base_path: Path | None = None
    # Options
    secure: bool = False  # Use HTTPS for S3
    retry_attempts: int = 3
    retry_backoff: float = 1.0


def get_storage_config() -> StorageConfig:
    """Get storage configuration from environment."""
    import os
    
    mode = get_runtime_mode()
    
    if mode == "vm":
        # S3/MinIO configuration
        return StorageConfig(
            mode="vm",
            endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            bucket=os.getenv("NEXUS_BUCKET", "nexus-lakehouse"),
            region=os.getenv("AWS_REGION", "us-east-1"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        )
    else:
        # Local filesystem
        return StorageConfig(
            mode="local",
            base_path=RUNTIME_DIR,
        )


# =============================================================================
# STORAGE BACKEND INTERFACE
# =============================================================================

class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    def write(self, path: str, data: Any, *, is_json: bool = True) -> str:
        """Write data to path.
        
        Args:
            path: Relative path within storage
            data: Data to write
            is_json: If True, serialize as JSON
            
        Returns:
            Full path/URL of written file
        """
        pass
    
    @abstractmethod
    def write_bytes(self, path: str, data: bytes) -> str:
        """Write raw bytes to path.
        
        Args:
            path: Relative path within storage
            data: Raw bytes
            
        Returns:
            Full path/URL of written file
        """
        pass
    
    @abstractmethod
    def read(self, path: str) -> Any:
        """Read data from path.
        
        Args:
            path: Relative path within storage
            
        Returns:
            Parsed data (JSON if applicable)
        """
        pass
    
    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """Read raw bytes from path.
        
        Args:
            path: Relative path within storage
            
        Returns:
            Raw bytes
        """
        pass
    
    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if path exists.
        
        Args:
            path: Relative path within storage
            
        Returns:
            True if path exists
        """
        pass
    
    @abstractmethod
    def list(self, prefix: str = "") -> list[str]:
        """List paths under prefix.
        
        Args:
            prefix: Path prefix to list
            
        Returns:
            List of relative paths
        """
        pass
    
    @abstractmethod
    def delete(self, path: str) -> bool:
        """Delete path.
        
        Args:
            path: Relative path within storage
            
        Returns:
            True if deleted
        """
        pass
    
    @abstractmethod
    def mkdir(self, path: str) -> None:
        """Create directory (no-op for some backends).
        
        Args:
            path: Directory path to create
        """
        pass
    
    def read_jsonl(self, path: str) -> Iterator[dict[str, Any]]:
        """Read JSONL file line by line.
        
        Args:
            path: Relative path to JSONL file
            
        Yields:
            Parsed JSON objects
        """
        content = self.read_bytes(path).decode("utf-8")
        for line in content.splitlines():
            line = line.strip()
            if line:
                yield json.loads(line)
    
    def write_jsonl(self, path: str, records: Iterator[dict[str, Any]]) -> str:
        """Write JSONL file from records iterator.
        
        Args:
            path: Relative path to JSONL file
            records: Iterator of records
            
        Returns:
            Full path/URL of written file
        """
        lines = []
        for record in records:
            lines.append(json.dumps(record, ensure_ascii=False))
        content = "\n".join(lines) + "\n"
        return self.write_bytes(path, content.encode("utf-8"))
    
    def append_jsonl(self, path: str, record: dict[str, Any]) -> str:
        """Append a single record to JSONL file.
        
        Args:
            path: Relative path to JSONL file
            record: Record to append
            
        Returns:
            Full path/URL of file
        """
        line = json.dumps(record, ensure_ascii=False) + "\n"
        
        if self.exists(path):
            # Read existing, append, write back
            existing = self.read_bytes(path)
            content = existing + line.encode("utf-8")
        else:
            content = line.encode("utf-8")
        
        return self.write_bytes(path, content)
    
    def iter_files(
        self,
        prefix: str = "",
        pattern: str = "**/*",
        filter_fn: Callable[[str], bool] | None = None,
    ) -> Iterator[str]:
        """Iterate over files matching pattern.
        
        Args:
            prefix: Path prefix
            pattern: Glob pattern relative to prefix
            filter_fn: Optional filter function
            
        Yields:
            Matching paths
        """
        for path in self.list(prefix):
            import fnmatch
            if fnmatch.fnmatch(path, pattern):
                if filter_fn is None or filter_fn(path):
                    yield path
    
    def get_full_path(self, path: str) -> str:
        """Get full path/URL for a relative path.
        
        Args:
            path: Relative path
            
        Returns:
            Full path or S3 URL
        """
        return path


# =============================================================================
# LOCAL FILESYSTEM BACKEND
# =============================================================================

class LocalStorageBackend(StorageBackend):
    """Local filesystem storage backend."""
    
    def __init__(self, base_path: Path | None = None):
        self.base_path = base_path or RUNTIME_DIR
        self._ensure_base_path()
    
    def _ensure_base_path(self) -> None:
        """Ensure base path exists."""
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def _resolve(self, path: str) -> Path:
        """Resolve relative path to absolute."""
        return self.base_path / path
    
    def write(self, path: str, data: Any, *, is_json: bool = True) -> str:
        file_path = self._resolve(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        if is_json:
            content = json.dumps(data, indent=2, ensure_ascii=False, default=str)
            file_path.write_text(content, encoding="utf-8")
        else:
            file_path.write_text(str(data), encoding="utf-8")
        
        return str(file_path)
    
    def write_bytes(self, path: str, data: bytes) -> str:
        file_path = self._resolve(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(data)
        return str(file_path)
    
    def read(self, path: str) -> Any:
        file_path = self._resolve(path)
        content = file_path.read_text(encoding="utf-8")
        
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return content
    
    def read_bytes(self, path: str) -> bytes:
        return self._resolve(path).read_bytes()
    
    def exists(self, path: str) -> bool:
        return self._resolve(path).exists()
    
    def list(self, prefix: str = "") -> list[str]:
        base = self._resolve(prefix) if prefix else self.base_path
        
        if not base.exists():
            return []
        
        if base.is_file():
            return [prefix]
        
        paths = []
        for p in base.rglob("*"):
            if p.is_file():
                rel = p.relative_to(self.base_path)
                paths.append(str(rel))
        
        return sorted(paths)
    
    def delete(self, path: str) -> bool:
        file_path = self._resolve(path)
        if file_path.exists():
            file_path.unlink()
            return True
        return False
    
    def mkdir(self, path: str) -> None:
        self._resolve(path).mkdir(parents=True, exist_ok=True)
    
    def get_full_path(self, path: str) -> str:
        return str(self._resolve(path))


# =============================================================================
# S3/MINIO BACKEND
# =============================================================================

class S3StorageBackend(StorageBackend):
    """S3/MinIO storage backend."""
    
    _client: Any = None
    _bucket: str | None = None
    
    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        bucket: str | None = None,
        region: str | None = None,
        secure: bool = False,
    ):
        self.endpoint = endpoint or "http://localhost:9000"
        self.access_key = access_key or "minioadmin"
        self.secret_key = secret_key or "minioadmin"
        self.bucket = bucket or "nexus-lakehouse"
        self.region = region or "us-east-1"
        self.secure = secure
        
        self._init_client()
    
    def _init_client(self) -> None:
        """Initialize S3 client."""
        try:
            import boto3
            from botocore.config import Config
            
            config = Config(
                retries={"max_attempts": 3},
                signature_version="s3v4",
            )
            
            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
                config=config,
                use_ssl=self.secure,
            )
            
            # Ensure bucket exists
            try:
                self._client.head_bucket(Bucket=self.bucket)
            except Exception:
                self._client.create_bucket(Bucket=self.bucket)
            
            self._bucket = self.bucket
            logger.info(f"S3 backend initialized: {self.endpoint}/{self.bucket}")
            
        except ImportError:
            logger.warning("boto3 not installed, S3 storage unavailable")
            self._client = None
    
    def _ensure_client(self) -> None:
        """Ensure S3 client is available."""
        if self._client is None:
            raise RuntimeError("S3 client not initialized. Install boto3: pip install boto3")
    
    def write(self, path: str, data: Any, *, is_json: bool = True) -> str:
        self._ensure_client()
        
        if is_json:
            content = json.dumps(data, indent=2, ensure_ascii=False, default=str)
        else:
            content = str(data)
        
        self._client.put_object(
            Bucket=self._bucket,
            Key=path,
            Body=content.encode("utf-8"),
            ContentType="application/json" if is_json else "text/plain",
        )
        
        return f"s3://{self._bucket}/{path}"
    
    def write_bytes(self, path: str, data: bytes) -> str:
        self._ensure_client()
        
        self._client.put_object(
            Bucket=self._bucket,
            Key=path,
            Body=data,
        )
        
        return f"s3://{self._bucket}/{path}"
    
    def read(self, path: str) -> Any:
        content = self.read_bytes(path).decode("utf-8")
        
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return content
    
    def read_bytes(self, path: str) -> bytes:
        self._ensure_client()
        
        response = self._client.get_object(Bucket=self._bucket, Key=path)
        return response["Body"].read()
    
    def exists(self, path: str) -> bool:
        self._ensure_client()
        
        try:
            self._client.head_object(Bucket=self._bucket, Key=path)
            return True
        except Exception:
            return False
    
    def list(self, prefix: str = "") -> list[str]:
        self._ensure_client()
        
        paths = []
        paginator = self._client.get_paginator("list_objects_v2")
        
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                paths.append(obj["Key"])
        
        return sorted(paths)
    
    def delete(self, path: str) -> bool:
        self._ensure_client()
        
        try:
            self._client.delete_object(Bucket=self._bucket, Key=path)
            return True
        except Exception:
            return False
    
    def mkdir(self, path: str) -> None:
        # S3 doesn't have directories, but we can create a .keep file
        if path and not path.endswith("/"):
            path = path + "/"
        self.write_bytes(path + ".keep", b"")


# =============================================================================
# STORAGE FACTORY & SINGLETON
# =============================================================================

_storage_backend: StorageBackend | None = None


def get_storage(force_refresh: bool = False) -> StorageBackend:
    """Get storage backend singleton.
    
    Args:
        force_refresh: Force re-initialization
        
    Returns:
        StorageBackend instance
    """
    global _storage_backend
    
    if _storage_backend is None or force_refresh:
        config = get_storage_config()
        
        if config.mode == "vm":
            _storage_backend = S3StorageBackend(
                endpoint=config.endpoint,
                access_key=config.access_key,
                secret_key=config.secret_key,
                bucket=config.bucket,
                region=config.region,
                secure=config.secure,
            )
        else:
            _storage_backend = LocalStorageBackend(base_path=config.base_path)
    
    return _storage_backend


def reset_storage() -> None:
    """Reset storage backend (for testing)."""
    global _storage_backend
    _storage_backend = None


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def write_json(path: str, data: Any) -> str:
    """Write JSON data to storage.
    
    Args:
        path: Relative path
        data: Data to serialize
        
    Returns:
        Full path/URL of written file
    """
    return get_storage().write(path, data, is_json=True)


def read_json(path: str) -> Any:
    """Read JSON data from storage.
    
    Args:
        path: Relative path
        
    Returns:
        Parsed JSON data
    """
    return get_storage().read(path)


def write_jsonl(path: str, records: Iterator[dict[str, Any]]) -> str:
    """Write JSONL data to storage.
    
    Args:
        path: Relative path
        records: Iterator of records
        
    Returns:
        Full path/URL of written file
    """
    return get_storage().write_jsonl(path, records)


def read_jsonl(path: str) -> Iterator[dict[str, Any]]:
    """Read JSONL data from storage.
    
    Args:
        path: Relative path
        
    Yields:
        Parsed JSON objects
    """
    return get_storage().read_jsonl(path)


def exists(path: str) -> bool:
    """Check if path exists in storage.
    
    Args:
        path: Relative path
        
    Returns:
        True if exists
    """
    return get_storage().exists(path)


def list_files(prefix: str = "") -> list[str]:
    """List files in storage.
    
    Args:
        prefix: Path prefix
        
    Returns:
        List of relative paths
    """
    return get_storage().list(prefix)


def delete(path: str) -> bool:
    """Delete path from storage.
    
    Args:
        path: Relative path
        
    Returns:
        True if deleted
    """
    return get_storage().delete(path)


# =============================================================================
# CONTEXT MANAGER FOR STORAGE OPERATIONS
# =============================================================================

class StorageContext:
    """Context manager for storage operations with automatic cleanup."""
    
    def __init__(self, prefix: str):
        self.prefix = prefix
        self.storage = get_storage()
    
    def __enter__(self) -> "StorageContext":
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # No cleanup needed for most backends
        pass
    
    def write(self, path: str, data: Any) -> str:
        """Write relative to prefix."""
        return self.storage.write(f"{self.prefix}/{path}", data)
    
    def read(self, path: str) -> Any:
        """Read relative to prefix."""
        return self.storage.read(f"{self.prefix}/{path}")
    
    def list(self) -> list[str]:
        """List files under prefix."""
        return self.storage.list(self.prefix)


def storage_context(prefix: str) -> StorageContext:
    """Create storage context for prefix.
    
    Args:
        prefix: Path prefix for all operations
        
    Usage:
        with storage_context("bronze/tfl_arrivals") as ctx:
            ctx.write("data.json", records)
            files = ctx.list()
    """
    return StorageContext(prefix)


__all__ = [
    # Configuration
    "StorageConfig",
    "get_storage_config",
    # Backends
    "StorageBackend",
    "LocalStorageBackend",
    "S3StorageBackend",
    # Factory
    "get_storage",
    "reset_storage",
    # Convenience
    "write_json",
    "read_json",
    "write_jsonl",
    "read_jsonl",
    "exists",
    "list_files",
    "delete",
    "StorageContext",
    "storage_context",
]
