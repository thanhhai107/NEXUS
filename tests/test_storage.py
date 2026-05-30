"""Tests for storage abstraction layer."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from common.storage import (
    LocalStorageBackend,
    StorageConfig,
    get_storage,
    get_storage_config,
    reset_storage,
    write_json,
    read_json,
    exists,
    storage_context,
)
from common.config import is_vm_mode


class TestStorageConfig:
    """Tests for StorageConfig."""

    def test_local_config(self, monkeypatch):
        """Test local storage config."""
        monkeypatch.setenv("NEXUS_RUNTIME_MODE", "local")
        reset_storage()
        
        config = get_storage_config()
        
        assert config.mode == "local"
        assert config.base_path is not None

    def test_vm_config_requires_env_vars(self, monkeypatch):
        """Test VM config requires environment variables."""
        monkeypatch.setenv("NEXUS_RUNTIME_MODE", "vm")
        monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
        monkeypatch.setenv("MINIO_ROOT_USER", "testuser")
        monkeypatch.setenv("MINIO_ROOT_PASSWORD", "testpass")
        reset_storage()
        
        config = get_storage_config()
        
        assert config.mode == "vm"
        assert config.endpoint == "http://minio:9000"
        assert config.access_key == "testuser"


class TestLocalStorageBackend:
    """Tests for LocalStorageBackend."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.storage = LocalStorageBackend(Path(self.temp_dir))

    def test_write_and_read_json(self):
        """Test writing and reading JSON."""
        data = {"key": "value", "number": 42}
        
        path = self.storage.write("test/data.json", data)
        
        assert Path(path).exists()
        assert self.storage.exists("test/data.json")
        
        read_data = self.storage.read("test/data.json")
        assert read_data == data

    def test_write_and_read_jsonl(self):
        """Test writing and reading JSONL."""
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        
        self.storage.write_jsonl("test/records.jsonl", records)
        
        read_records = list(self.storage.read_jsonl("test/records.jsonl"))
        assert len(read_records) == 2
        assert read_records[0]["id"] == 1
        assert read_records[1]["name"] == "Bob"

    def test_append_jsonl(self):
        """Test appending to JSONL."""
        records = [{"id": 1}]
        self.storage.write_jsonl("test/append.jsonl", records)
        
        self.storage.append_jsonl("test/append.jsonl", {"id": 2})
        
        read_records = list(self.storage.read_jsonl("test/append.jsonl"))
        assert len(read_records) == 2

    def test_exists(self):
        """Test exists check."""
        assert not self.storage.exists("nonexistent.json")
        
        self.storage.write("exists.json", {"test": True})
        
        assert self.storage.exists("exists.json")

    def test_list(self):
        """Test listing files."""
        self.storage.write("dir1/file1.json", {"test": 1})
        self.storage.write("dir1/file2.json", {"test": 2})
        self.storage.write("dir2/file3.json", {"test": 3})
        
        files = self.storage.list("dir1")
        
        assert len(files) == 2
        assert any("file1.json" in f for f in files)
        assert any("file2.json" in f for f in files)

    def test_delete(self):
        """Test deleting files."""
        self.storage.write("delete_me.json", {"test": True})
        assert self.storage.exists("delete_me.json")
        
        result = self.storage.delete("delete_me.json")
        
        assert result is True
        assert not self.storage.exists("delete_me.json")

    def test_nested_directories(self):
        """Test nested directory creation."""
        path = self.storage.write("a/b/c/d/deep.json", {"deep": True})
        
        assert Path(path).exists()
        read_data = self.storage.read("a/b/c/d/deep.json")
        assert read_data["deep"] is True


class TestStorageFactory:
    """Tests for storage factory functions."""

    def setup_method(self):
        """Reset storage before each test."""
        reset_storage()

    def test_get_storage_local(self, monkeypatch):
        """Test getting local storage."""
        monkeypatch.setenv("NEXUS_RUNTIME_MODE", "local")
        reset_storage()
        
        storage = get_storage()
        
        assert isinstance(storage, LocalStorageBackend)

    def test_convenience_functions(self, tmp_path, monkeypatch):
        """Test convenience functions."""
        monkeypatch.setenv("NEXUS_RUNTIME_MODE", "local")
        monkeypatch.setenv("NEXUS_RUNTIME_DIR", str(tmp_path))
        reset_storage()
        
        data = {"test": "value"}
        write_json("test.json", data)
        
        assert exists("test.json")
        read_data = read_json("test.json")
        assert read_data == data


class TestStorageContext:
    """Tests for storage context manager."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()

    def test_context_manager(self):
        """Test storage context manager."""
        with storage_context("test") as ctx:
            ctx.storage = LocalStorageBackend(Path(self.temp_dir))
            ctx.write("data.json", {"value": 123})
        
        # Verify file was written
        path = Path(self.temp_dir) / "test" / "data.json"
        assert path.exists()
        
        data = json.loads(path.read_text())
        assert data["value"] == 123


class TestStorageIntegration:
    """Integration tests for storage with config."""

    def test_runtime_mode_detection(self, monkeypatch):
        """Test runtime mode detection."""
        # Test local mode
        monkeypatch.setenv("NEXUS_RUNTIME_MODE", "local")
        reset_storage()
        
        assert is_vm_mode() is False
        
        # Test VM mode
        monkeypatch.setenv("NEXUS_RUNTIME_MODE", "vm")
        reset_storage()
        
        # Note: is_vm_mode() checks NEXUS_RUNTIME_MODE, not just environment
        # But actual S3 usage requires MINIO_ENDPOINT to be set
        monkeypatch.setenv("MINIO_ENDPOINT", "http://localhost:9000")
        
        # Reset again to pick up new env
        reset_storage()
        
        config = get_storage_config()
        assert config.mode == "vm"
