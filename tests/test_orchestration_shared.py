"""Tests for orchestration.shared modules."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from orchestration.shared.checkpoint import (
    load_checkpoint,
    save_checkpoint,
    is_chunk_completed,
    mark_chunk_complete,
    mark_chunk_failed,
    mark_chunk_skipped,
    get_completed_chunks,
    get_failed_chunks,
    clear_checkpoint,
)


class TestCheckpoint:
    """Tests for checkpoint management."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.run_id = "test_run_20260530"
        self.dataset = "test_dataset"

    def test_load_checkpoint_creates_new_if_not_exists(self):
        """Test that load_checkpoint creates a new checkpoint if none exists."""
        checkpoint = load_checkpoint(self.run_id, self.dataset)
        
        assert checkpoint["run_id"] == self.run_id
        assert checkpoint["dataset"] == self.dataset
        assert checkpoint["completed_chunks"] == {}
        assert checkpoint["failed_chunks"] == {}

    def test_save_and_load_checkpoint(self):
        """Test saving and loading checkpoint."""
        data = {
            "run_id": self.run_id,
            "dataset": self.dataset,
            "completed_chunks": {"chunk1": {"completed_at": "2026-05-30"}},
        }
        
        save_checkpoint(self.run_id, self.dataset, data)
        loaded = load_checkpoint(self.run_id, self.dataset)
        
        assert loaded["completed_chunks"]["chunk1"]["completed_at"] == "2026-05-30"

    def test_is_chunk_completed(self):
        """Test checking if chunk is completed."""
        # Clear any existing checkpoint first
        clear_checkpoint(self.run_id, self.dataset)
        
        # Initially not completed
        assert not is_chunk_completed(self.run_id, self.dataset, "chunk1")
        
        # Mark as complete
        mark_chunk_complete(self.run_id, self.dataset, "chunk1")
        
        # Now should be completed
        assert is_chunk_completed(self.run_id, self.dataset, "chunk1")

    def test_mark_chunk_failed(self):
        """Test marking chunk as failed."""
        mark_chunk_failed(self.run_id, self.dataset, "chunk1", "Test error")
        
        checkpoint = load_checkpoint(self.run_id, self.dataset)
        assert "chunk1" in checkpoint["failed_chunks"]
        assert checkpoint["failed_chunks"]["chunk1"]["error"] == "Test error"

    def test_mark_chunk_skipped(self):
        """Test marking chunk as skipped."""
        mark_chunk_skipped(self.run_id, self.dataset, "chunk1", "Already exists")
        
        checkpoint = load_checkpoint(self.run_id, self.dataset)
        assert "chunk1" in checkpoint["skipped_chunks"]
        assert checkpoint["skipped_chunks"]["chunk1"]["reason"] == "Already exists"

    def test_get_completed_chunks(self):
        """Test getting list of completed chunks."""
        mark_chunk_complete(self.run_id, self.dataset, "chunk1")
        mark_chunk_complete(self.run_id, self.dataset, "chunk2")
        
        completed = get_completed_chunks(self.run_id, self.dataset)
        assert "chunk1" in completed
        assert "chunk2" in completed

    def test_get_failed_chunks(self):
        """Test getting list of failed chunks."""
        mark_chunk_failed(self.run_id, self.dataset, "chunk1", "Error 1")
        mark_chunk_failed(self.run_id, self.dataset, "chunk2", "Error 2")
        
        failed = get_failed_chunks(self.run_id, self.dataset)
        assert "chunk1" in failed
        assert "chunk2" in failed

    def test_clear_checkpoint(self):
        """Test clearing a checkpoint."""
        mark_chunk_complete(self.run_id, self.dataset, "chunk1")
        assert is_chunk_completed(self.run_id, self.dataset, "chunk1")
        
        clear_checkpoint(self.run_id, self.dataset)
        checkpoint = load_checkpoint(self.run_id, self.dataset)
        assert checkpoint["completed_chunks"] == {}
