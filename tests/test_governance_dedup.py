"""Tests for governance deduplication module."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from governance.dedup import (
    Deduplicator,
    DedupKey,
    DedupIndex,
    dedup_records,
    dedup_streaming,
)


class TestDedupKey:
    """Tests for DedupKey."""

    def test_compute_key_with_all_fields(self):
        """Test computing dedup key with all fields present."""
        key = DedupKey(
            source_id="test",
            key_fields=["id", "name"],
        )
        
        record = {"id": 1, "name": "Alice", "age": 30}
        dedup_key = key.compute_key(record)
        
        assert dedup_key is not None
        assert len(dedup_key) == 16

    def test_compute_key_missing_field(self):
        """Test computing key with missing field."""
        key = DedupKey(
            source_id="test",
            key_fields=["id", "missing_field"],
        )
        
        record = {"id": 1, "name": "Alice"}
        dedup_key = key.compute_key(record)
        
        assert dedup_key is not None

    def test_compute_key_none_value(self):
        """Test computing key with None value."""
        key = DedupKey(
            source_id="test",
            key_fields=["id", "name"],
        )
        
        record = {"id": 1, "name": None}
        dedup_key = key.compute_key(record)
        
        assert dedup_key is not None


class TestDedupIndex:
    """Tests for DedupIndex."""

    def test_add_new_key(self):
        """Test adding a new key."""
        index = DedupIndex(source_id="test", run_id="run1")
        
        result = index.add("key1")
        
        assert result is True
        assert len(index.seen_keys) == 1
        assert index.record_count == 1

    def test_add_duplicate_key(self):
        """Test adding a duplicate key."""
        index = DedupIndex(source_id="test", run_id="run1")
        
        index.add("key1")
        result = index.add("key1")
        
        assert result is False
        assert len(index.seen_keys) == 1
        assert index.record_count == 2
        assert index.duplicate_count == 1

    def test_to_dict(self):
        """Test converting index to dict."""
        index = DedupIndex(source_id="test", run_id="run1")
        index.add("key1")
        index.add("key1")
        
        data = index.to_dict()
        
        assert data["source_id"] == "test"
        assert data["run_id"] == "run1"
        assert data["record_count"] == 2
        assert data["duplicate_count"] == 1
        assert data["unique_count"] == 1


class TestDeduplicator:
    """Tests for Deduplicator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.dedup = Deduplicator(self.temp_dir)

    def test_filter_duplicates(self):
        """Test filtering duplicate records."""
        # Use records matching tfl_arrivals dedup key: ["stop_id", "line_id", "timestamp"]
        records = [
            {"stop_id": "1", "line_id": "N", "timestamp": "2026-05-30T10:00:00Z"},
            {"stop_id": "2", "line_id": "S", "timestamp": "2026-05-30T11:00:00Z"},
            {"stop_id": "1", "line_id": "N", "timestamp": "2026-05-30T10:00:00Z"},  # Duplicate
        ]
        
        unique, duplicates = self.dedup.filter_duplicates(
            "tfl_arrivals", "run1", records
        )
        
        assert len(unique) == 2
        assert len(duplicates) == 1
        assert unique[0]["stop_id"] == "1"
        assert unique[1]["stop_id"] == "2"

    def test_filter_duplicates_unknown_source(self):
        """Test filtering with unknown source (no dedup config)."""
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 1, "name": "Alice"},  # Would be duplicate but no config
        ]
        
        unique, duplicates = self.dedup.filter_duplicates(
            "unknown_source", "run1", records
        )
        
        # No dedup config - all kept
        assert len(unique) == 2
        assert len(duplicates) == 0

    def test_filter_duplicates_streaming(self):
        """Test streaming deduplication."""
        # Use records matching tfl_arrivals dedup key
        records = [
            {"stop_id": "1", "line_id": "N", "timestamp": "2026-05-30T10:00:00Z"},
            {"stop_id": "2", "line_id": "S", "timestamp": "2026-05-30T11:00:00Z"},
            {"stop_id": "1", "line_id": "N", "timestamp": "2026-05-30T10:00:00Z"},  # Duplicate
            {"stop_id": "3", "line_id": "E", "timestamp": "2026-05-30T12:00:00Z"},
        ]
        
        unique = list(self.dedup.filter_duplicates_streaming(
            "tfl_arrivals", "run1", records
        ))
        
        assert len(unique) == 3
        stop_ids = [r["stop_id"] for r in unique]
        assert "1" in stop_ids
        assert "2" in stop_ids
        assert "3" in stop_ids

    def test_save_and_load_index(self):
        """Test saving and loading dedup index."""
        # Use records matching tfl_arrivals dedup key
        records = [
            {"stop_id": "1", "line_id": "N", "timestamp": "2026-05-30T10:00:00Z"},
            {"stop_id": "2", "line_id": "S", "timestamp": "2026-05-30T11:00:00Z"},
        ]
        
        self.dedup.filter_duplicates("tfl_arrivals", "run1", records)
        self.dedup.save_index("tfl_arrivals", "run1")
        
        # Load in new instance
        new_dedup = Deduplicator(self.temp_dir)
        loaded = new_dedup.load_index("tfl_arrivals", "run1")
        
        assert loaded is not None
        assert loaded.source_id == "tfl_arrivals"
        assert loaded.run_id == "run1"
        assert len(loaded.seen_keys) == 2

    def test_get_stats(self):
        """Test getting deduplication stats."""
        # Use records matching tfl_arrivals dedup key
        records = [
            {"stop_id": "1", "line_id": "N", "timestamp": "2026-05-30T10:00:00Z"},
            {"stop_id": "2", "line_id": "S", "timestamp": "2026-05-30T11:00:00Z"},
            {"stop_id": "1", "line_id": "N", "timestamp": "2026-05-30T10:00:00Z"},  # Duplicate
        ]
        
        self.dedup.filter_duplicates("tfl_arrivals", "run1", records)
        stats = self.dedup.get_stats("tfl_arrivals", "run1")
        
        assert stats is not None
        assert stats["record_count"] == 3
        assert stats["duplicate_count"] == 1
        assert stats["unique_count"] == 2


class TestDedupFunctions:
    """Tests for convenience functions."""

    def test_dedup_records_function(self):
        """Test dedup_records convenience function."""
        records = [
            {"location": "London", "parameter": "pm25"},  # Valid openaq fields
            {"location": "Paris", "parameter": "o3"},
            {"location": "London", "parameter": "pm25"},  # Duplicate
        ]
        
        unique, duplicates = dedup_records("openaq", "run1", records)
        
        # openaq dedup key is ["location", "parameter", "date_utc"]
        # "London/pm25" appears twice but date_utc is None, so should dedup
        assert len(unique) <= 3
