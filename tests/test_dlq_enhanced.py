"""Tests for enhanced DLQ with retry backoff."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from governance.dlq_enhanced import (
    RetryPolicy,
    DLQEntry,
    EnhancedDLQ,
    record_dlq_with_retry,
)


class TestRetryPolicy:
    """Tests for RetryPolicy."""

    def test_get_delay_exponential(self):
        """Test exponential backoff calculation."""
        policy = RetryPolicy(
            backoff_base_seconds=1.0,
            backoff_max_seconds=60.0,
            jitter_seconds=0.0,
        )
        
        # Attempt 1: 1 * 2^0 = 1s
        assert policy.get_delay(1) == 1.0
        
        # Attempt 2: 1 * 2^1 = 2s
        assert policy.get_delay(2) == 2.0
        
        # Attempt 3: 1 * 2^2 = 4s
        assert policy.get_delay(3) == 4.0

    def test_get_delay_max_cap(self):
        """Test delay is capped at max."""
        policy = RetryPolicy(
            backoff_base_seconds=10.0,
            backoff_max_seconds=30.0,
            jitter_seconds=0.0,
        )
        
        # Attempt 4: 10 * 2^3 = 80, but capped at 30
        assert policy.get_delay(4) == 30.0

    def test_get_delay_with_jitter(self):
        """Test delay includes jitter."""
        policy = RetryPolicy(
            backoff_base_seconds=1.0,
            jitter_seconds=0.5,
        )
        
        delays = [policy.get_delay(1) for _ in range(10)]
        
        # All should be around 1.0-1.5
        assert all(1.0 <= d <= 1.5 for d in delays)


class TestDLQEntry:
    """Tests for DLQEntry."""

    def test_to_dict(self):
        """Test converting entry to dict."""
        entry = DLQEntry(
            category="test",
            source="openaq",
            error="Connection timeout",
            payload={"url": "http://example.com"},
            captured_at="2026-05-30T10:00:00Z",
            attempts=2,
        )
        
        data = entry.to_dict()
        
        assert data["category"] == "test"
        assert data["source"] == "openaq"
        assert data["error"] == "Connection timeout"
        assert data["attempts"] == 2
        assert data["status"] == "pending"

    def test_from_dict(self):
        """Test creating entry from dict."""
        data = {
            "category": "test",
            "source": "tfl",
            "error": "API error",
            "payload": {"id": 1},
            "captured_at": "2026-05-30T10:00:00Z",
            "attempts": 3,
            "status": "failed",
        }
        
        entry = DLQEntry.from_dict(data)
        
        assert entry.category == "test"
        assert entry.source == "tfl"
        assert entry.attempts == 3
        assert entry.status == "failed"


class TestEnhancedDLQ:
    """Tests for EnhancedDLQ."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.dlq = EnhancedDLQ(self.temp_dir)

    def test_record(self):
        """Test recording an entry to DLQ."""
        path = self.dlq.record(
            category="test",
            source="openaq",
            error="Connection failed",
            payload={"url": "http://api.openaq.org"},
        )
        
        assert path.exists()

    def test_list_pending(self):
        """Test listing pending entries."""
        self.dlq.record(
            category="test",
            source="openaq",
            error="Error 1",
            payload={"id": 1},
        )
        self.dlq.record(
            category="test",
            source="tfl",
            error="Error 2",
            payload={"id": 2},
        )
        
        pending = self.dlq.list_pending("test")
        
        assert len(pending) == 2
        sources = [e.source for e in pending]
        assert "openaq" in sources
        assert "tfl" in sources

    def test_list_pending_with_category_filter(self):
        """Test listing pending with category filter."""
        self.dlq.record(
            category="api_error",
            source="openaq",
            error="Error",
            payload={},
        )
        self.dlq.record(
            category="parse_error",
            source="tfl",
            error="Error",
            payload={},
        )
        
        pending = self.dlq.list_pending("api_error")
        
        assert len(pending) == 1
        assert pending[0].source == "openaq"

    def test_retry_entry_success(self):
        """Test retrying a successful entry."""
        entry = self.dlq.record(
            category="test",
            source="openaq",
            error="Temporary error",
            payload={"id": 1},
        )
        
        pending = self.dlq.list_pending("test")
        assert len(pending) == 1
        
        def success_handler(e):
            return True
        
        updated = self.dlq.retry_entry(pending[0], success_handler)
        
        assert updated.status == "succeeded"
        assert updated.attempts == 1

    def test_retry_entry_failure_with_backoff(self):
        """Test retry failure schedules next retry."""
        entry = self.dlq.record(
            category="test",
            source="openaq",
            error="Temporary error",
            payload={"id": 1},
        )
        
        pending = self.dlq.list_pending("test")
        
        def failure_handler(e):
            return False
        
        updated = self.dlq.retry_entry(pending[0], failure_handler)
        
        assert updated.status == "pending"
        assert updated.attempts == 1
        assert updated.next_retry is not None

    def test_retry_entry_max_attempts_exceeded(self):
        """Test retry stops after max attempts."""
        policy = RetryPolicy(max_attempts=2)
        
        entry = self.dlq.record(
            category="test",
            source="openaq",
            error="Persistent error",
            payload={"id": 1},
        )
        
        pending = self.dlq.list_pending("test")
        
        def failure_handler(e):
            return False
        
        # First retry
        updated = self.dlq.retry_entry(pending[0], failure_handler, policy)
        assert updated.status == "pending"
        assert updated.attempts == 1
        
        # Second retry (max)
        updated = self.dlq.retry_entry(updated, failure_handler, policy)
        assert updated.status == "failed"
        assert updated.attempts == 2

    def test_get_stats(self):
        """Test getting DLQ statistics."""
        self.dlq.record(
            category="test",
            source="openaq",
            error="Error",
            payload={},
        )
        self.dlq.record(
            category="test",
            source="tfl",
            error="Error",
            payload={},
        )
        
        stats = self.dlq.get_stats("test")
        
        assert stats["total"] == 2
        assert stats["pending"] == 2
        assert "openaq" in stats["by_source"]
        assert "tfl" in stats["by_source"]

    def test_replay_with_backoff(self):
        """Test replaying DLQ with backoff."""
        self.dlq.record(
            category="test",
            source="openaq",
            error="Error",
            payload={"id": 1},
        )
        
        success_count = [0]
        
        def handler(entry):
            success_count[0] += 1
            return True
        
        results = self.dlq.replay_with_backoff(handler, "test")
        
        assert results["total"] == 1
        assert results["succeeded"] == 1


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())

    def test_record_dlq_with_retry(self):
        """Test record_dlq_with_retry function."""
        path = record_dlq_with_retry(
            category="test",
            source="openaq",
            error="Test error",
            payload={"id": 1},
        )
        
        assert path.exists()
