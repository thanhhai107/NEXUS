"""Tests for worker detection and distributed processing."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest

from common.worker import (
    WorkerInfo,
    get_worker_count,
    get_worker_index,
    get_worker_info,
    hash_partition,
    is_coordinator,
    partition_iterator,
    partition_work,
    ServiceStatus,
    WorkerHealth,
    check_worker_health,
    write_heartbeat,
    read_heartbeats,
)
from common.storage import reset_storage


class TestWorkerDetection:
    """Tests for worker detection."""

    def test_local_worker_info(self):
        """Test worker info for local execution."""
        info = get_worker_info()

        assert isinstance(info, WorkerInfo)
        assert info.worker_id is not None
        assert info.worker_index >= 0
        assert info.total_workers >= 1
        assert info.is_coordinator == (info.worker_index == 0)

    def test_worker_count(self):
        """Test worker count detection."""
        count = get_worker_count()
        assert count >= 1

    def test_worker_index(self):
        """Test worker index."""
        index = get_worker_index()
        assert index >= 0
        assert index < get_worker_count()

    def test_is_coordinator(self):
        """Test coordinator check."""
        is_coord = is_coordinator()
        assert isinstance(is_coord, bool)


class TestDataPartitioning:
    """Tests for data partitioning."""

    def test_partition_work_list(self):
        """Test partitioning a list."""
        items = list(range(10))

        # With 2 partitions, worker 0 gets even, worker 1 gets odd
        worker_0_items = partition_work(items, num_partitions=2, worker_index=0)
        worker_1_items = partition_work(items, num_partitions=2, worker_index=1)

        assert worker_0_items == [0, 2, 4, 6, 8]
        assert worker_1_items == [1, 3, 5, 7, 9]

    def test_partition_work_with_key(self):
        """Test partitioning with a key function."""
        items = [
            {"id": "a", "value": 1},
            {"id": "b", "value": 2},
            {"id": "a", "value": 3},
            {"id": "b", "value": 4},
        ]

        # Not using key function, just index-based
        result = partition_work(items, num_partitions=2, worker_index=0)

        assert len(result) == 2

    def test_partition_iterator(self):
        """Test partitioning an iterator."""
        def gen():
            for i in range(10):
                yield i

        result = list(partition_iterator(gen(), num_partitions=2, worker_index=0))

        assert result == [0, 2, 4, 6, 8]

    def test_hash_partition(self):
        """Test hash partitioning for consistency."""
        # Same key should always go to same partition
        key = "tpcdi_trade_001"

        p1 = hash_partition(key, num_partitions=4)
        p2 = hash_partition(key, num_partitions=4)
        p3 = hash_partition(key, num_partitions=4)

        assert p1 == p2 == p3
        assert 0 <= p1 < 4

    def test_hash_partition_different_keys(self):
        """Test that different keys distribute across partitions."""
        partitions = set()

        for i in range(20):
            key = f"station_{i}"
            partitions.add(hash_partition(key, num_partitions=4))

        # Should have multiple partitions used
        assert len(partitions) > 1


class TestWorkerInfo:
    """Tests for WorkerInfo dataclass."""

    def test_worker_info_creation(self):
        """Test creating WorkerInfo."""
        info = WorkerInfo(
            worker_id="test-1",
            worker_index=0,
            total_workers=2,
            is_coordinator=True,
            is_distributed=True,
            hostname="test-host",
            pid=12345,
            supports_parallel_io=True,
            supports_multiprocess=True,
        )

        assert info.worker_id == "test-1"
        assert info.worker_index == 0
        assert info.total_workers == 2
        assert info.is_coordinator is True
        assert info.is_distributed is True

    def test_worker_info_not_coordinator(self):
        """Test WorkerInfo for non-coordinator."""
        info = WorkerInfo(
            worker_id="test-2",
            worker_index=1,
            total_workers=2,
            is_coordinator=False,
            is_distributed=True,
            hostname="test-host",
            pid=12346,
            supports_parallel_io=True,
            supports_multiprocess=True,
        )

        assert info.is_coordinator is False


class TestServiceStatus:
    """Tests for ServiceStatus dataclass."""

    def test_service_running(self):
        s = ServiceStatus(name="kafka", role="master", running=True, container_id="abc123", uptime_seconds=3600)
        d = s.to_dict()
        assert d["name"] == "kafka"
        assert d["role"] == "master"
        assert d["running"] is True
        assert d["container_id"] == "abc123"
        assert d["uptime_seconds"] == 3600

    def test_service_stopped(self):
        s = ServiceStatus(name="spark-worker", role="worker", running=False, detail="container not found")
        d = s.to_dict()
        assert d["running"] is False
        assert d["detail"] == "container not found"


class TestWorkerHealth:
    """Tests for WorkerHealth dataclass."""

    def test_healthy_worker(self):
        services = [
            ServiceStatus(name="kafka", role="master", running=True, container_id="c1", uptime_seconds=100),
            ServiceStatus(name="spark", role="master", running=True, container_id="c2", uptime_seconds=200),
        ]
        wh = WorkerHealth(
            worker_id="test-worker",
            hostname="test-host",
            role="master",
            is_reachable=True,
            checked_at="2025-01-01T00:00:00Z",
            services=services,
            healthy_count=2,
            total_count=2,
            is_healthy=True,
        )
        d = wh.to_dict()
        assert d["worker_id"] == "test-worker"
        assert d["is_reachable"] is True
        assert d["is_healthy"] is True
        assert d["healthy_count"] == 2
        assert len(d["services"]) == 2

    def test_unhealthy_worker(self):
        services = [
            ServiceStatus(name="kafka", role="master", running=False, detail="container not found"),
        ]
        wh = WorkerHealth(
            worker_id="test-worker",
            hostname="test-host",
            role="master",
            is_reachable=False,
            checked_at="2025-01-01T00:00:00Z",
            services=services,
            healthy_count=0,
            total_count=1,
            is_healthy=False,
        )
        assert wh.is_healthy is False
        assert wh.is_reachable is False


class TestHealthCheck:
    """Tests for worker health check functions."""

    def test_check_local_health(self):
        health = check_worker_health()
        assert isinstance(health, WorkerHealth)
        assert health.worker_id is not None
        assert health.hostname is not None
        assert health.checked_at is not None
        assert health.total_count >= 0

    def test_check_local_health_explicit_role(self):
        health = check_worker_health(role="master")
        assert isinstance(health, WorkerHealth)
        assert health.role == "master"


class TestHeartbeat:
    """Tests for heartbeat write/read."""

    def test_write_and_read_heartbeat(self):
        import common.storage as _st
        os.environ["NEXUS_RUNTIME_MODE"] = "local"
        _st.reset_storage()
        try:
            hb = write_heartbeat(worker_id="test-hb-writer", ttl_seconds=300)
            assert hb["worker_id"] == "test-hb-writer"
            assert "timestamp" in hb
            assert "hostname" in hb

            records = read_heartbeats(max_age_seconds=600)
            assert isinstance(records, list)
            found = [r for r in records if r["worker_id"] == "test-hb-writer"]
            assert len(found) >= 1
            assert found[0]["alive"] is True
        finally:
            os.environ.pop("NEXUS_RUNTIME_MODE", None)
            _st.reset_storage()

    def test_read_heartbeats_stale(self):
        import common.storage as _st
        os.environ["NEXUS_RUNTIME_MODE"] = "local"
        _st.reset_storage()
        try:
            records = read_heartbeats(max_age_seconds=0)
            assert isinstance(records, list)
            for r in records:
                assert r["alive"] is False
        finally:
            os.environ.pop("NEXUS_RUNTIME_MODE", None)
            _st.reset_storage()
