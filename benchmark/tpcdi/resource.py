"""TPC-DI Resource Monitoring.

Tracks CPU utilization, memory usage, and I/O throughput during the
benchmark run.  Wraps ``psutil`` when available; degrades gracefully
to ``/proc`` reads on Linux when psutil is not installed.

All values are sampled at a configurable interval and averaged.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ResourceSnapshot:
    timestamp: float = 0.0
    cpu_percent: float = 0.0
    memory_rss_mb: float = 0.0
    memory_vms_mb: float = 0.0
    io_read_mb: float = 0.0
    io_write_mb: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": round(self.timestamp, 3),
            "cpu_percent": round(self.cpu_percent, 2),
            "memory_rss_mb": round(self.memory_rss_mb, 2),
            "memory_vms_mb": round(self.memory_vms_mb, 2),
            "io_read_mb": round(self.io_read_mb, 3),
            "io_write_mb": round(self.io_write_mb, 3),
        }


_HAS_PSUTIL = False
try:
    import psutil as _psutil
    _HAS_PSUTIL = True
except ImportError:
    _psutil = None  # type: ignore[assignment]


def _read_proc_stat() -> dict[str, int]:
    """Read minimal CPU info from ``/proc/self/stat``."""
    try:
        with open("/proc/self/stat", "r") as f:
            fields = f.read().split()
        return {
            "utime": int(fields[13]),
            "stime": int(fields[14]),
        }
    except Exception:
        return {}


def _read_proc_status_rss() -> int:
    """Read RSS from ``/proc/self/status`` (kB)."""
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    return int(parts[1])
    except Exception:
        return 0


def _read_proc_io() -> dict[str, int]:
    """Read read/write bytes from ``/proc/self/io``."""
    try:
        with open(f"/proc/{os.getpid()}/io", "r") as f:
            io_data = {}
            for line in f:
                if line.startswith("rchar:") or line.startswith("wchar:"):
                    key, val = line.split(":")
                    io_data[key.strip()] = int(val.strip())
        return io_data
    except Exception:
        return {}


class ResourceMonitor:
    """Thread-safe resource monitor that samples CPU/mem/I/O at intervals."""

    def __init__(self, interval_seconds: float = 0.5):
        self.interval = interval_seconds
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self.snapshots: list[ResourceSnapshot] = []

        self._proc: Any = None
        if _HAS_PSUTIL:
            self._proc = _psutil.Process(os.getpid())

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self.snapshots.clear()
        self._prev_io = self._read_io_bytes()
        self._prev_stat = _read_proc_stat()
        self._prev_sample_time = time.monotonic()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None

    # ------------------------------------------------------------------
    # Sampling loop
    # ------------------------------------------------------------------
    def _sample_loop(self) -> None:
        while self._running:
            snapshot = self._sample()
            with self._lock:
                self.snapshots.append(snapshot)
            time.sleep(self.interval)

    def _sample(self) -> ResourceSnapshot:
        now = time.time()

        if _HAS_PSUTIL:
            return self._psutil_sample(now)
        return self._proc_sample(now)

    def _psutil_sample(self, now: float) -> ResourceSnapshot:
        p = self._proc
        cpu = p.cpu_percent(interval=0.0)
        mem = p.memory_info()
        io_counters = p.io_counters() if hasattr(p, "io_counters") else None
        return ResourceSnapshot(
            timestamp=now,
            cpu_percent=cpu,
            memory_rss_mb=mem.rss / (1024 * 1024),
            memory_vms_mb=mem.vms / (1024 * 1024),
            io_read_mb=(io_counters.read_bytes / (1024 * 1024)) if io_counters else 0.0,
            io_write_mb=(io_counters.write_bytes / (1024 * 1024)) if io_counters else 0.0,
        )

    def _proc_sample(self, now: float) -> ResourceSnapshot:
        stat = _read_proc_stat()
        rss_kb = _read_proc_status_rss()

        io_data = _read_proc_io()
        current_io = io_data.get("rchar", 0) + io_data.get("wchar", 0)

        elapsed = now - self._prev_sample_time
        cpu_delta = 0.0
        if elapsed > 0 and self._prev_stat:
            prev_ticks = self._prev_stat.get("utime", 0) + self._prev_stat.get("stime", 0)
            curr_ticks = stat.get("utime", 0) + stat.get("stime", 0)
            tick_delta = curr_ticks - prev_ticks
            cpu_delta = (tick_delta / os.sysconf("SC_CLK_TCK")) / elapsed * 100.0
            cpu_delta = max(0.0, min(cpu_delta, 100.0 * os.cpu_count() if os.cpu_count() else 100.0))

        io_delta = 0.0
        if elapsed > 0 and self._prev_io > 0:
            io_delta = (current_io - self._prev_io) / elapsed / (1024 * 1024)

        self._prev_stat = stat
        self._prev_io = current_io
        self._prev_sample_time = now

        return ResourceSnapshot(
            timestamp=now,
            cpu_percent=cpu_delta,
            memory_rss_mb=rss_kb / 1024.0,
            memory_vms_mb=0.0,
            io_read_mb=io_delta,
            io_write_mb=0.0,
        )

    def _read_io_bytes(self) -> int:
        io_data = _read_proc_io()
        return io_data.get("rchar", 0) + io_data.get("wchar", 0)

    # ------------------------------------------------------------------
    # Query interface
    # ------------------------------------------------------------------
    @property
    def _snapshots(self) -> list[ResourceSnapshot]:
        with self._lock:
            return list(self.snapshots)

    def avg_cpu(self) -> float:
        snaps = self._snapshots
        if not snaps:
            return 0.0
        return sum(s.cpu_percent for s in snaps) / len(snaps)

    def peak_memory_mb(self) -> float:
        snaps = self._snapshots
        if not snaps:
            return 0.0
        return max(s.memory_rss_mb for s in snaps)

    def avg_memory_mb(self) -> float:
        snaps = self._snapshots
        if not snaps:
            return 0.0
        return sum(s.memory_rss_mb for s in snaps) / len(snaps)

    def avg_io_mbps(self) -> float:
        snaps = self._snapshots
        if not snaps:
            return 0.0
        return sum(s.io_read_mb for s in snaps) / len(snaps)

    def summary(self) -> dict[str, Any]:
        return {
            "sample_count": len(self._snapshots),
            "interval_seconds": self.interval,
            "avg_cpu_percent": round(self.avg_cpu(), 2),
            "peak_memory_rss_mb": round(self.peak_memory_mb(), 2),
            "avg_memory_rss_mb": round(self.avg_memory_mb(), 2),
            "avg_io_mbps": round(self.avg_io_mbps(), 3),
        }
