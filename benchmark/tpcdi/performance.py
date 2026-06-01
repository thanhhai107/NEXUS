"""TPC-DI Performance Metrics — DIU/hr measurement.

DIU/hr (Daily Ingestion Units per hour) is the sole official TPC-DI benchmark
metric.  It measures the throughput of the *incremental load* (Phase 2).

Rules:
  - Phase 1 (historical load) time is tracked but NOT included in DIU/hr.
  - Phase 2 (incremental load) time is the denominator.
  - DIU/hr = Total DIU for the scale factor ÷ Phase 2 wall-clock hours.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

DIU_BY_SCALE_FACTOR: dict[int, int] = {
    1: 3,
    3: 10,
    10: 100,
    100: 1000,
    1000: 10000,
}


def get_diu(scale_factor: int) -> int:
    """Return the number of Daily Ingestion Units for a given scale factor.

    Falls back to ``scale_factor * 10`` for unlisted scale factors.
    """
    return DIU_BY_SCALE_FACTOR.get(scale_factor, scale_factor * 10)


@dataclass
class TpcdiPerformanceTimer:
    """Context manager / explicit-call timer for TPC-DI phases.

    Typical usage (context manager)::

        timer = TpcdiPerformanceTimer(scale_factor=1)
        with timer.phase1():
            run_historical_load()
        with timer.phase2(days=1):
            run_incremental_load()
        print(timer.summary())

    Typical usage (explicit start/stop)::

        timer = TpcdiPerformanceTimer(scale_factor=1)
        timer.start_phase1()
        run_historical_load()
        timer.stop_phase1()
        timer.start_phase2()
        run_incremental_load()
        timer.stop_phase2()
        print(timer.summary())
    """

    scale_factor: int = 1
    _phase1_start: float = 0.0
    _phase1_end: float | None = None
    _phase2_start: float = 0.0
    _phase2_end: float | None = None

    # ------------------------------------------------------------------
    # Explicit start / stop
    # ------------------------------------------------------------------
    def start_phase1(self) -> None:
        self._phase1_start = time.perf_counter()

    def stop_phase1(self) -> None:
        self._phase1_end = time.perf_counter()

    def start_phase2(self) -> None:
        self._phase2_start = time.perf_counter()

    def stop_phase2(self) -> None:
        self._phase2_end = time.perf_counter()

    # ------------------------------------------------------------------
    # Context managers
    # ------------------------------------------------------------------
    def phase1(self):
        return _PhaseContext(self, "phase1")

    def phase2(self, days: int = 1):
        return _PhaseContext(self, "phase2", days=days)

    # ------------------------------------------------------------------
    # Query interface
    # ------------------------------------------------------------------
    @property
    def phase1_seconds(self) -> float:
        if self._phase1_start and self._phase1_end:
            return self._phase1_end - self._phase1_start
        return 0.0

    @property
    def phase2_seconds(self) -> float:
        if self._phase2_start and self._phase2_end:
            return self._phase2_end - self._phase2_start
        return 0.0

    @property
    def total_seconds(self) -> float:
        return self.phase1_seconds + self.phase2_seconds

    @property
    def diu_per_hour(self) -> float:
        return compute_diu_hr(self.scale_factor, self.phase2_seconds)

    @property
    def rows_per_second(self) -> float | None:
        if hasattr(self, "_total_rows"):
            return self._total_rows / self.phase2_seconds if self.phase2_seconds > 0 else 0.0
        return None

    def set_row_count(self, rows: int) -> None:
        self._total_rows = rows

    def summary(self) -> dict:
        total_diu = get_diu(self.scale_factor)
        return {
            "scale_factor": self.scale_factor,
            "total_diu": total_diu,
            "phase1_seconds": round(self.phase1_seconds, 3),
            "phase2_seconds": round(self.phase2_seconds, 3),
            "total_seconds": round(self.total_seconds, 3),
            "diu_per_hour": round(self.diu_per_hour, 3),
            "rows_per_second": round(self.rows_per_second, 3) if self.rows_per_second is not None else None,
        }


class _PhaseContext:
    def __init__(self, timer: TpcdiPerformanceTimer, phase: str, days: int = 1):
        self._timer = timer
        self._phase = phase
        self._days = days

    def __enter__(self) -> TpcdiPerformanceTimer:
        if self._phase == "phase1":
            self._timer.start_phase1()
        else:
            self._timer.start_phase2()
        return self._timer

    def __exit__(self, *_: object) -> None:
        if self._phase == "phase1":
            self._timer.stop_phase1()
        else:
            self._timer.stop_phase2()


def compute_diu_hr(scale_factor: int, phase2_seconds: float) -> float:
    total_diu = get_diu(scale_factor)
    if phase2_seconds <= 0:
        return 0.0
    hours = phase2_seconds / 3600.0
    return total_diu / hours
