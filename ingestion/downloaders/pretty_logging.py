"""Pretty logging for parallel downloads.

Inspired by Docker pull/logs output with:
- Colored status indicators (on supported terminals)
- Real-time progress updates
- Progress bars with percentage
- Summary table at the end
"""

from __future__ import annotations

import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

# Check if terminal supports ANSI colors
def _supports_ansi() -> bool:
    """Check if terminal supports ANSI escape codes."""
    if os.name == "nt":
        return os.getenv("ANSICON") or os.getenv("WT_SESSION") or os.getenv("TERM_PROGRAM") == "vscode"
    return sys.stdout.isatty()


SUPPORTS_ANSI = _supports_ansi()


# ANSI color codes
class Colors:
    RESET = "\033[0m" if SUPPORTS_ANSI else ""
    BOLD = "\033[1m" if SUPPORTS_ANSI else ""
    DIM = "\033[2m" if SUPPORTS_ANSI else ""

    # Foreground colors
    BLACK = "\033[30m" if SUPPORTS_ANSI else ""
    RED = "\033[31m" if SUPPORTS_ANSI else ""
    GREEN = "\033[32m" if SUPPORTS_ANSI else ""
    YELLOW = "\033[33m" if SUPPORTS_ANSI else ""
    BLUE = "\033[34m" if SUPPORTS_ANSI else ""
    MAGENTA = "\033[35m" if SUPPORTS_ANSI else ""
    CYAN = "\033[36m" if SUPPORTS_ANSI else ""
    WHITE = "\033[37m" if SUPPORTS_ANSI else ""

    # Bright colors
    BRIGHT_RED = "\033[91m" if SUPPORTS_ANSI else ""
    BRIGHT_GREEN = "\033[92m" if SUPPORTS_ANSI else ""
    BRIGHT_YELLOW = "\033[93m" if SUPPORTS_ANSI else ""
    BRIGHT_BLUE = "\033[94m" if SUPPORTS_ANSI else ""
    BRIGHT_MAGENTA = "\033[95m" if SUPPORTS_ANSI else ""
    BRIGHT_CYAN = "\033[96m" if SUPPORTS_ANSI else ""


class Status(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    WARNING = "warning"


@dataclass
class SourceProgress:
    source_key: str
    source_id: str
    status: Status = Status.PENDING
    rows: int = 0
    files: int = 0
    size_mb: float = 0.0
    message: str = ""
    worker_id: int = 0
    start_time: datetime | None = None
    end_time: datetime | None = None
    error: str = ""


class PrettyLogger:
    """Pretty logger with Docker-like output."""

    # Status symbols (ASCII for Windows compatibility)
    SYMBOLS = {
        Status.PENDING: "[...]",
        Status.RUNNING: "[>>>]",
        Status.SUCCESS: "[OK] ",
        Status.FAILED: "[!!]",
        Status.WARNING: "[W!]",
    }

    # Status colors
    STATUS_COLORS = {
        Status.PENDING: Colors.DIM,
        Status.RUNNING: Colors.CYAN,
        Status.SUCCESS: Colors.BRIGHT_GREEN,
        Status.FAILED: Colors.BRIGHT_RED,
        Status.WARNING: Colors.BRIGHT_YELLOW,
    }

    def __init__(self, show_progress_bar: bool = True, width: int = 80):
        self.show_progress_bar = show_progress_bar
        self.width = width
        self.sources: dict[str, SourceProgress] = {}
        self.lock = threading.Lock()
        self._lines_printed = 0
        self._use_ansi = SUPPORTS_ANSI

    def add_source(self, source_key: str, source_id: str, worker_id: int) -> None:
        with self.lock:
            self.sources[source_key] = SourceProgress(
                source_key=source_key,
                source_id=source_id,
                worker_id=worker_id,
            )

    def start_source(self, source_key: str) -> None:
        with self.lock:
            if source_key in self.sources:
                self.sources[source_key].status = Status.RUNNING
                self.sources[source_key].start_time = datetime.now(timezone.utc)
        self._print_all()

    def update_source(
        self,
        source_key: str,
        status: Status | None = None,
        rows: int | None = None,
        files: int | None = None,
        size_mb: float | None = None,
        message: str = "",
    ) -> None:
        with self.lock:
            if source_key in self.sources:
                source = self.sources[source_key]
                if status:
                    source.status = status
                if rows is not None:
                    source.rows = rows
                if files is not None:
                    source.files = files
                if size_mb is not None:
                    source.size_mb = size_mb
                if message:
                    source.message = message
                if status in (Status.SUCCESS, Status.FAILED):
                    source.end_time = datetime.now(timezone.utc)
        self._print_all()

    def finish_source(
        self,
        source_key: str,
        status: Status,
        rows: int = 0,
        files: int = 0,
        size_mb: float = 0.0,
        error: str = "",
    ) -> None:
        with self.lock:
            if source_key in self.sources:
                self.sources[source_key].status = status
                self.sources[source_key].rows = rows
                self.sources[source_key].files = files
                self.sources[source_key].size_mb = size_mb
                self.sources[source_key].end_time = datetime.now(timezone.utc)
                self.sources[source_key].error = error
        self._print_all()

    def _get_elapsed(self, source: SourceProgress) -> str:
        if not source.start_time:
            return ""
        end = source.end_time or datetime.now(timezone.utc)
        elapsed = (end - source.start_time).total_seconds()
        if elapsed < 60:
            return f"{elapsed:.1f}s"
        else:
            mins = int(elapsed // 60)
            secs = int(elapsed % 60)
            return f"{mins}m {secs}s"

    def _format_size(self, size_mb: float) -> str:
        if size_mb >= 1000:
            return f"{size_mb/1024:.2f} GB"
        return f"{size_mb:.2f} MB"

    def _print_source_line(self, source: SourceProgress) -> str:
        symbol = self.SYMBOLS[source.status]
        status_color = self.STATUS_COLORS[source.status]

        # Worker badge
        worker_badge = f"{Colors.DIM}[w{source.worker_id}]{Colors.RESET}" if source.worker_id else ""

        # Source name
        name = f"{Colors.BOLD}{source.source_key}{Colors.RESET}"

        # Status text
        if source.status == Status.RUNNING:
            status_text = f"{Colors.CYAN}Running...{Colors.RESET}"
        elif source.status == Status.SUCCESS:
            elapsed = self._get_elapsed(source)
            status_text = (
                f"{Colors.BRIGHT_GREEN}Completed{Colors.RESET} "
                f"{DIM}|{Colors.RESET} "
                f"{Colors.WHITE}{source.rows:,} rows{Colors.RESET} "
                f"{DIM}|{Colors.RESET} "
                f"{Colors.WHITE}{self._format_size(source.size_mb)}{Colors.RESET} "
                f"{DIM}|{Colors.RESET} "
                f"{Colors.WHITE}{elapsed}{Colors.RESET}"
            )
        elif source.status == Status.FAILED:
            error_text = source.error[:50] + "..." if len(source.error) > 50 else source.error
            status_text = f"{Colors.BRIGHT_RED}Failed: {error_text}{Colors.RESET}"
        elif source.status == Status.PENDING:
            status_text = f"{DIM}Waiting...{Colors.RESET}"
        else:
            status_text = ""

        return f" {symbol} {worker_badge} {name:15s} {status_text}"

    def _print_progress_bar(self) -> str:
        total = len(self.sources)
        if total == 0:
            return ""

        completed = sum(1 for s in self.sources.values() if s.status == Status.SUCCESS)
        failed = sum(1 for s in self.sources.values() if s.status == Status.FAILED)
        running = sum(1 for s in self.sources.values() if s.status == Status.RUNNING)

        # Progress bar
        bar_width = 30
        filled = completed + failed
        progress = filled / total
        bar = "#" * int(progress * bar_width) + "-" * (bar_width - int(progress * bar_width))

        pct = int(progress * 100)

        # Summary line
        line = (
            f"{Colors.BOLD}Progress:{Colors.RESET} "
            f"[{Colors.BRIGHT_GREEN}{bar}{Colors.RESET}] "
            f"{pct}% "
            f"{DIM}|{Colors.RESET} "
            f"{Colors.BRIGHT_GREEN}{completed} done{Colors.RESET} "
            f"{DIM}|{Colors.RESET} "
            f"{Colors.BRIGHT_RED}{failed} failed{Colors.RESET} "
            f"{DIM}|{Colors.RESET} "
            f"{Colors.CYAN}{running} running{Colors.RESET}"
        )

        return line

    def _print_all(self) -> None:
        if not self._use_ansi or not sys.stdout.isatty():
            return

        # Move cursor up and clear previous output
        if self._lines_printed > 0:
            print(f"\033[{self._lines_printed}A", end="")

        lines = []

        # Header
        header = f"{Colors.BOLD}{Colors.BRIGHT_CYAN}{'='*self.width}{Colors.RESET}"
        lines.append(header)

        # Progress bar
        if self.show_progress_bar:
            lines.append(self._print_progress_bar())
            lines.append("")

        # Source lines
        for source in self.sources.values():
            lines.append(self._print_source_line(source))

        # Footer
        lines.append(f"{Colors.BOLD}{Colors.BRIGHT_CYAN}{'='*self.width}{Colors.RESET}")

        # Clear any leftover lines
        output = "\n".join(lines)
        remaining = self._lines_printed - len(lines)
        if remaining > 0:
            output += "\n" + "\n".join([" " * self.width] * remaining)

        print(output, end="", flush=True)
        self._lines_printed = len(lines)

    def print_summary(self, elapsed_seconds: float) -> None:
        # Clear the progress display
        if self._use_ansi and self._lines_printed > 0:
            print(f"\033[{self._lines_printed}A", end="")
            print("\n" * self._lines_printed, end="")

        total = len(self.sources)
        succeeded = [s for s in self.sources.values() if s.status == Status.SUCCESS]
        failed = [s for s in self.sources.values() if s.status == Status.FAILED]

        total_rows = sum(s.rows for s in self.sources.values())
        total_size = sum(s.size_mb for s in self.sources.values())

        # Summary header
        print()
        print(f"{Colors.BOLD}{Colors.BRIGHT_CYAN}{'-'*self.width}{Colors.RESET}")
        print(f"{Colors.BOLD}  NEXUS Download Summary{Colors.RESET}")
        print(f"{Colors.BRIGHT_CYAN}{'-'*self.width}{Colors.RESET}")
        print()

        # Stats
        elapsed_mins = int(elapsed_seconds // 60)
        elapsed_secs = int(elapsed_seconds % 60)
        elapsed_str = f"{elapsed_mins}m {elapsed_secs}s" if elapsed_mins > 0 else f"{elapsed_secs}s"

        stats = [
            ("Total sources", str(total)),
            ("Succeeded", f"{Colors.BRIGHT_GREEN}{len(succeeded)}{Colors.RESET}"),
            ("Failed", f"{Colors.BRIGHT_RED}{len(failed)}{Colors.RESET}"),
            ("Total rows", f"{total_rows:,}"),
            ("Total size", self._format_size(total_size)),
            ("Elapsed time", elapsed_str),
        ]

        for label, value in stats:
            print(f"  {Colors.DIM}{label:15s}{Colors.RESET}  {value}")

        print()

        # Source table
        if succeeded or failed:
            print(f"{Colors.BOLD}  Source Details:{Colors.RESET}")
            print(f"{Colors.DIM}  {'Source':<18s} {'Status':<12s} {'Rows':>12s} {'Size':>12s} {'Time':>10s}{Colors.RESET}")
            print(f"{Colors.BRIGHT_CYAN}  {'-'*62}{Colors.RESET}")

            for source in sorted(self.sources.values(), key=lambda s: s.source_key):
                symbol = self.SYMBOLS[source.status]
                status_color = self.STATUS_COLORS[source.status]
                elapsed = self._get_elapsed(source)

                status_str = f"{status_color}{source.status.value.upper()}{Colors.RESET}"
                rows_str = f"{source.rows:,}" if source.rows else "-"
                size_str = self._format_size(source.size_mb) if source.size_mb > 0 else "-"

                print(
                    f"  {symbol} {source.source_key:<16s} {status_str:<12s} "
                    f"{Colors.WHITE}{rows_str:>12s}{Colors.RESET} "
                    f"{Colors.WHITE}{size_str:>12s}{Colors.RESET} "
                    f"{Colors.DIM}{elapsed:>10s}{Colors.RESET}"
                )

        # Footer
        print()
        print(f"{Colors.BOLD}{Colors.BRIGHT_CYAN}{'='*self.width}{Colors.RESET}")
        print()

        # Reset lines counter
        self._lines_printed = 0


def run_parallel_pretty(
    specs: list[Any],
    context: Any,
    max_workers: int,
) -> list[dict[str, Any]]:
    """Run sources in parallel with pretty logging.

    Args:
        specs: List of source specs
        context: Download context
        max_workers: Max concurrent workers

    Returns:
        List of profiles
    """
    from ingestion.downloaders.london_downloader import run_source

    results: list[dict[str, Any]] = []
    start_time = datetime.now(timezone.utc)

    # Initialize logger
    logger = PrettyLogger(show_progress_bar=True)

    # Assign sources to workers
    worker_sources: dict[int, list] = {}
    for idx, spec in enumerate(specs):
        worker_id = (idx % max_workers) + 1
        if worker_id not in worker_sources:
            worker_sources[worker_id] = []
        worker_sources[worker_id].append(spec)
        logger.add_source(spec.key, spec.source_id, worker_id)

    # Print header
    print()
    print(f" {Colors.BOLD}{Colors.BRIGHT_CYAN}[ NEXUS Parallel Download ]{Colors.RESET}")
    print(f" {Colors.DIM}Starting {len(specs)} sources with {max_workers} workers{Colors.RESET}")
    print()

    def run_with_logging(spec: Any, context: Any, logger: PrettyLogger) -> dict[str, Any]:
        """Run source with progress logging."""
        logger.start_source(spec.key)

        try:
            profile = run_source(spec, context)
            logger.finish_source(
                spec.key,
                Status.SUCCESS if profile.get("status") != "failed" else Status.FAILED,
                rows=profile.get("row_count", 0),
                files=profile.get("file_count", 0),
                size_mb=profile.get("size_mb", 0.0),
            )
            return profile
        except Exception as exc:
            logger.finish_source(
                spec.key,
                Status.FAILED,
                error=str(exc),
            )
            return {
                "source_id": spec.source_id,
                "source_key": spec.key,
                "status": "failed",
                "error": str(exc),
                "row_count": 0,
                "file_count": 0,
                "size_mb": 0.0,
            }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(run_with_logging, spec, context, logger): spec
            for spec in specs
        }

        for future in as_completed(futures):
            spec = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception:
                pass

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()

    # Print summary
    logger.print_summary(elapsed)

    return results
