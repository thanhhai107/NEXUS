"""Source Download Hook for Airflow.

Provides a reusable hook for downloading data from various sources.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from airflow.hooks.base import BaseHook

from orchestration.airflow.config import (
    get_source_polling_config,
    get_source_retry_config,
    get_source_timeout,
)
from orchestration.airflow.pools import get_pool_name


class SourceDownloadHook(BaseHook):
    """Hook for downloading data from sources.
    
    This hook provides a clean interface for source downloads,
    pulling configuration from centralized config files.
    """

    def __init__(self, source: str | None = None, mode: str = "small_demo"):
        """Initialize the hook.
        
        Args:
            source: Source identifier (e.g., 'openaq', 'tfl_arrivals')
            mode: Download mode (e.g., 'small_demo', 'full_demo')
        """
        super().__init__()
        self.source = source
        self.mode = mode
        self.project_root = Path(__file__).resolve().parents[3]
        self.nexus_repo_path = os.getenv("NEXUS_REPO_PATH", str(self.project_root))

    def get_download_command(
        self,
        run_id: str | None = None,
        backfill: bool = False,
    ) -> str:
        """Build the download command for this source.
        
        Args:
            run_id: Run ID (defaults to Airflow ts_nodash)
            backfill: Whether to run in backfill mode
        
        Returns:
            Bash command string to execute
        """
        cmd_parts = [
            f"cd {self.nexus_repo_path}",
            "&&",
            "python ingestion/downloaders/london_downloader.py",
            f"--source {self.source}",
            f"--mode {self.mode}",
        ]
        
        if run_id:
            cmd_parts.append(f"--run-id {run_id}")
        else:
            cmd_parts.append("--run-id {{ ts_nodash }}")
        
        if backfill:
            cmd_parts.append("--backfill")
        
        return " ".join(cmd_parts)

    def get_pool(self) -> str:
        """Get the Airflow pool for this source."""
        if self.source:
            return get_pool_name(self.source)
        return "default_pool"

    def get_timeout(self) -> int:
        """Get the timeout in minutes for this source."""
        if self.source:
            return get_source_timeout(self.source)
        return 10

    def get_retry_config(self) -> dict[str, Any]:
        """Get retry configuration for this source."""
        if self.source:
            return get_source_retry_config(self.source)
        return {
            "max_attempts": 3,
            "backoff_base_seconds": 1.0,
            "backoff_max_seconds": 60.0,
        }

    def download(
        self,
        run_id: str | None = None,
        backfill: bool = False,
    ) -> dict[str, Any]:
        """Execute the download for this source.
        
        Args:
            run_id: Run ID
            backfill: Whether to run in backfill mode
        
        Returns:
            Dict with execution results
        """
        import subprocess
        
        cmd = self.get_download_command(run_id, backfill)
        
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=self.get_timeout() * 60,
            )
            
            return {
                "success": result.returncode == 0,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "error": f"Download timed out after {self.get_timeout()} minutes",
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
            }


def get_source_hook(source: str, mode: str = "small_demo") -> SourceDownloadHook:
    """Factory function to create a source download hook.
    
    Args:
        source: Source identifier
        mode: Download mode
    
    Returns:
        Configured SourceDownloadHook
    """
    return SourceDownloadHook(source=source, mode=mode)
