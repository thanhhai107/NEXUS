"""Simple CLI to download data from all sources for local testing.

Usage:
    python scripts/test_download.py --source openmeteo --mode small_demo
    python scripts/test_download.py --source londonair --mode small_demo
    python scripts/test_download.py --list

This creates a data/local_test/ directory with downloaded files.
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.downloaders.london_downloader import main


if __name__ == "__main__":
    sys.exit(main())
