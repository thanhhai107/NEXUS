"""
Compatibility wrapper for the NEXUS Greater London downloader.

The implementation lives in ingestion.downloaders.london_downloader so the
legacy command keeps working:

    python scripts/download_data.py
    python scripts/download_data.py --source openmeteo --mode small_demo
    python scripts/download_data.py --poll --duration-days 7 --interval-minutes 15
"""

from __future__ import annotations

import codecs
import sys

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

if sys.stdout.encoding and sys.stdout.encoding.lower().replace("-", "") != "utf8":
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, errors="replace")
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, errors="replace")

from ingestion.downloaders.london_downloader import main


if __name__ == "__main__":
    raise SystemExit(main())
