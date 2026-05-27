"""
London Public Transport Journeys Source Adapter.

Downloads London public transport journey data from London Datastore.
"""

from __future__ import annotations

import os

from ingestion.base.core import DownloadContext, SourceRun
from ingestion.base.http import download_file
from ingestion.base.utils import source_options


def download_london_journeys(run: SourceRun, context: DownloadContext) -> None:
    """Download London public transport journeys CSV."""
    opts = source_options(context, "london_journeys")
    url = os.environ.get("LONDON_PUBLIC_TRANSPORT_JOURNEYS_URL") or opts.get("url")
    chunk_id = "london_journeys:full_csv"
    if run.should_skip(chunk_id):
        return
    path, row_count = download_file(
        run,
        url,
        relative_path="snapshot=current/london_journeys.csv",
        max_bytes=int(opts.get("max_bytes", 50_000_000)),
        timeout=180,
    )
    run.mark_complete(chunk_id, {"record_count": row_count, "path": str(path)})
