"""
NaPTAN (National Public Transport Access Nodes) Source Adapter.

Downloads UK public transport stop data from NaPTAN API.
"""

from __future__ import annotations

import os

from ingestion.base.core import DownloadContext, SourceRun
from ingestion.base.http import download_file
from ingestion.base.utils import source_options


def download_naptan(run: SourceRun, context: DownloadContext) -> None:
    """Download NaPTAN access nodes CSV."""
    opts = source_options(context, "naptan")
    url = os.environ.get("NAPTAN_ACCESS_NODES_CSV_URL") or opts.get("url")
    params = dict(opts.get("params", {}))
    chunk_id = "naptan:atco=490"
    if run.should_skip(chunk_id):
        return
    path, row_count = download_file(
        run,
        url,
        params=params,
        relative_path="snapshot=current/atco_area=490/naptan_stops.csv",
        max_bytes=int(opts.get("max_bytes", 200_000_000)),
        timeout=180,
    )
    run.mark_complete(chunk_id, {"record_count": row_count, "path": str(path)})
