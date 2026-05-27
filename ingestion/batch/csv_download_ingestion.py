from __future__ import annotations

import argparse
import sys
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from common.config import BRONZE_DIR
from ingestion.batch.common import read_csv_records, write_jsonl
from ingestion.base.core import DownloadContext, SourceRun
from ingestion.base.http import download_file
from ingestion.base.utils import load_config, resolve_output_dir, run_id_now, sanitize_segment


def download_csv(url: str, max_bytes: int = 200_000_000) -> Path:
    """Download a CSV file from a URL to a temporary local file.

    Streams through the shared resilient downloader runtime so CSV downloads
    get bounded retry, request timeout, backoff, request logs and checkpointing.
    """
    run = batch_csv_source_run(url)
    chunk_id = "csv_download:full_file"
    if not run.should_skip(chunk_id):
        parsed = urlparse(url)
        filename = sanitize_segment(Path(parsed.path).name or "download.csv")
        if not filename.lower().endswith(".csv"):
            filename = f"{filename}.csv"
        try:
            path, row_count = download_file(
                run,
                url,
                relative_path=f"batch_csv_download/{filename}",
                max_bytes=max_bytes,
                timeout=180,
            )
            run.mark_complete(chunk_id, {"record_count": row_count, "path": str(path)})
        except Exception as exc:
            run.mark_failed(chunk_id, str(exc))
            run.finish("failed", str(exc))
            raise
    else:
        outputs = run.checkpoint.get("chunk_outputs", {}).get(chunk_id, [])
        path = Path(outputs[0]["path"]) if outputs else next(run.raw_dir.rglob("*.csv"))

    run.finish("success" if not run.failed_requests else "partial")
    return path


def batch_csv_source_run(url: str) -> SourceRun:
    config = load_config()
    parsed = urlparse(url)
    source_id = f"batch_csv_{sanitize_segment(parsed.netloc or parsed.path or 'download')}"
    context = DownloadContext(
        config=config,
        mode_name="batch_csv_download",
        mode={},
        output_dir=resolve_output_dir(config, None),
        run_id=run_id_now(),
    )
    return SourceRun(source_id, context, "batch_csv_download")


def ingest_csv_download(dataset: str, url: str, max_rows: int | None = None) -> Path:
    """Download a CSV from a URL and ingest it into the raw local landing zone.

    This handler is for datasets with source_type=csv_download in datasets.yml.
    """
    downloads_dir = DATASETS_DIR
    downloads_dir.mkdir(parents=True, exist_ok=True)

    print(f"Downloading CSV from {url} ...")
    csv_path = download_csv(url)

    try:
        records = read_csv_records(csv_path)
        if max_rows and len(records) > max_rows:
            print(f"Sampling {max_rows} of {len(records)} records for dataset={dataset}")
            records = records[:max_rows]

        output_path = write_jsonl(dataset=dataset, records=records, source=url)
        print(f"Ingested {len(records)} records for dataset={dataset} into {output_path}")
        return output_path
    finally:
        # Keep the resilient download artifact under runtime/downloads for
        # checkpoint/resume evidence; runtime outputs are ignored by Git.
        pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and ingest a CSV open-data source from a URL."
    )
    parser.add_argument(
        "--dataset", required=True,
        help="Dataset name from domains/*/datasets.yml",
    )
    parser.add_argument(
        "--url", required=True,
        help="Direct CSV download URL",
    )
    parser.add_argument(
        "--max-rows", type=int, default=None,
        help="Optional limit on number of rows to ingest",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ingest_csv_download(dataset=args.dataset, url=args.url, max_rows=args.max_rows)
