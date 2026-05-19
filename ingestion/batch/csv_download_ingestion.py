from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.batch.common import read_csv_records, write_jsonl


def download_csv(url: str, max_bytes: int = 200_000_000) -> Path:
    """Download a CSV file from a URL to a temporary local file.

    Streams the download to avoid loading large files entirely into memory.
    Returns the path to the downloaded temporary file.
    """
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    suffix = ".csv"
    tmp = tempfile.NamedTemporaryFile(
        delete=False, suffix=suffix, dir=str(PROJECT_ROOT / "runtime" / "downloads")
    )
    downloaded = 0
    for chunk in response.iter_content(chunk_size=65536):
        downloaded += len(chunk)
        if downloaded > max_bytes:
            tmp.close()
            Path(tmp.name).unlink(missing_ok=True)
            raise ValueError(
                f"Download exceeded {max_bytes} bytes limit. "
                f"Consider filtering or sampling the source."
            )
        tmp.write(chunk)
    tmp.close()
    return Path(tmp.name)


def ingest_csv_download(dataset: str, url: str, max_rows: int | None = None) -> Path:
    """Download a CSV from a URL and ingest it into the raw local landing zone.

    This handler is for datasets with source_type=csv_download in datasets.yml.
    """
    downloads_dir = PROJECT_ROOT / "runtime" / "downloads"
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
        csv_path.unlink(missing_ok=True)


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
