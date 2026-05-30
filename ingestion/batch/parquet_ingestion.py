from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.batch.common import write_jsonl
from ingestion.base.core import DownloadContext, SourceRun
from ingestion.base.http import download_file
from ingestion.base.utils import load_config, resolve_output_dir, run_id_now, sanitize_segment


def _read_parquet_records(path: Path) -> list[dict]:
    try:
        import pyarrow.parquet as pq

        table = pq.read_table(path)
        return _pyarrow_table_to_records(table)
    except ImportError:
        pass

    try:
        import pandas as pd

        df = pd.read_parquet(path)
        return _pandas_df_to_records(df)
    except ImportError:
        pass

    raise ImportError(
        "Neither pyarrow nor pandas is installed. "
        "Install one with: pip install pyarrow  or  pip install pandas pyarrow"
    )


def _pyarrow_table_to_records(table) -> list[dict]:
    import pyarrow as pa

    records: list[dict] = []
    for batch in table.to_batches(max_chunksize=10_000):
        for row_idx in range(batch.num_rows):
            record: dict = {}
            for col_idx, field in enumerate(batch.schema):
                value = batch.column(col_idx)[row_idx].as_py()
                record[field.name] = _normalise_value(value)
            records.append(record)
    return records


def _pandas_df_to_records(df) -> list[dict]:
    records = df.to_dict(orient="records")
    return [_normalise_record(record) for record in records]


def _normalise_value(value):
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return [_normalise_value(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _normalise_value(v) for k, v in value.items()}
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _normalise_record(record: dict) -> dict:
    normalised: dict = {}
    for key, value in record.items():
        normalised[str(key)] = _normalise_value(value)
    return normalised


def _flatten_record(record: dict, prefix: str = "") -> dict:
    flat: dict = {}
    for key, value in record.items():
        full_key = f"{prefix}{key}" if prefix else key
        if isinstance(value, dict):
            flat.update(_flatten_record(value, f"{full_key}."))
        elif isinstance(value, list) and value and all(isinstance(v, dict) for v in value):
            for idx, item in enumerate(value):
                flat.update(_flatten_record(item, f"{full_key}.{idx}."))
        else:
            flat[full_key] = value
    return flat


def ingest_parquet(dataset: str, source: Path, max_rows: int | None = None) -> Path:
    records = _read_parquet_records(source)
    if max_rows is not None and len(records) > max_rows:
        print(f"Sampling {max_rows} of {len(records)} records for dataset={dataset}")
        records = records[:max_rows]
    output_path = write_jsonl(dataset=dataset, records=records, source=str(source))
    print(f"Ingested {len(records)} records for dataset={dataset} into {output_path}")
    return output_path


def batch_parquet_source_run(url: str) -> SourceRun:
    config = load_config()
    parsed = urlparse(url)
    source_id = f"batch_parquet_{sanitize_segment(parsed.netloc or parsed.path or 'download')}"
    context = DownloadContext(
        config=config,
        mode_name="batch_parquet_download",
        mode={},
        output_dir=resolve_output_dir(config, None),
        run_id=run_id_now(),
    )
    return SourceRun(source_id, context, "batch_parquet_download")


def download_parquet(url: str, max_bytes: int = 200_000_000) -> Path:
    run = batch_parquet_source_run(url)
    chunk_id = "parquet_download:full_file"
    if not run.should_skip(chunk_id):
        parsed = urlparse(url)
        filename = sanitize_segment(Path(parsed.path).name or "download.parquet")
        if not filename.lower().endswith(".parquet"):
            filename = f"{filename}.parquet"
        try:
            path, _row_count = download_file(
                run,
                url,
                relative_path=f"batch_parquet_download/{filename}",
                max_bytes=max_bytes,
                timeout=180,
            )
            run.mark_complete(chunk_id, {"path": str(path)})
        except Exception as exc:
            run.mark_failed(chunk_id, str(exc))
            run.finish("failed", str(exc))
            raise
    else:
        outputs = run.checkpoint.get("chunk_outputs", {}).get(chunk_id, [])
        path = Path(outputs[0]["path"]) if outputs else next(run.raw_dir.rglob("*.parquet"))

    run.finish("success" if not run.failed_requests else "partial")
    return path


def ingest_parquet_download(dataset: str, url: str, max_rows: int | None = None) -> Path:
    print(f"Downloading Parquet from {url} ...")
    parquet_path = download_parquet(url)

    try:
        records = _read_parquet_records(parquet_path)
        if max_rows is not None and len(records) > max_rows:
            print(f"Sampling {max_rows} of {len(records)} records for dataset={dataset}")
            records = records[:max_rows]

        output_path = write_jsonl(dataset=dataset, records=records, source=url)
        print(f"Ingested {len(records)} records for dataset={dataset} into {output_path}")
        return output_path
    finally:
        pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest a Parquet data source.")
    parser.add_argument("--dataset", required=True, help="Dataset name from domains/*/datasets.yml")
    parser.add_argument(
        "--source", type=Path, default=None,
        help="Path to local Parquet file",
    )
    parser.add_argument("--url", default=None, help="Parquet download URL")
    parser.add_argument(
        "--max-rows", type=int, default=None,
        help="Optional limit on number of rows to ingest",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.url:
        ingest_parquet_download(dataset=args.dataset, url=args.url, max_rows=args.max_rows)
    elif args.source:
        ingest_parquet(dataset=args.dataset, source=args.source, max_rows=args.max_rows)
    else:
        print("Error: Either --source or --url must be provided.", file=sys.stderr)
        sys.exit(1)
