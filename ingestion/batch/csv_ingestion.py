from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.batch.common import read_csv_records, write_jsonl


def ingest_csv(dataset: str, source: Path) -> Path:
    """Ingest a CSV file into the raw local landing zone.

    In a full deployment this output is copied to MinIO under the raw prefix.
    """
    records = read_csv_records(source)
    output_path = write_jsonl(dataset=dataset, records=records, source=str(source))
    print(f"Ingested {len(records)} records for dataset={dataset} into {output_path}")
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest a CSV open-data source.")
    parser.add_argument("--dataset", required=True, help="Dataset name from domains/*/datasets.yml")
    parser.add_argument("--source", required=True, type=Path, help="Path to source CSV file")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ingest_csv(dataset=args.dataset, source=args.source)
