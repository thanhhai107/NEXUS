"""Extract sample data from bronze lake into assets/samples.

Extracts a small sample (first 50 records) from each source in runtime/lake/bronze/,
preserving the original format (CSV stays CSV, JSON stays JSON, JSONL stays JSONL).

Usage:
    python scripts/extract_bronze_samples.py

Environment:
    Uses is_vm_mode() to determine data path:
    - Local: PROJECT_ROOT / "runtime" / "lake" / "bronze"
    - VM: /data/lake/bronze
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from common.config import is_vm_mode

# Choose bronze directory based on environment
if is_vm_mode():
    BRONZE_DIR = Path("/data/lake/bronze")
else:
    BRONZE_DIR = PROJECT_ROOT / "runtime" / "lake" / "bronze"

SAMPLES_DIR = PROJECT_ROOT / "assets" / "samples"
MAX_RECORDS_PER_SOURCE = 50


def main() -> int:
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    # Clear existing samples
    for f in SAMPLES_DIR.glob("*"):
        f.unlink(missing_ok=True)

    sources = [d for d in BRONZE_DIR.iterdir() if d.is_dir()]
    if not sources:
        print(f"No sources found in {BRONZE_DIR}")
        return 1

    extracted = 0
    for source_dir in sorted(sources):
        source_name = source_dir.name

        # Find first data file
        data_file = _find_data_file(source_dir)
        if not data_file:
            print(f"  [SKIP] {source_name}: no data file found")
            continue

        # Determine output extension based on original format
        extension = _get_extension(data_file)
        sample_path = SAMPLES_DIR / f"{source_name}{extension}"

        format_type = data_file.suffix.lower()
        if format_type == ".jsonl":
            success = _extract_jsonl(data_file, sample_path)
        elif format_type == ".json":
            success = _extract_json(data_file, sample_path)
        elif format_type == ".csv":
            success = _extract_csv(data_file, sample_path)
        elif format_type == ".xml":
            success = _extract_xml(data_file, sample_path)
        else:
            print(f"  [SKIP] {source_name}: unsupported format {format_type}")
            continue

        if success:
            size = sample_path.stat().st_size
            print(f"  [OK] {source_name}: {format_type} ({size:,} bytes)")
            extracted += 1
        else:
            print(f"  [FAIL] {source_name}: extraction failed")

    print(f"\nExtracted {extracted}/{len(sources)} samples to {SAMPLES_DIR}")
    return 0


def _find_data_file(source_dir: Path) -> Path | None:
    """Find the first data file in a bronze source directory."""
    # Look in raw subdirectory first
    for pattern in ["raw/**/*.jsonl", "raw/**/*.json", "raw/**/*.csv", "raw/**/*.xml"]:
        files = list(source_dir.glob(pattern))
        if files:
            return files[0]

    # Fallback: any data file
    for pattern in ["**/*.jsonl", "**/*.json", "**/*.csv", "**/*.xml"]:
        files = list(source_dir.glob(pattern))
        # Exclude metadata files
        files = [f for f in files if "metadata" not in str(f) and "manifest" not in str(f)]
        if files:
            return files[0]

    return None


def _extract_jsonl(source: Path, dest: Path) -> bool:
    """Extract first N records from JSONL file, preserving format."""
    try:
        with source.open("r", encoding="utf-8") as f_in, \
             dest.open("w", encoding="utf-8", newline="\n") as f_out:
            count = 0
            for line in f_in:
                if not line.strip():
                    continue
                f_out.write(line)
                count += 1
                if count >= MAX_RECORDS_PER_SOURCE:
                    break
        return True
    except Exception as e:
        print(f"    Error: {e}")
        return False


def _extract_json(source: Path, dest: Path) -> bool:
    """Extract first N records from JSON file, preserving format."""
    try:
        with source.open("r", encoding="utf-8") as f:
            data = json.load(f)

        # Handle both array and object with records
        if isinstance(data, list):
            output = data[:MAX_RECORDS_PER_SOURCE]
        elif isinstance(data, dict):
            # Try to find records array
            for key in ["records", "data", "items", "results", "features"]:
                if key in data and isinstance(data[key], list):
                    data[key] = data[key][:MAX_RECORDS_PER_SOURCE]
                    break
            output = data
        else:
            output = data

        with dest.open("w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        return True
    except Exception as e:
        print(f"    Error: {e}")
        return False


def _extract_csv(source: Path, dest: Path) -> bool:
    """Extract first N rows from CSV file, preserving format."""
    try:
        # Try different encodings
        content = None
        for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
            try:
                with source.open("r", encoding=encoding) as f:
                    content = f.read()
                break
            except UnicodeDecodeError:
                continue

        if content is None:
            return False

        import io
        lines = content.splitlines()

        # Find header (skip metadata lines)
        header_idx = 0
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and not stripped.startswith("//"):
                # Check if it looks like CSV header
                if "," in line or ";" in line or "\t" in line:
                    header_idx = i
                    break

        header_line = lines[header_idx]
        delimiter = _detect_delimiter(header_line)

        reader = csv.DictReader(
            io.StringIO("\n".join(lines[header_idx:])),
            delimiter=delimiter
        )

        rows = []
        for i, row in enumerate(reader):
            if i >= MAX_RECORDS_PER_SOURCE:
                break
            rows.append(row)

        if not rows:
            return False

        # Write output with same delimiter
        with dest.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=rows[0].keys(),
                delimiter=delimiter,
                lineterminator="\n",
                extrasaction="ignore"
            )
            writer.writeheader()
            writer.writerows(rows)

        return True
    except Exception as e:
        print(f"    Error: {e}")
        return False


def _extract_xml(source: Path, dest: Path) -> bool:
    """Extract first N records from XML file, preserving format."""
    try:
        import xml.etree.ElementTree as ET

        tree = ET.parse(source)
        root = tree.getroot()

        # Try to find repeated elements
        children = list(root)
        if len(children) > 1:
            # Multiple records at root level
            # Keep only first N
            for child in children[MAX_RECORDS_PER_SOURCE:]:
                root.remove(child)
        elif len(children) == 1:
            grandchildren = list(children[0])
            record_tags = {"record", "item", "entry", "row", "data", "event", "object"}
            if children[0].tag.lower() in record_tags and len(grandchildren) > 1:
                for gc in grandchildren[MAX_RECORDS_PER_SOURCE:]:
                    children[0].remove(gc)

        tree.write(dest, encoding="utf-8", xml_declaration=True)
        return True
    except Exception as e:
        print(f"    Error: {e}")
        return False


def _detect_delimiter(header_line: str) -> str:
    """Detect CSV delimiter."""
    delimiters = [",", ";", "\t", "|"]
    counts = {d: header_line.count(d) for d in delimiters}
    return max(counts, key=counts.get)


def _get_extension(source_file: Path) -> str:
    """Get appropriate file extension based on source file."""
    suffix = source_file.suffix.lower()
    if suffix == ".jsonl":
        return ".jsonl"
    elif suffix == ".json":
        # Check if it's actually JSON array or object
        try:
            with source_file.open("r", encoding="utf-8") as f:
                first_char = f.read(1).strip()
            if first_char == "[":
                return ".json"
        except:
            pass
        return ".json"
    elif suffix == ".csv":
        return ".csv"
    elif suffix == ".xml":
        return ".xml"
    return ".txt"


if __name__ == "__main__":
    raise SystemExit(main())
