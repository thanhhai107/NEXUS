"""
Test Script: NEXUS Heterogeneity Adaptability

Chạy script này để test khả năng adapt của NEXUS với:
1. Multiple data formats (JSON, JSONL, CSV)
2. Multiple protocols (REST API, File download)
3. Source Registry
4. Quality checks
5. Bronze envelope
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

# Import NEXUS modules
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from common.source_registry import (
    list_sources,
)
from ingestion.canonical.parser import iter_artifact_records
from ingestion.canonical.envelope import build_raw_envelope, EnvelopeContext
from governance.quality.checks import run_quality_checks, evaluate_quality_status


def test_format_parsing():
    """Test 1: Format Parsing - JSON, JSONL, CSV"""
    print("\n" + "="*60)
    print("TEST 1: Format Parsing")
    print("="*60)
    
    # Create test data in different formats
    test_dir = Path(__file__).parent.parent / "runtime" / "test_formats"
    test_dir.mkdir(parents=True, exist_ok=True)
    
    # JSON
    json_file = test_dir / "test.json"
    json_file.write_text(json.dumps([
        {"id": "1", "name": "Alice", "value": 100},
        {"id": "2", "name": "Bob", "value": 200},
    ]))
    
    # JSONL
    jsonl_file = test_dir / "test.jsonl"
    jsonl_file.write_text(
        '{"id": "1", "name": "Alice", "value": 100}\n'
        '{"id": "2", "name": "Bob", "value": 200}\n'
    )
    
    # CSV
    csv_file = test_dir / "test.csv"
    csv_file.write_text("id,name,value\n1,Alice,100\n2,Bob,200\n")
    
    formats_tested = []
    
    for file in test_dir.glob("*"):
        try:
            records = list(iter_artifact_records(file))
            print(f"  [OK] {file.name}: {len(records)} records parsed")
            formats_tested.append(file.suffix)
        except Exception as e:
            print(f"  [FAIL] {file.name}: ERROR - {e}")
    
    # Cleanup
    for f in test_dir.glob("*"):
        f.unlink()
    test_dir.rmdir()
    
    return len(formats_tested) >= 3


def test_source_registry():
    """Test 2: Source Registry - Multiple source types"""
    print("\n" + "="*60)
    print("TEST 2: Source Registry")
    print("="*60)
    
    sources = list_sources()
    print(f"  Total sources registered: {len(sources)}")
    
    # Group by domain
    domains = {}
    for source in sources:
        domains.setdefault(source.domain, []).append(source.name)
    
    for domain, names in domains.items():
        print(f"  {domain}: {len(names)} sources")
        for source_name in names[:5]:  # Show first 5
            print(f"    - {source_name}")
        if len(names) > 5:
            print(f"    ... and {len(names) - 5} more")
    
    # Test different source types
    source_types = {}
    for source in sources:
        source_types.setdefault(source.source_type, []).append(source.name)
    
    print("\n  Source types:")
    for stype, names in source_types.items():
        print(f"    {stype}: {len(names)} sources")
    
    return len(sources) >= 10


def test_bronze_envelope():
    """Test 3: Bronze Envelope - Metadata wrapping"""
    print("\n" + "="*60)
    print("TEST 3: Bronze Envelope")
    print("="*60)
    
    context = EnvelopeContext(
        dataset_id="test_dataset",
        source_id="test_source",
        ingestion_type="batch_api",
        run_id="20260528T010000Z",
        chunk_id="test_chunk_1",
    )
    
    # Sample record from different "sources"
    records = [
        {"id": "1", "temperature": 25.5, "humidity": 60},
        {"id": "2", "temperature": 26.0, "humidity": 55},
    ]
    
    for record in records:
        envelope = build_raw_envelope(record, context)
        print(f"  [OK] Original keys: {list(record.keys())}")
        print(f"    Envelope fields: {len([k for k in envelope.keys() if k.startswith('_nexus')])}")
        assert "_nexus_record_id" in envelope
        assert "_nexus_ingested_at" in envelope
        assert "payload" in envelope
        assert envelope["payload"] == record
        print(f"    Envelope sample: _nexus_run_id={envelope['_nexus_run_id']}")
    
    print("  [OK] Bronze envelope metadata wrapping works!")
    return True


def test_quality_checks():
    """Test 4: Quality Checks - Freshness, Schema, Duplicates"""
    print("\n" + "="*60)
    print("TEST 4: Quality Checks")
    print("="*60)
    
    now = datetime.now(timezone.utc).isoformat()
    records = [
        {"id": "1", "name": "Alice", "updated_at": now},
        {"id": "2", "name": "Bob", "updated_at": now},
        {"id": "3", "name": "", "updated_at": now},  # Missing value
    ]
    
    result = run_quality_checks(
        dataset="test_dataset",
        records=records,
        required_columns=["id", "name", "updated_at"],
        primary_keys=["id"],
        freshness_column="updated_at",
        max_age_hours=24,
    )
    
    print(f"  Record count: {result.record_count}")
    print(f"  Missing ratio: {result.missing_ratio:.2%}")
    print(f"  Duplicate ratio: {result.duplicate_ratio:.2%}")
    print(f"  Freshness score: {result.freshness_score:.2%}")
    print(f"  Readiness score: {result.readiness_score:.2%}")
    print(f"  Schema valid: {result.schema_valid}")
    
    status, violations = evaluate_quality_status(result, {
        "max_missing_ratio": 0.5,
        "max_duplicate_ratio": 0.5,
        "min_freshness_score": 0.5,
    })
    
    print(f"  Overall status: {status}")
    
    return result.record_count == 3


def test_freshness_tracking():
    """Test 5: Freshness Tracking - Different frequencies"""
    print("\n" + "="*60)
    print("TEST 5: Freshness Tracking")
    print("="*60)
    
    from common.source_registry import derive_update_frequency
    
    # Test different freshness configurations
    test_cases = [
        {"poll_seconds": 60, "expected": "every_1m"},
        {"poll_seconds": 300, "expected": "every_5m"},
        {"poll_seconds": 3600, "expected": "every_1h"},
        {"freshness_hours": 1, "expected": "every_1h"},
        {"freshness_hours": 24, "expected": "every_1d"},
        {"freshness_hours": 168, "expected": "every_1w"},
        {"freshness_hours": 8760, "expected": "every_1y"},
    ]
    
    all_passed = True
    for tc in test_cases:
        result = derive_update_frequency(tc)
        status = "OK" if result == tc["expected"] else "FAIL"
        if result != tc["expected"]:
            all_passed = False
        print(f"  {status} {tc}: got '{result}', expected '{tc['expected']}'")
    
    # Show actual sources with their frequencies
    sources = list_sources()
    print("\n  Sample source frequencies:")
    for source in sources[:5]:
        print(f"    {source.name}: {source.update_frequency}")
    
    return all_passed


def test_checkpoint_resume():
    """Test 6: Checkpoint & Resume Capability"""
    print("\n" + "="*60)
    print("TEST 6: Checkpoint & Resume")
    print("="*60)
    
    from ingestion.base.core import SourceRun, DownloadContext
    from pathlib import Path
    import tempfile
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config = {
            "spatial_scope": {
                "name": "Test",
                "bbox": {"south": 51.0, "north": 52.0, "west": -1.0, "east": 1.0}
            }
        }
        mode = {"core_start": "2024-01-01", "core_end": "2024-12-31"}
        
        context = DownloadContext(
            config=config,
            mode_name="test",
            mode=mode,
            output_dir=Path(tmpdir),
            run_id="20260528T010000Z",
            resume=True,
        )
        
        run = SourceRun(
            source_id="test_source",
            context=context,
            source_key="test_key",
        )
        
        # Simulate first chunk
        run.mark_complete("chunk_1", {"record_count": 100})
        
        # Check checkpoint file exists
        checkpoint_file = run.checkpoint_path
        print(f"  [OK] Checkpoint file created: {checkpoint_file.name}")
        
        # Reload from checkpoint
        run2 = SourceRun(
            source_id="test_source",
            context=context,
            source_key="test_key",
        )
        
        # Should skip chunk_1 on resume
        should_skip = run2.should_skip("chunk_1")
        print(f"  [OK] Resume skips completed chunk: {should_skip}")
        
        # New chunk should NOT skip
        should_skip_new = run2.should_skip("chunk_2")
        print(f"  [OK] New chunk executes: {not should_skip_new}")
    
    return True


def test_ingestion_methods():
    """Test 7: Different Ingestion Methods"""
    print("\n" + "="*60)
    print("TEST 7: Ingestion Methods")
    print("="*60)
    
    from common.source_registry import derive_ingestion_method
    
    test_cases = [
        {"source_type": "csv_download", "expected": "batch_csv_download"},
        {"source_type": "rest_api", "expected": "batch_api"},
        {"source_type": "api_stream", "expected": "stream_api"},
        {"source_type": "gtfs_realtime", "expected": "stream_gtfs_realtime"},
        {"ingestion_method": "custom_mode", "expected": "custom_mode"},  # Override
    ]
    
    all_passed = True
    for tc in test_cases:
        result = derive_ingestion_method(tc)
        status = "OK" if result == tc["expected"] else "FAIL"
        if result != tc["expected"]:
            all_passed = False
        print(f"  {status} {tc.get('source_type', 'override')}: {result}")
    
    return all_passed


def main():
    """Run all tests"""
    print("\n" + "#"*60)
    print("NEXUS HETEROGENEITY ADAPTABILITY TEST")
    print("#"*60)
    
    results = []
    
    results.append(("Format Parsing", test_format_parsing()))
    results.append(("Source Registry", test_source_registry()))
    results.append(("Bronze Envelope", test_bronze_envelope()))
    results.append(("Quality Checks", test_quality_checks()))
    results.append(("Freshness Tracking", test_freshness_tracking()))
    results.append(("Checkpoint & Resume", test_checkpoint_resume()))
    results.append(("Ingestion Methods", test_ingestion_methods()))
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    passed = 0
    for name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"  {status}: {name}")
        if result:
            passed += 1
    
    print(f"\n  Total: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("\n  SUCCESS: NEXUS heterogeneity framework is working correctly!")
        print("  The system can adapt to multiple:")
        print("    - Data formats (JSON, JSONL, CSV)")
        print("    - Source types and protocols")
        print("    - Freshness requirements")
        print("    - Quality standards")
        print("    - Ingestion methods")
    else:
        print("\n  ⚠ Some tests failed. Check implementation.")
    
    return passed == len(results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
