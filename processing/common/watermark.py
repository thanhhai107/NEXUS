"""Watermark tracking for backfill detection.

Tracks the last processed timestamp for each source by querying Iceberg tables.
Used by backfill DAG to detect gaps and determine what data needs to be reprocessed.

Usage:
    from processing.common.watermark import WatermarkTracker

    tracker = WatermarkTracker()
    last_processed = tracker.get_last_processed_watermark("tfl_arrivals")
    tracker.save_watermark("tfl_arrivals", datetime.now())
"""

from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# Watermark storage path (when Iceberg is not available)
WATERMARK_DIR = Path.home() / ".nexus" / "watermarks"
WATERMARK_FILE = WATERMARK_DIR / "watermarks.json"


class WatermarkStore(ABC):
    """Abstract base for watermark storage backends."""

    @abstractmethod
    def get(self, source: str, event_time_col: str = "_nexus_event_time") -> datetime | None:
        """Get last processed watermark for a source."""
        pass

    @abstractmethod
    def save(self, source: str, watermark: datetime, event_time_col: str = "_nexus_event_time") -> None:
        """Save watermark for a source."""
        pass


class FileWatermarkStore(WatermarkStore):
    """File-based watermark storage (fallback when Iceberg not available)."""

    def __init__(self, watermark_file: Path = WATERMARK_FILE):
        self.watermark_file = watermark_file
        self._ensure_file()

    def _ensure_file(self) -> None:
        """Ensure watermark file exists."""
        self.watermark_file.parent.mkdir(parents=True, exist_ok=True)
        if not self.watermark_file.exists():
            self.watermark_file.write_text("{}", encoding="utf-8")

    def _load(self) -> dict[str, datetime]:
        """Load watermarks from file."""
        try:
            data = json.loads(self.watermark_file.read_text(encoding="utf-8"))
            return {k: datetime.fromisoformat(v) for k, v in data.items()}
        except (json.JSONDecodeError, ValueError):
            return {}

    def _save_all(self, watermarks: dict[str, datetime]) -> None:
        """Save all watermarks to file."""
        data = {k: v.isoformat() for k, v in watermarks.items()}
        self.watermark_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def get(self, source: str, event_time_col: str = "_nexus_event_time") -> datetime | None:
        """Get last processed watermark for a source."""
        watermarks = self._load()
        key = f"{source}:{event_time_col}"
        return watermarks.get(key)

    def save(self, source: str, watermark: datetime, event_time_col: str = "_nexus_event_time") -> None:
        """Save watermark for a source."""
        watermarks = self._load()
        key = f"{source}:{event_time_col}"
        watermarks[key] = watermark
        self._save_all(watermarks)


class IcebergWatermarkStore(WatermarkStore):
    """Iceberg-based watermark storage (queries Iceberg tables directly)."""

    def __init__(self, catalog_name: str = " iceberg", database: str = "silver"):
        self.catalog_name = catalog_name
        self.database = database

    def get(self, source: str, event_time_col: str = "_nexus_event_time") -> datetime | None:
        """Query Iceberg table for last processed watermark.

        Args:
            source: Source key (e.g., 'tfl_arrivals')
            event_time_col: Column name containing event time

        Returns:
            Last processed timestamp, or None if no data exists
        """
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark is None:
                return None

            table_name = f"{self.catalog_name}.{self.database}.{source}"

            query = f"""
                SELECT MAX({event_time_col}) as last_event_time
                FROM {table_name}
                WHERE _nexus_source_key = '{source}'
            """

            df = spark.sql(query)
            result = df.collect()

            if result and result[0]["last_event_time"]:
                return result[0]["last_event_time"]
            return None

        except Exception as e:
            print(f"Error querying Iceberg for {source}: {e}")
            return None

    def save(self, source: str, watermark: datetime, event_time_col: str = "_nexus_event_time") -> None:
        """Save watermark to a metadata table in Iceberg.

        This writes to a dedicated watermark tracking table rather than modifying
        the source table itself.
        """
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType

            spark = SparkSession.getActiveSession()
            if spark is None:
                return

            watermark_table = f"{self.catalog_name}.{self.database}._watermarks"

            # Create watermark record
            record = [(source, event_time_col, watermark.isoformat())]
            schema = StructType([
                StructField("source", StringType(), False),
                StructField("event_time_col", StringType(), False),
                StructField("watermark", TimestampType(), False),
                StructField("updated_at", TimestampType(), False),
            ])

            df = spark.createDataFrame(record, schema)
            df.show()

            # Merge into watermark table (upsert)
            merge_query = f"""
                MERGE INTO {watermark_table} AS target
                USING (SELECT '{source}' as source, '{event_time_col}' as event_time_col) AS source_key
                ON target.source = source_key.source AND target.event_time_col = source_key.event_time_col
                WHEN MATCHED THEN UPDATE SET watermark = target.watermark, updated_at = current_timestamp()
                WHEN NOT MATCHED THEN INSERT (source, event_time_col, watermark, updated_at)
                VALUES (source_key.source, source_key.event_time_col, timestamp '{watermark.isoformat()}', current_timestamp())
            """
            spark.sql(merge_query)

        except Exception as e:
            print(f"Error saving watermark to Iceberg for {source}: {e}")


class WatermarkTracker:
    """Main interface for watermark tracking.

    Uses IcebergWatermarkStore when available, falls back to FileWatermarkStore.
    """

    def __init__(
        self,
        use_iceberg: bool = True,
        catalog_name: str = "iceberg",
        database: str = "silver",
    ):
        self.use_iceberg = use_iceberg

        if use_iceberg and self._iceberg_available():
            self.store: WatermarkStore = IcebergWatermarkStore(catalog_name, database)
        else:
            self.store = FileWatermarkStore()

    def _iceberg_available(self) -> bool:
        """Check if Iceberg is available."""
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            return spark is not None
        except ImportError:
            return False

    def get_last_processed_watermark(
        self,
        source: str,
        event_time_col: str = "_nexus_event_time",
    ) -> datetime | None:
        """Get last processed watermark for a source.

        Args:
            source: Source key
            event_time_col: Column containing event time

        Returns:
            Last processed timestamp, or None if never processed
        """
        return self.store.get(source, event_time_col)

    def save_watermark(
        self,
        source: str,
        watermark: datetime,
        event_time_col: str = "_nexus_event_time",
    ) -> None:
        """Save watermark for a source.

        Args:
            source: Source key
            watermark: Timestamp to save
            event_time_col: Column containing event time
        """
        self.store.save(source, watermark, event_time_col)

    def get_gap_info(
        self,
        source: str,
        current_time: datetime | None = None,
        event_time_col: str = "_nexus_event_time",
    ) -> dict[str, Any]:
        """Calculate gap information for backfill detection.

        Args:
            source: Source key
            current_time: Current time (defaults to now)
            event_time_col: Column containing event time

        Returns:
            Dict with gap information:
                - last_processed: Last processed timestamp
                - current_time: Current time
                - gap_minutes: Gap in minutes
                - needs_backfill: True if gap exceeds threshold
        """
        if current_time is None:
            current_time = datetime.now(timezone.utc)

        last_processed = self.get_last_processed_watermark(source, event_time_col)

        if last_processed is None:
            return {
                "source": source,
                "last_processed": None,
                "current_time": current_time,
                "gap_minutes": None,
                "needs_backfill": False,
                "reason": "No previous watermark found",
            }

        gap_minutes = (current_time - last_processed).total_seconds() / 60

        # Get threshold from config
        from common.config import load_backfill_config
        backfill_config = load_backfill_config()
        gap_threshold = backfill_config.get("gap_threshold_minutes", 30)

        return {
            "source": source,
            "last_processed": last_processed,
            "current_time": current_time,
            "gap_minutes": gap_minutes,
            "needs_backfill": gap_minutes > gap_threshold,
            "reason": f"Gap of {gap_minutes:.1f} minutes exceeds threshold of {gap_threshold}" if gap_minutes > gap_threshold else "Within threshold",
        }
