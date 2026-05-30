"""Dead Letter Queue Module.

Provides DLQ for operational failures (not bad data - those go to quarantine).
Supports both file-based and Postgres storage, with retry backoff and scheduling.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from common.config import RUNTIME_DIR


logger = logging.getLogger(__name__)


DEFAULT_DLQ_DIR = RUNTIME_DIR / "dlq"
DLQ_STREAM = "dlq"
DLQ_INDEX_DIR = RUNTIME_DIR / "dlq" / ".index"


# --- Retry Policy ---

@dataclass
class RetryPolicy:
    """Retry policy for DLQ replay."""
    max_attempts: int = 3
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    jitter_seconds: float = 0.5
    
    def get_delay(self, attempt: int) -> float:
        """Calculate backoff delay for an attempt."""
        import random
        delay = min(self.backoff_base_seconds * (2 ** (attempt - 1)), self.backoff_max_seconds)
        jitter = random.uniform(0, self.jitter_seconds)
        return delay + jitter


# --- DLQ Entry ---

@dataclass
class DLQEntry:
    """An entry in the Dead Letter Queue."""
    category: str
    source: str
    error: str
    payload: dict[str, Any]
    captured_at: str
    attempts: int = 0
    last_attempt: str | None = None
    next_retry: str | None = None
    error_type: str | None = None
    dataset: str | None = None
    topic: str | None = None
    batch_id: str | None = None
    run_id: str | None = None
    status: str = "pending"  # pending, retrying, succeeded, failed, skipped
    context: dict[str, Any] | None = None
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "source": self.source,
            "error": self.error,
            "error_type": self.error_type,
            "attempts": self.attempts,
            "last_attempt": self.last_attempt,
            "next_retry": self.next_retry,
            "dataset": self.dataset,
            "topic": self.topic,
            "batch_id": self.batch_id,
            "run_id": self.run_id,
            "status": self.status,
            "captured_at": self.captured_at,
            "context": self.context,
            "payload": self.payload,
        }
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DLQEntry":
        return cls(
            category=data.get("category", ""),
            source=data.get("source", ""),
            error=data.get("error", ""),
            error_type=data.get("error_type"),
            payload=data.get("payload", {}),
            captured_at=data.get("captured_at", ""),
            attempts=data.get("attempts", 0),
            last_attempt=data.get("last_attempt"),
            next_retry=data.get("next_retry"),
            dataset=data.get("dataset"),
            topic=data.get("topic"),
            batch_id=data.get("batch_id"),
            run_id=data.get("run_id"),
            status=data.get("status", "pending"),
            context=data.get("context"),
        )


# --- Enhanced DLQ Class ---

class EnhancedDLQ:
    """Enhanced Dead Letter Queue with retry backoff and scheduling."""
    
    def __init__(
        self,
        dlq_dir: Path | None = None,
        index_dir: Path | None = None,
        retry_policy: RetryPolicy | None = None,
    ):
        self.dlq_dir = dlq_dir or DEFAULT_DLQ_DIR
        self.index_dir = index_dir or DLQ_INDEX_DIR
        self.dlq_dir.mkdir(parents=True, exist_ok=True)
        self.index_dir.mkdir(parents=True, exist_ok=True)
        self.retry_policy = retry_policy or RetryPolicy()
    
    def record(
        self,
        category: str,
        source: str,
        error: str,
        payload: dict[str, Any],
        **kwargs,
    ) -> Path:
        """Record an event to the DLQ."""
        entry = DLQEntry(
            category=category,
            source=source,
            error=error,
            payload=payload,
            captured_at=datetime.now(timezone.utc).isoformat(),
            context=kwargs.get("context"),
            **kwargs,
        )
        return self._write_entry(entry)
    
    def _write_entry(self, entry: DLQEntry) -> Path:
        """Write a DLQ entry to disk."""
        entry_path = self.dlq_dir / f"{entry.category}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.jsonl"
        
        with entry_path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(json.dumps(entry.to_dict(), ensure_ascii=False) + "\n")
        
        self._update_index(entry)
        return entry_path
    
    def _update_index(self, entry: DLQEntry) -> None:
        """Update the DLQ index."""
        index_file = self.index_dir / f"{entry.category}.index.json"
        
        index = {}
        if index_file.exists():
            try:
                index = json.loads(index_file.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                index = {}
        
        key = f"{entry.source}:{entry.captured_at}"
        index[key] = entry.to_dict()
        
        if len(index) > 1000:
            sorted_items = sorted(index.items(), key=lambda x: x[1]["captured_at"])
            index = dict(sorted_items[-1000:])
        
        index_file.write_text(json.dumps(index, indent=2), encoding="utf-8")
    
    def list_pending(self, category: str | None = None) -> list[DLQEntry]:
        """List pending DLQ entries."""
        entries = []
        now = datetime.now(timezone.utc)
        
        for path in self.dlq_dir.glob("*.jsonl"):
            if category and not path.name.startswith(category):
                continue
            
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line)
                        entry = DLQEntry.from_dict(data)
                        
                        if entry.status != "pending":
                            continue
                        
                        if entry.next_retry:
                            next_retry = datetime.fromisoformat(entry.next_retry)
                            if next_retry > now:
                                continue
                        
                        entries.append(entry)
                    except (json.JSONDecodeError, KeyError):
                        continue
        
        return sorted(entries, key=lambda e: e.captured_at)
    
    def retry_entry(
        self,
        entry: DLQEntry,
        handler: Callable[[DLQEntry], bool],
        retry_policy: RetryPolicy | None = None,
    ) -> DLQEntry:
        """Retry a DLQ entry with backoff."""
        policy = retry_policy or self.retry_policy
        
        entry.attempts += 1
        entry.last_attempt = datetime.now(timezone.utc).isoformat()
        entry.status = "retrying"
        
        try:
            success = handler(entry)
            
            if success:
                entry.status = "succeeded"
                logger.info(f"DLQ entry succeeded after {entry.attempts} attempts")
            else:
                self._handle_failure(entry, policy)
        except Exception as e:
            entry.error = f"{type(e).__name__}: {str(e)}"
            self._handle_failure(entry, policy)
        
        self._update_entry(entry)
        return entry
    
    def _handle_failure(self, entry: DLQEntry, policy: RetryPolicy) -> None:
        """Handle a failed retry attempt."""
        if entry.attempts >= policy.max_attempts:
            entry.status = "failed"
            logger.warning(f"DLQ entry exceeded max attempts ({policy.max_attempts})")
        else:
            entry.status = "pending"
            delay = policy.get_delay(entry.attempts)
            entry.next_retry = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()
    
    def _update_entry(self, entry: DLQEntry) -> None:
        """Update an entry in the DLQ file."""
        for path in self.dlq_dir.glob(f"{entry.category}_*.jsonl"):
            lines = []
            updated = False
            
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    data = json.loads(line.strip())
                    if (
                        data.get("source") == entry.source
                        and data.get("captured_at") == entry.captured_at
                        and not updated
                    ):
                        lines.append(json.dumps(entry.to_dict(), ensure_ascii=False))
                        updated = True
                    else:
                        lines.append(line.rstrip())
            
            if updated:
                with path.open("w", encoding="utf-8", newline="\n") as f:
                    for line in lines:
                        f.write(line + "\n")
                break
    
    def replay_with_backoff(
        self,
        handler: Callable[[DLQEntry], bool],
        category: str | None = None,
        batch_size: int = 100,
        retry_policy: RetryPolicy | None = None,
    ) -> dict[str, Any]:
        """Replay DLQ entries with exponential backoff."""
        policy = retry_policy or self.retry_policy
        pending = self.list_pending(category)[:batch_size]
        
        results = {"total": 0, "succeeded": 0, "failed": 0, "pending": 0, "retried": 0}
        
        for entry in pending:
            updated = self.retry_entry(entry, handler, policy)
            
            if updated.status == "succeeded":
                results["succeeded"] += 1
            elif updated.status == "failed":
                results["failed"] += 1
            elif updated.status == "pending":
                results["pending"] += 1
                results["retried"] += 1
            results["total"] += 1
        
        return results
    
    def get_stats(self, category: str | None = None) -> dict[str, Any]:
        """Get DLQ statistics."""
        stats = {"total": 0, "pending": 0, "retrying": 0, "succeeded": 0, "failed": 0, "by_source": {}}
        
        for path in self.dlq_dir.glob("*.jsonl"):
            if category and not path.name.startswith(category):
                continue
            
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line)
                        entry = DLQEntry.from_dict(data)
                        
                        stats["total"] += 1
                        stats[entry.status] = stats.get(entry.status, 0) + 1
                        
                        source = entry.source
                        if source not in stats["by_source"]:
                            stats["by_source"][source] = {"total": 0, "pending": 0, "failed": 0}
                        stats["by_source"][source]["total"] += 1
                        stats["by_source"][source][entry.status] = stats["by_source"][source].get(entry.status, 0) + 1
                    except (json.JSONDecodeError, KeyError):
                        continue
        
        return stats
    
    def archive_completed(self, older_than_days: int = 7) -> int:
        """Archive completed DLQ entries older than specified days."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
        archived = 0
        
        for path in self.dlq_dir.glob("*.jsonl"):
            lines = []
            removed = 0
            
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    data = json.loads(line.strip())
                    entry = DLQEntry.from_dict(data)
                    
                    captured = datetime.fromisoformat(entry.captured_at)
                    
                    if entry.status in ("succeeded", "skipped") and captured < cutoff:
                        removed += 1
                    else:
                        lines.append(line.rstrip())
            
            if removed > 0:
                with path.open("w", encoding="utf-8", newline="\n") as f:
                    for line in lines:
                        f.write(line + "\n")
                archived += removed
        
        return archived


# --- Global instance ---
_dlq: EnhancedDLQ | None = None


def get_dlq() -> EnhancedDLQ:
    """Get global DLQ instance."""
    global _dlq
    if _dlq is None:
        _dlq = EnhancedDLQ()
    return _dlq


# --- File helpers ---

def _dlq_file(category: str, dlq_dir: Path) -> Path:
    dlq_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return dlq_dir / f"{category}_{timestamp}.jsonl"


# --- Core Functions (Backward Compatible) ---

def record_dlq_event(
    category: str,
    payload: Mapping[str, Any],
    *,
    source: str,
    error: str,
    error_type: str | None = None,
    attempts: int | None = None,
    dataset: str | None = None,
    topic: str | None = None,
    batch_id: str | None = None,
    run_id: str | None = None,
    source_path: str | Path | None = None,
    actor: str | None = None,
    dlq_dir: Path | None = None,
) -> Path:
    """Capture an operational failure in the DLQ.
    
    Use this for failures that are NOT bad data records (those go to quarantine).
    Examples: Kafka publish failed, downstream task crashed, job timed out.
    """
    # Try to use Postgres if available
    try:
        from governance.storage import append_governance_event, using_postgres_storage
        from governance.context import GovernanceContext, utc_now_iso
        
        if using_postgres_storage():
            context = GovernanceContext.from_values(batch_id, run_id, source_path, actor)
            envelope = {
                "category": category,
                "source": source,
                "error": error,
                "error_type": error_type,
                "attempts": attempts,
                "dataset": dataset,
                "topic": topic,
                **context.to_event_fields(),
                "captured_at": utc_now_iso(),
                "payload": dict(payload),
            }
            append_governance_event(DLQ_STREAM, envelope)
            return dlq_dir or DEFAULT_DLQ_DIR
    except ImportError:
        pass
    
    # Fall back to file-based
    dlq_dir = dlq_dir or DEFAULT_DLQ_DIR
    
    # Build context manually if imports failed
    context = {
        "batch_id": batch_id,
        "run_id": run_id,
        "source_path": str(source_path) if source_path else None,
        "actor": actor,
    }
    
    envelope = {
        "category": category,
        "source": source,
        "error": error,
        "error_type": error_type,
        "attempts": attempts,
        "dataset": dataset,
        "topic": topic,
        **context,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "payload": dict(payload),
    }
    
    output_path = _dlq_file(category, dlq_dir)
    with output_path.open("a", encoding="utf-8", newline="\n") as file:
        file.write(json.dumps(envelope, ensure_ascii=False) + "\n")
    return output_path


def list_dlq_events(dlq_dir: Path | None = None) -> list[dict[str, Any]]:
    """List all DLQ events."""
    dlq_dir = dlq_dir or DEFAULT_DLQ_DIR
    if not dlq_dir.exists():
        return []
    
    events: list[dict[str, Any]] = []
    for path in sorted(dlq_dir.glob("*.jsonl")):
        with path.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return events


def replay_dlq_events(
    handler,
    *,
    category: str | None = None,
    source: str | None = None,
    dataset: str | None = None,
    dlq_dir: Path | None = None,
    retry_policy: RetryPolicy | None = None,
    with_backoff: bool = False,
) -> dict[str, Any]:
    """Iterate DLQ events and call handler for each match.
    
    Args:
        handler: Function that processes each event, returns True on success
        category: Filter by category
        source: Filter by source
        dataset: Filter by dataset
        dlq_dir: DLQ directory
        retry_policy: Retry policy for backoff (if with_backoff=True)
        with_backoff: Whether to use exponential backoff retry
        
    Returns:
        Dict with matched, succeeded, failed counts
    """
    if with_backoff:
        dlq = EnhancedDLQ(dlq_dir=dlq_dir, retry_policy=retry_policy)
        
        def entry_handler(entry: DLQEntry) -> bool:
            # Convert DLQEntry to dict for handler compatibility
            event = entry.to_dict()
            return bool(handler(event))
        
        return dlq.replay_with_backoff(entry_handler, category=category, retry_policy=retry_policy)
    
    # Original replay logic
    dlq_dir = dlq_dir or DEFAULT_DLQ_DIR
    events = list_dlq_events(dlq_dir)
    matched = 0
    succeeded = 0
    failed: list[dict[str, Any]] = []
    
    for event in events:
        if category and event.get("category") != category:
            continue
        if source and event.get("source") != source:
            continue
        if dataset and event.get("dataset") != dataset:
            continue
        
        matched += 1
        try:
            ok = bool(handler(event))
        except Exception as exc:
            ok = False
            failed.append({"event": event, "error": f"{type(exc).__name__}: {exc}"})
        
        if ok:
            succeeded += 1
        elif not failed or failed[-1]["event"] is not event:
            failed.append({"event": event, "error": "handler_returned_false"})
    
    return {"matched": matched, "succeeded": succeeded, "failed": failed}


# --- Convenience functions using enhanced DLQ ---

def record_dlq_with_retry(
    category: str,
    source: str,
    error: str,
    payload: dict[str, Any],
    **kwargs,
) -> Path:
    """Record to DLQ using enhanced DLQ with retry support."""
    return get_dlq().record(category, source, error, payload, **kwargs)


def replay_dlq_with_backoff(
    handler: Callable[[Any], bool],
    category: str | None = None,
    retry_policy: RetryPolicy | None = None,
) -> dict[str, Any]:
    """Replay DLQ with exponential backoff."""
    return get_dlq().replay_with_backoff(handler, category=category, retry_policy=retry_policy)


def get_dlq_stats(category: str | None = None) -> dict[str, Any]:
    """Get DLQ statistics."""
    return get_dlq().get_stats(category)


__all__ = [
    # Core
    "DEFAULT_DLQ_DIR",
    "DLQ_STREAM",
    "record_dlq_event",
    "list_dlq_events",
    "replay_dlq_events",
    # Enhanced
    "EnhancedDLQ",
    "DLQEntry",
    "RetryPolicy",
    "get_dlq",
    "record_dlq_with_retry",
    "replay_dlq_with_backoff",
    "get_dlq_stats",
]
