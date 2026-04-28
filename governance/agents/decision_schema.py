from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal


Decision = Literal["PASS", "WARNING", "FAIL"]
VALID_DECISIONS = {"PASS", "WARNING", "FAIL"}


@dataclass
class AgentDecision:
    dataset_name: str
    batch_id: str
    decision: Decision
    confidence: float
    reason: str
    recommended_action: str
    issues: list[str] = field(default_factory=list)
    root_causes: list[str] = field(default_factory=list)
    recommended_fixes: list[str] = field(default_factory=list)
    reprocess_required: bool = False
    evidence: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def __post_init__(self) -> None:
        if self.decision not in VALID_DECISIONS:
            raise ValueError(f"Invalid decision: {self.decision}")
        self.confidence = max(0.0, min(1.0, float(self.confidence)))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "AgentDecision":
        return cls(
            dataset_name=str(payload["dataset_name"]),
            batch_id=str(payload["batch_id"]),
            decision=payload["decision"],
            confidence=float(payload["confidence"]),
            reason=str(payload["reason"]),
            recommended_action=str(payload["recommended_action"]),
            issues=list(payload.get("issues") or []),
            root_causes=list(payload.get("root_causes") or []),
            recommended_fixes=list(payload.get("recommended_fixes") or []),
            reprocess_required=bool(payload.get("reprocess_required", False)),
            evidence=dict(payload.get("evidence") or {}),
            created_at=str(payload.get("created_at") or datetime.now(timezone.utc).isoformat()),
        )

    @classmethod
    def from_json(cls, payload: str) -> "AgentDecision":
        return cls.from_dict(json.loads(payload))
