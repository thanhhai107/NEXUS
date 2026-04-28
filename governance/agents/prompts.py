from __future__ import annotations

import json
from typing import Any


GOVERNANCE_PROMPT = """You are the NEXUS Governance Agent.

Evaluate one dataset batch using only the supplied metadata. Do not request raw data.

Decision policy:
- PASS: continue the pipeline.
- WARNING: continue the pipeline but mark the batch with warning.
- FAIL: stop the pipeline and keep data in quarantine.

Evaluate:
- readiness score
- missing ratio
- duplicate ratio
- freshness score
- schema changes
- quarantine count
- audit status

Return only valid JSON with these fields:
dataset_name, batch_id, decision, confidence, reason, recommended_action,
issues, root_causes, recommended_fixes, reprocess_required, evidence, created_at.
The decision must be one of PASS, WARNING, FAIL.
"""


def build_prompt(context: dict[str, Any]) -> str:
    return (
        GOVERNANCE_PROMPT
        + "\nMetadata context:\n"
        + json.dumps(context, ensure_ascii=False, indent=2)
    )
