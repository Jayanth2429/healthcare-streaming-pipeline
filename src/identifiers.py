from __future__ import annotations

import hashlib


def deterministic_event_id(
    *,
    subject_id: str,
    hadm_id: str | None,
    event_type: str,
    source_table: str,
    event_timestamp: str,
    source_record_id: str,
) -> str:
    """Create a repeatable identifier from stable source/event attributes."""
    raw = "|".join(
        [
            subject_id.strip(),
            (hadm_id or "").strip(),
            event_type.strip().upper(),
            source_table.strip().lower(),
            event_timestamp.strip(),
            source_record_id.strip(),
        ]
    ).encode("utf-8")
    return "evt_" + hashlib.sha256(raw).hexdigest()[:24]
