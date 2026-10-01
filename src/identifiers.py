from __future__ import annotations

import hashlib


def deterministic_event_id(
    patient_id: str,
    event_type: str,
    facility_id: str,
    timestamp: str,
) -> str:
    """Create a repeatable identifier from stable event attributes."""
    raw = f"{patient_id}|{event_type}|{facility_id}|{timestamp}".encode("utf-8")
    return "evt_" + hashlib.sha256(raw).hexdigest()[:20]
