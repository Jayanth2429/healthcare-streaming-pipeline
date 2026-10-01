from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

EventType = Literal["ADMISSION", "DISCHARGE", "TRANSFER"]
SourceTable = Literal["admissions", "transfers"]


class HealthcareEvent(BaseModel):
    """Normalized event derived from the public MIMIC-IV Clinical Database Demo."""

    event_id: str = Field(min_length=6)
    subject_id: str = Field(min_length=1)
    hadm_id: str | None = None
    event_type: EventType
    event_timestamp: datetime
    source_table: SourceTable
    source_record_id: str = Field(min_length=1)
    care_unit: str | None = None
    payload: dict[str, str]
