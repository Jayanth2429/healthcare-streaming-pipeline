from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


EventType = Literal["ADT", "ORU", "MDM"]


class HealthcareEvent(BaseModel):
    """Normalized synthetic healthcare event used by the demo pipeline."""

    event_id: str = Field(min_length=6)
    patient_id: str = Field(min_length=4)
    event_type: EventType
    facility_id: str = Field(min_length=4)
    event_timestamp: datetime
    source_system: str = Field(min_length=3)
    payload: dict[str, str]
