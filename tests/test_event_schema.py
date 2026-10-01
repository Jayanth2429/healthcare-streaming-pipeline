from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.event_model import HealthcareEvent
from src.identifiers import deterministic_event_id


def test_valid_event():
    event = HealthcareEvent(
        event_id="evt_123456",
        patient_id="pt_1234",
        event_type="ADT",
        facility_id="facility_01",
        event_timestamp=datetime.now(timezone.utc),
        source_system="synthetic_ehr",
        payload={"visit_type": "ER", "status": "admitted"},
    )
    assert event.event_type == "ADT"


def test_invalid_event_type_rejected():
    with pytest.raises(ValidationError):
        HealthcareEvent(
            event_id="evt_123456",
            patient_id="pt_1234",
            event_type="INVALID",
            facility_id="facility_01",
            event_timestamp=datetime.now(timezone.utc),
            source_system="synthetic_ehr",
            payload={},
        )


def test_deterministic_event_id_is_repeatable():
    args = ("pt_1234", "ADT", "facility_01", "2026-10-01T14:22:10+00:00")
    assert deterministic_event_id(*args) == deterministic_event_id(*args)


def test_deterministic_event_id_changes_when_business_attributes_change():
    timestamp = "2026-10-01T14:22:10+00:00"
    first = deterministic_event_id("pt_1234", "ADT", "facility_01", timestamp)
    second = deterministic_event_id("pt_1234", "ORU", "facility_01", timestamp)
    assert first != second
