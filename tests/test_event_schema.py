from datetime import datetime

import pytest
from pydantic import ValidationError

from src.event_model import HealthcareEvent
from src.identifiers import deterministic_event_id


def _valid_event() -> HealthcareEvent:
    return HealthcareEvent(
        event_id="evt_1234567890",
        subject_id="10000032",
        hadm_id="22595853",
        event_type="ADMISSION",
        event_timestamp=datetime.fromisoformat("2180-05-06T22:23:00"),
        source_table="admissions",
        source_record_id="22595853:admit",
        care_unit=None,
        payload={"admission_type": "URGENT"},
    )


def test_valid_event():
    assert _valid_event().event_type == "ADMISSION"


def test_invalid_event_type_rejected():
    with pytest.raises(ValidationError):
        HealthcareEvent(
            event_id="evt_1234567890",
            subject_id="10000032",
            hadm_id="22595853",
            event_type="INVALID",
            event_timestamp=datetime.fromisoformat("2180-05-06T22:23:00"),
            source_table="admissions",
            source_record_id="22595853:admit",
            payload={},
        )


def test_deterministic_event_id_is_repeatable():
    kwargs = dict(
        subject_id="10000032",
        hadm_id="22595853",
        event_type="ADMISSION",
        source_table="admissions",
        event_timestamp="2180-05-06 22:23:00",
        source_record_id="22595853:admit",
    )
    assert deterministic_event_id(**kwargs) == deterministic_event_id(**kwargs)


def test_deterministic_event_id_changes_for_different_source_record():
    common = dict(
        subject_id="10000032",
        hadm_id="22595853",
        event_type="TRANSFER",
        source_table="transfers",
        event_timestamp="2180-05-06 23:30:00",
    )
    first = deterministic_event_id(**common, source_record_id="111")
    second = deterministic_event_id(**common, source_record_id="222")
    assert first != second
