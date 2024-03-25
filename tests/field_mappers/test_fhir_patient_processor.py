import pytest
from field_mappers.patient_processor import FHIRPatientProcessor
from datetime import datetime, timedelta


@pytest.fixture
def processor():
    now = datetime.utcnow()
    return FHIRPatientProcessor(now)


def test_validate_with_all_required_fields_present(caplog, processor):
    processor.data = {
            "name": "Test Name",
            "id": "123456",
            "other_field": "some value"
    }
    processor.validate()
    # Verify that no warning was logged for missing required fields
    assert not any("Missing required field" in message for message in caplog.text
                   ), "No required fields should be missing"


def test_validate_with_missing_required_fields(caplog, processor):
    processor.data = {
            "name": "Test Name",
            # "id" is intentionally missing to test the validation logic
    }
    processor.validate()
    # Verify that a warning was logged for the missing "id" field
    assert "Missing required field: id" in caplog.text, "A warning should be logged for the missing 'id' field"


def test_validate_dates_with_invalid_dates(caplog, processor):
    processor.data = {
            "birthDate": "MM-YY",
    }
    processor.validate_dates()
    assert "WARNING" in caplog.text, "Warnings should be logged for invalid date formats"


def test_validate_dates_with_future_last_updated(caplog, processor):
    processor.data = {
            "meta": {"lastUpdated": "2022-02-30TAA:00:00Z"}  # Invalid datetime
    }
    processor.validate_dates()
    assert "WARNING" in caplog.text, "Warnings should be logged for invalid date formats"
