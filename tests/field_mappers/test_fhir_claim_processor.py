from field_mappers.claim_processor import FHIRClaimProcessor
from datetime import datetime
import pytest


@pytest.fixture
def processor():
    now = datetime.utcnow()
    return FHIRClaimProcessor(now)


def test_map_values_with_null_patient_reference(processor):
    processor.data = {
            "billablePeriod": {"start": "2022-01-01", "end": "2022-01-31"},
            "total": {"value": 100},
            # patient.reference is not set, simulating a null value
    }
    processor.map_values()

    assert processor.data[
               'patient_id'] is None, "Expected 'patient_id' to be None when 'patient.reference' is not provided"

def test_gender_normalization(processor):
    processor.data = {
            "gender": "M"
    }

    processor.normalize()

    assert processor.data[
               'gender'] is 'Male'
