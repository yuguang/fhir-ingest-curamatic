import json
from field_mappers.base import FHIRResourceProcessor, get_value_at_json_path
from common.utils import TransformerLogger
from normalizers.enum_normalizer import GenderNormalizer

LOG = TransformerLogger(__name__)



class FHIRClaimProcessor(FHIRResourceProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.date_fields = ["billablePeriod.start", "billablePeriod.end"]
        self.datetime_fields = ["created"]
        self.required_fields = ['billablePeriod', 'contained', 'created', 'id',
                           'patient', 'provider', 'resourceType', 'status', 'total', 'insurance']


    def validate(self):
        super().validate()
        if not self.data["resourceType"].lower() == "claim":
            self.log_warning("Wrong resource type")
    def map_values(self):
        """
        Maps to structured zone table
        Note: due to time constraints not all fields are included
        """
        mapping = {
                "patient_id": "patient.reference",
                "billing_start": "billablePeriod.start",
                "billing_end": "billablePeriod.end",
                "provider": "provider.reference",
                "admitting_diagnosis": "diagnosis[0].diagnosisCodeableConcept.coding[0].code",
                "insurance": "insurance[0].coverage.identifier.value",
                "status": "status",
                "amount": "total.value"
        }
        instance_dict = {
                'origin': self.origin,
        }
        for dest, source in mapping.items():
            value = get_value_at_json_path(self.data, source)
            if value is None:
                self.log_warning(f"Missing value for {dest} at {source}")
            if "admitting_diagnosis" == dest:
                # check the diagnosis has a type of "admitting"
                diagnosis_type = get_value_at_json_path(self.data, "diagnosis[0].diagnosisCodeableConcept.type[0].coding[0].code")
                if not (diagnosis_type is not None and diagnosis_type.lower() == "admitting"):
                    # only record the admitting diagnosis if the type is "admitting"
                    LOG.info(f"Missing value for {dest} at {source}")
                    value = None
            instance_dict[dest] = value
        self.data = instance_dict


    def normalize(self):
        if "gender" in self.data:
            self.data["gender"] = GenderNormalizer.normalize(self.data["gender"])
        if "diagnosis" in self.data:
            self.data["diagnosis"] = self.data["diagnosis"].upper()
