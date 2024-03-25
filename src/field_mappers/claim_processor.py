import json
from field_mappers.base import FHIRResourceProcessor, get_value_at_json_path
import logging
from normalizers.enum_normalizer import GenderNormalizer

LOG = logging.getLogger(__name__)



class FHIRClaimProcessor(FHIRResourceProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.date_fields = ["billablePeriod.start", "billablePeriod.end"]
        self.datetime_fields = ["created"]
        self.required_fields = ['billablePeriod', 'contained', 'created', 'id',
                           'patient', 'provider', 'resourceType', 'status', 'total']

    def map_values(self):
        """
        Maps to structured zone table
        Note: due to time constraints not all fields are included
        """
        mapping = {
                "patient_id": "patient.reference",
                "billing_start": "billablePeriod.start",
                "billing_end": "billablePeriod.end",
                "amount": "total.value"
        }
        instance_dict = {
                'origin': self.origin,
        }
        for dest, source in mapping.items():
            instance_dict[dest] = get_value_at_json_path(self.data, source)
        self.data = instance_dict


    def normalize(self):
        if "gender" in self.data:
            self.data["gender"] = GenderNormalizer.normalize(self.data["gender"])
