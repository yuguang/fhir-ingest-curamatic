import json
from field_mappers.base import FHIRResourceProcessor, get_value_at_json_path
from common.utils import TransformerLogger
from normalizers.enum_normalizer import GenderNormalizer

LOG = TransformerLogger(__name__)



class FHIRPatientProcessor(FHIRResourceProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.date_fields = ["birthDate"]
        self.datetime_fields = ["meta.lastUpdated"]
        self.required_fields = [
                "name",
                "id"
        ]


    def map_values(self):
        """
        Maps to structured zone table
        Note: due to time constraints not all fields are included
        """
        mapping = {
                "first_name": "name[0].given[0]",
                "last_name": "name[0].family",
                "patient_id": "id"
        }
        instance_dict = {
                'origin': self.origin,
        }
        for dest, source in mapping.items():
            instance_dict[dest] = get_value_at_json_path(self.data, source)
        self.data = instance_dict


    def normalize(self):
        pass
