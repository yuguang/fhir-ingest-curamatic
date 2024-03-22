import json
from field_mappers.base import FHIRResourceProcessor, get_value_at_json_path
import logging
from normalizers.enum_normalizer import GenderNormalizer

LOG = logging.getLogger(__name__)



class FHIRPatientProcessor(FHIRResourceProcessor):
    def validate_dates(self):
        """
        Validate date fields
        """
        date_fields = ["birthDate"]
        datetime_fields = ["meta.lastUpdated"]
        for field in date_fields:
            self.validate_date_string(self.data, field)
        for field in datetime_fields:
            self.validate_datetime_string(self.data, field)
        if self.data["meta.lastUpdated"] is not None and self.data["meta.lastUpdated"] > self.ingest_ts:
            self.log_warning("Invalid value: lastUpdated is in the future")

    def validate(self):
        pass


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
