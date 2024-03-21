import json
from field_mappers.base import FHIRResourceProcessor
import logging

LOG = logging.getLogger(__name__)


class FHIRClaimsProcessor(FHIRResourceProcessor):

    def validate(self):
        """
        Validate the claims data.

        Implement validations such as checking required fields,
        validating date formats, ensuring identifiers meet expected patterns, etc.
        """
        required_fields = ['billablePeriod', 'contained', 'created', 'diagnosis', 'id',
                           'patient', 'provider', 'resourceType', 'status', 'total', 'type', 'use']
        for field in required_fields:
            if field not in self.data:
                LOG.warning(f"Missing required field: {field}")

        # patient ID may be missing and come in later in some cases
        if "patient" not in self.data:
            LOG.info("Patient data is missing.")
        elif "id" not in self.data:
            LOG.warning("Claim ID is missing.")



    def map_values(self):
        """
        Map values in the claims data to standardized formats or codes as needed.

        This can include mapping diagnosis codes, normalizing provider identifiers, etc.
        """
        # Example mapping: Shorten patient name to conform to specifications
        for name in self.data.get("contained", []):
            if name.get("resourceType") == "Patient":
                name["name"][0]["text"] = f"{name['name'][0]['given'][0][:10]}, {name['name'][0]['family'][:15]}"

        # Add more mappings as needed

    def normalize(self):
        """
        Normalize values such as dates and currencies to ensure consistency.

        This might include converting dates to a standard format, normalizing currency values, etc.
        """
        # Example normalization: Ensure date format is YYYY-MM-DD
        if "billablePeriod" in self.data:
            self.data["billablePeriod"]["start"] = self._normalize_date(
                    self.data["billablePeriod"]["start"]
                    )
            self.data["billablePeriod"]["end"] = self._normalize_date(self.data["billablePeriod"]["end"])

        # Add more normalizations as needed

    def _normalize_date(self, date_str):
        """
        Helper method to normalize date strings to a standard format (e.g., YYYY-MM-DD).
        """
        # Implement normalization logic if needed, currently assuming date is already in correct format
        return date_str
