import json, re
from datetime import datetime
from typing import Dict, Any
import logging

LOG = logging.getLogger(__name__)

ISO8601_MATCH = match_iso8601 = re.compile(
        r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T'
        r'(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?'
        r'(Z|[+-](?:2[0-3]|[01][0-9]):?[0-5][0-9])?$'
).match


class ValidationError(Exception):
    pass


def get_value_at_json_path(json_data, json_path):
    """Returns the value at the given JSON path within the given JSON data.

    Args:
      json_data: A dictionary or list representing the JSON data.
      json_path: A string representing the JSON path.

    Returns:
      The value at the given JSON path, or None if the path does not exist.
    """
    import re

    # Split the JSON path into parts, handling both dots and square brackets.
    keys = re.split(r'\.|\[|\]', json_path)
    keys = [key for key in keys if key]  # Remove empty strings from the list

    # Iterate over the keys, getting the value at each key or index from the JSON data.
    for key in keys:
        if isinstance(json_data, list):
            try:
                # Convert the key to an integer index and get the value from the list.
                index = int(key)
                json_data = json_data[index]
            except ValueError:
                # If the key is not an integer, return None.
                return None
            except IndexError:
                # If the index is out of range, return None.
                return None
        elif isinstance(json_data, dict):
            # If the JSON data is a dictionary, get the value at the given key.
            if key not in json_data:
                return None
            json_data = json_data[key]
        else:
            # If the JSON data is not a list or dictionary, return None.
            return None

    # Return the value at the given JSON path.
    return json_data


class FHIRResourceProcessor:
    def __init__(self, ingest_ts):
        """
        Initialize the processor with FHIR claims data.

        Parameters:
        - ingest_ts: a timestamp representing the current local time for the source data
        """
        self.ingest_ts = ingest_ts
        self.data = {}
        self.origin = 1
        self.row_num = 0
        self.date_fields = []
        self.datetime_fields = []
        self.required_fields = []

    def validate_dates(self):
        """
        Validate date fields
        """
        for field in self.date_fields:
            self.validate_date_string(self.data, field)
        for field in self.datetime_fields:
            self.validate_datetime_string(self.data, field)

    def _nested_field_dne(self, field):
        """
        Checks if a nested JSON field does not exist (DNE)
        """
        if ("." in field or "[" in field) and not get_value_at_json_path(self.data, field):
            return False

    def validate(self):
        """
        Validate the data.

        Implement validations such as checking required fields,
        validating date formats, ensuring identifiers meet expected patterns, etc.
        """
        for field in self.required_fields:
            if field not in self.data or self._nested_field_dne(field):
                self.log_warning(f"Missing required field: {field}")

        self.validate_dates()

    def log_warning(self, msg):
        LOG.warning(f"{msg} at row {self.row_num}")

    @staticmethod
    def validate_date_string(record: Dict[str, Any], key: str):
        """
        Validate date string format (%Y-%m-%d).
        """
        if not get_value_at_json_path(record, key):
            return
        try:
            date_string = get_value_at_json_path(record, key)
            if len(date_string) != 10:
                raise ValueError()

            datetime.strptime(date_string, '%Y-%m-%d')

        except ValueError:
            LOG.warning(
                    "Values in column '{key}' is not valid date string, should be YYYY-MM-DD".format(key=key)
            )

    @staticmethod
    def validate_datetime_string(record: Dict[str, Any], key: str):
        """
        Validate date string format (ISO8601).
        """
        if not get_value_at_json_path(record, key):
            return
        # noinspection PyBroadException
        try:
            if ISO8601_MATCH(get_value_at_json_path(record, key)) is not None:
                return True
        except:
            pass
        LOG.warning("Values in column '{key}' is not valid ISO8601 string".format(key=key))

    def map_values(self):
        """
        Map values in the data to standardized formats or codes as needed.

        This can include mapping diagnosis codes, normalizing provider identifiers, etc.
        """
        pass

    def normalize(self):
        """
        Normalize values such as dates and currencies to ensure consistency.

        This might include converting dates to a standard format, normalizing currency values, etc.
        """
        pass

    def _normalize_date(self, date_str):
        """
        Helper method to normalize date strings to a standard format (e.g., YYYY-MM-DD).
        """
        # Implement normalization logic if needed, currently assuming date is already in correct format
        return date_str

    def process(self, data, row_num):
        """
        Main method to process the claims data by validating, mapping, and normalizing it.
        """
        self.data = data
        self.row_num = row_num
        self.validate()
        self.map_values()
        self.normalize()
        return self.data
