import pytest
from field_mappers.base import FHIRResourceProcessor

# Assuming the method is part of a class named 'WarningChecker'
class WarningChecker(FHIRResourceProcessor):
    def __init__(self, total_warnings, total_rows_processed):
        self.total_warnings = total_warnings
        self.total_rows_processed = total_rows_processed

@pytest.mark.parametrize("total_warnings, total_rows_processed, threshold, expected", [
    (30, 100, 50, True), # Warnings are below the threshold
    (60, 100, 50, False),  # Warnings are above the threshold
    (0, 100, 10, True),  # No warnings
    (100, 100, 90, False)  # Warnings exceed the threshold significantly
])
def test_total_warnings_below_threshold(total_warnings, total_rows_processed, threshold, expected):
    checker = WarningChecker(total_warnings, total_rows_processed)
    assert checker.total_warnings_below_threshold(threshold) == expected, "The method did not return the expected result"
