import dask.dataframe as dd
from soda.scan import Scan

# Create Soda Library Scan object and set a few required properties
scan = Scan()
scan.set_scan_definition_name("test")
scan.set_data_source_name("dask")

# Read a `patients` CSV file with columns 'city', 'population'
ddf = dd.read_csv('patients.csv')

scan.add_dask_dataframe(dataset_name="patients", dask_df=ddf)

# Define checks using SodaCL

checks = """
checks for patients:
    - row_count > 0
    - duplicate_count(patient_id) = 0
"""

# Add the checks to the scan and set output to verbose
scan.add_sodacl_yaml_str(checks)

scan.set_verbose(True)

# Execute the scan
scan.execute()

# Inspect the scan object to review scan results
scan.get_scan_results()
