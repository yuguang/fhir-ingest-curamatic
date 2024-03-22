import json, csv
from pandas.io.json import json_normalize
from datetime import datetime
from field_mappers.claim_processor import FHIRClaimsProcessor
from field_mappers.patient_processor import FHIRPatientProcessor

def load_fhir_data(ndjson_path):
    """
    Load FHIR data from an NDJSON file.

    Parameters:
    - ndjson_path: Path to the NDJSON file containing FHIR data.

    Returns:
    - fhir_data: A list of dictionaries, where each dictionary represents a FHIR record.
    """
    fhir_data = []
    with open(ndjson_path, 'r') as file:
        for line in file:
            # Parse the JSON object in each line
            fhir_record = json.loads(line)
            fhir_data.append(fhir_record)
    return fhir_data


def write_to_file(filename, output_data):
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)

        normalized_data = json_normalize(output_data)
        writer.writerow(normalized_data.columns)

        for index, row in normalized_data.iterrows():
            writer.writerow(row)

# process claims
ndjson_path = 'Claim.ndjson'
fhir_data = load_fhir_data(ndjson_path)
print(f"Loaded {len(fhir_data)} FHIR records.")

ingest_time = datetime.now()
csv_file = "claims.csv"
output = []
for row_num, row in enumerate(fhir_data):
    processor = FHIRClaimsProcessor(ingest_time)
    processed_data = processor.process(row, row_num)
    output.append(processed_data)
    print(json.dumps(processed_data, indent=4))
write_to_file(csv_file, output)

# process patients
ndjson_path = 'Patient.ndjson'
fhir_data = load_fhir_data(ndjson_path)
print(f"Loaded {len(fhir_data)} FHIR records.")

ingest_time = datetime.now()
csv_file = "patients.csv"

output = []
for row_num, row in enumerate(fhir_data):
    processor = FHIRPatientProcessor(ingest_time)
    processed_data = processor.process(row, row_num)
    output.append(processed_data)
    print(json.dumps(processed_data, indent=4))

write_to_file(csv_file, output)
