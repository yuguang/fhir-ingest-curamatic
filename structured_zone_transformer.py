from normalizers.enum_normalizer import GenderNormalizer
import json
from datetime import datetime
from field_mappers.claim_processor import FHIRClaimsProcessor

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

# Example usage
ndjson_path = 'Claim.ndjson'
fhir_data = load_fhir_data(ndjson_path)
print(f"Loaded {len(fhir_data)} FHIR records.")

ingest_time = datetime.now()
for row in fhir_data:
    processor = FHIRClaimsProcessor(ingest_time)
    processed_data = processor.process(row)
    print(json.dumps(processed_data, indent=4))
