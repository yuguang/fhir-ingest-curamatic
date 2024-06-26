import json, csv
from pandas.io.json import json_normalize
import pandas as pd
from datetime import datetime
from field_mappers.claim_processor import FHIRClaimProcessor
from field_mappers.patient_processor import FHIRPatientProcessor
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os, psycopg2
from common.utils import TransformerLogger

LOG = TransformerLogger(__name__)


# Load environment variables from .env file
load_dotenv()

DATABASE_USER = os.getenv('DATABASE_USER')
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
DATABASE_HOST = os.getenv('DATABASE_HOST')
DATABASE_PORT = os.getenv('DATABASE_PORT')
DATABASE_NAME = os.getenv('DATABASE_NAME')

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
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)

        normalized_data = json_normalize(output_data)
        writer.writerow(normalized_data.columns)

        for index, row in normalized_data.iterrows():
            writer.writerow(row)

def write_to_db(table, output_data, mode='replace'):
    df = pd.DataFrame(output_data)
    pg_connection_dict = {
            'dbname': DATABASE_NAME,
            'user': DATABASE_USER,
            'password': DATABASE_PASSWORD,
            'port': DATABASE_PORT,
            'host': DATABASE_HOST
    }
    engine = psycopg2.connect(**pg_connection_dict)
    df.to_sql(table, engine, index=False, if_exists=mode)

pg_connection_dict = {
        'dbname': DATABASE_NAME,
        'user': DATABASE_USER,
        'password': DATABASE_PASSWORD,
        'port': DATABASE_PORT,
        'host': DATABASE_HOST
}


def upsert_claim(claim_details):
    """
    Upsert a claim record. If the claim is new or modified, it's inserted/updated in the 'claims' table.
    Previous versions of modified records are saved to 'claims_history'.

    :param claim_details: Dictionary with claim data.
    """
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**pg_connection_dict)
    try:
        with conn.cursor() as cursor:
            # Extract claim details
            claim_id = claim_details['claim_id']
            patient_id = claim_details['patient_id']
            billing_start = claim_details['billing_start']
            billing_end = claim_details['billing_end']
            provider = claim_details['provider']
            admitting_diagnosis = claim_details.get('admitting_diagnosis')
            insurance = claim_details['insurance']
            status = claim_details['status']
            amount = claim_details['amount']

            # Check for an existing claim for the patient within the same billing period
            cursor.execute("""
                SELECT claim_id, insert_ts FROM claims WHERE claim_id = %s
            """, (claim_id,)
            )
            existing_claim = cursor.fetchone()

            now = datetime.now()
            # set the default for the history table
            insert_ts = now

            if existing_claim:
                # If existing, move current record to history before updating
                claim_id_db, insert_ts = existing_claim

                # Update existing claim record
                cursor.execute("""
                    UPDATE claims
                    SET provider = %s, admitting_diagnosis = %s, insurance = %s, status = %s,
                        amount = %s, insert_ts = %s
                    WHERE claim_id = %s
                """, (
                provider, admitting_diagnosis, insurance, status, amount, now, claim_id)
                               )
            else:
                # Insert new claim record
                cursor.execute("""
                    INSERT INTO claims (claim_id, patient_id, billing_start, billing_end, provider, admitting_diagnosis,
                                        insurance, status, amount, insert_ts)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (claim_id, patient_id, billing_start, billing_end, provider, admitting_diagnosis,
                      insurance, status, amount, now)
                               )
            cursor.execute("""
                INSERT INTO claims_history (claim_id, patient_id, billing_start, billing_end, provider,
                                            admitting_diagnosis, insurance, status, amount, insert_ts, change_ts)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (claim_id, patient_id, billing_start, billing_end, provider,
                  admitting_diagnosis, insurance, status, amount, insert_ts, now)
                           )

            # Commit the transaction
            conn.commit()

    except Exception as e:
        LOG.error(f"An error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()

def upsert_patient(patient_id, first_name, last_name, origin):
    """Insert or update a patient's record and log changes to the history table."""
    try:
        conn = psycopg2.connect(**pg_connection_dict)
        cursor = conn.cursor()

        # Check if the patient already exists in the current table
        cursor.execute('SELECT id, insert_ts FROM patients WHERE patient_id = %s', (patient_id,))
        result = cursor.fetchone()
        # set the default for the history table
        now = datetime.now()
        insert_ts = now

        if result:
            # If patient exists, update the current table and log to history
            patient_id_db, insert_ts = result
            cursor.execute('''
                UPDATE patients SET first_name = %s, last_name = %s, insert_ts = %s WHERE patient_id = %s
            ''', (first_name, last_name, now, patient_id)
                           )
        else:
            # If patient does not exist, insert into the current table
            cursor.execute('''
                INSERT INTO patients (first_name, last_name, patient_id, insert_ts)
                VALUES (%s, %s, %s, %s)
            ''', (first_name, last_name, patient_id, now)
                           )

        cursor.execute('''
            INSERT INTO patients_history (first_name, last_name, patient_id, insert_ts, change_ts)
            VALUES (%s, %s, %s, %s, %s)
        ''', (first_name, last_name, patient_id, insert_ts, now)
                       )

        conn.commit()

    except Exception as e:
        LOG.error(f"An error occurred: {e}")
        conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def percent_of_patients_above_threshold(patient_ids, threshold, connection_dict):
    """
    Checks that the percentage of patient IDs in the history table is above a certain threshold
    :param patient_ids: a list of patient IDs
    :param threshold: percentage in the range of 0-100
    :return: bool indicating whether the percentage was above threshold
    """
    conn = psycopg2.connect(**connection_dict)

    # handle special case where there are no records
    sql_query = f"""
    SELECT COUNT(*) AS count
    FROM patients_history
    """
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        result = cursor.fetchone()
        if result and result[0] == 0:
            return True

    sql_query = f"""
    SELECT COUNT(*) AS count_matching_values
    FROM patients_history
    WHERE patient_id IN ({', '.join([f"'{s}'" for s in map(str, patient_ids)])});
    """

    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        result = cursor.fetchone()
        total_values_in_list = len(patient_ids)
        if result:
            matching_values_count = result[0]
            percentage = (matching_values_count / total_values_in_list) * 100
            return percentage > (threshold / 100)
        else:
            LOG.info("Percent of patients in history table below threshold")
            return False


if __name__ == "__main__":
    ndjson_path = '/data/Claim.ndjson'
    fhir_data = load_fhir_data(ndjson_path)
    LOG.info(f"Loaded {len(fhir_data)} FHIR records.")

    ingest_time = datetime.now()
    output = []
    processor = FHIRClaimProcessor(ingest_time)
    for row_num, row in enumerate(fhir_data):
        processed_data = processor.process(row, row_num)
        output.append(processed_data)
    if processor.total_warnings_below_threshold(5):
        for processed_data in output:
            upsert_claim(processed_data)
    else:
        LOG.warning(f"File at {ndjson_path} failed threshold checks")

    # process patients
    ndjson_path = '/data/Patient.ndjson'
    fhir_data = load_fhir_data(ndjson_path)
    LOG.info(f"Loaded {len(fhir_data)} FHIR records.")

    ingest_time = datetime.now()
    output = []
    patient_ids = []
    processor = FHIRPatientProcessor(ingest_time)
    for row_num, row in enumerate(fhir_data):
        processed_data = processor.process(row, row_num)
        patient_ids.append(processed_data['patient_id'])
        output.append(processed_data)

    # checks that percent of patients seen before in the file being ingested is above 20% and the percent of warnings
    # for records being ingested is less than 5% of total record count
    if percent_of_patients_above_threshold(patient_ids, threshold=20, connection_dict=pg_connection_dict) and \
        processor.total_warnings_below_threshold(5):
        for processed_data in output:
            upsert_patient(**processed_data)
    else:
        LOG.warning(f"File at {ndjson_path} failed threshold checks")
