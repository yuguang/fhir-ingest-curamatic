import json, csv
from pandas.io.json import json_normalize
import pandas as pd
from datetime import datetime
from field_mappers.claim_processor import FHIRClaimProcessor
from field_mappers.patient_processor import FHIRPatientProcessor
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os, psycopg2
import logging

LOG = logging.getLogger(__name__)


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


def upsert_patient(patient_id, first_name, last_name, origin):
    """Insert or update a patient's record and log changes to the history table."""
    try:
        conn = psycopg2.connect(**pg_connection_dict)
        cursor = conn.cursor()

        # Check if the patient already exists in the current table
        cursor.execute('SELECT id, insert_ts FROM patients WHERE patient_id = %s', (patient_id,))
        result = cursor.fetchone()
        now = datetime.now()

        if result:
            # If patient exists, update the current table and log to history
            patient_id_db, insert_ts = result
            cursor.execute('''
                UPDATE patients SET first_name = %s, last_name = %s, insert_ts = %s WHERE patient_id = %s
            ''', (first_name, last_name, now, patient_id)
                           )

            cursor.execute('''
                INSERT INTO patients_history (first_name, last_name, patient_id, insert_ts, change_ts)
                VALUES (%s, %s, %s, %s, %s)
            ''', (first_name, last_name, patient_id, insert_ts, now)
                           )
        else:
            # If patient does not exist, insert into the current table
            cursor.execute('''
                INSERT INTO patients (first_name, last_name, patient_id, insert_ts)
                VALUES (%s, %s, %s, %s)
            ''', (first_name, last_name, patient_id, now)
                           )

        conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
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
    sql_query = f"""
    SELECT COUNT(*) AS count_matching_values
    FROM patients_history
    WHERE patient_id IN ({', '.join([f"'{s}'" for s in map(str, patient_ids)])});
    """

    conn = psycopg2.connect(**connection_dict)
    with conn.cursor() as conn:
        conn.execute(sql_query)
        result = conn.fetchone()
        total_values_in_list = len(patient_ids)
        if result:
            matching_values_count = result[0]
            percentage = (matching_values_count / total_values_in_list) * 100
            return percentage > (threshold / 100)
        else:
            return False


if __name__ == "__main__":
    # process claims
    # ndjson_path = '/data/Claim.ndjson'
    # fhir_data = load_fhir_data(ndjson_path)
    # LOG.info(f"Loaded {len(fhir_data)} FHIR records.")
    #
    # ingest_time = datetime.now()
    # output = []
    # for row_num, row in enumerate(fhir_data):
    #     processor = FHIRClaimsProcessor(ingest_time)
    #     processed_data = processor.process(row, row_num)
    #     output.append(processed_data)
    # write_to_db("claims", output)

    # process patients
    ndjson_path = '/data/Patient.ndjson'
    fhir_data = load_fhir_data(ndjson_path)
    LOG.info(f"Loaded {len(fhir_data)} FHIR records.")

    ingest_time = datetime.now()
    output = []
    patient_ids = []
    for row_num, row in enumerate(fhir_data):
        processor = FHIRPatientProcessor(ingest_time)
        processed_data = processor.process(row, row_num)
        patient_ids.append(processed_data['patient_id'])
        output.append(processed_data)

    if percent_of_patients_above_threshold(patient_ids, threshold=20, connection_dict=pg_connection_dict):
        for processed_data in output:
            upsert_patient(**processed_data)
