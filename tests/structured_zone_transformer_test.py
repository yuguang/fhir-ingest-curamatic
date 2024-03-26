import unittest
import psycopg2
from datetime import datetime
from structured_zone_transformer import upsert_patient, pg_connection_dict, percent_of_patients_above_threshold


# Assuming upsert_patient is defined somewhere, import it
# from your_script import upsert_patient

def create_test_tables(conn):
    """Create test tables in the database."""
    with conn.cursor() as cursor:
        cursor.execute('''
            CREATE TEMPORARY TABLE patients
            (
                id         INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                first_name VARCHAR(128) NOT NULL,
                last_name  VARCHAR(128) NOT NULL,
                patient_id VARCHAR(255) NOT NULL,
                insert_ts  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        '''
                       )
        cursor.execute('''
            CREATE TEMPORARY TABLE patients_history
            (
                id         INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                first_name VARCHAR(128) NOT NULL,
                last_name  VARCHAR(128) NOT NULL,
                patient_id VARCHAR(255) NOT NULL,
                insert_ts  TIMESTAMP,
                change_ts  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        '''
                       )
        conn.commit()


class TestPatientPercentageThreshold(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pg_connection_dict["host"] = "localhost"
        cls.pg_connection_dict = pg_connection_dict
        cls.conn = psycopg2.connect(**pg_connection_dict)

        # Ensure the patients_history table exists and is empty
        with cls.conn.cursor() as cursor:
            # Insert test data
            test_data = [
                    ('Test', 'Name 1', 'Test Patient ID 1'),
                    ('Test', 'Name 2', 'Test Patient ID 2')  # Adjust as needed
            ]
            insert_query = "INSERT INTO patients_history (first_name, last_name, patient_id) VALUES (%s, %s, %s);"
            cursor.executemany(insert_query, test_data)
            cls.conn.commit()

    @classmethod
    def tearDownClass(cls):
        """Clean up after tests."""
        with cls.conn.cursor() as cursor:
            delete_query = "DELETE FROM patients_history"
            cursor.execute(delete_query)
            cls.conn.commit()
        cls.conn.close()

    def test_percent_above_threshold(self):
        """Test the percent_of_patients_above_threshold function."""
        # Assuming you've defined or imported percent_of_patients_above_threshold above
        result = percent_of_patients_above_threshold(['Test Patient ID 1', 'Test Patient ID 2', 'Nonexistent ID'], 33,
                                                     self.pg_connection_dict
                                                     )
        self.assertTrue(result)

    def test_percent_below_threshold(self):
        """Test the percent_of_patients_above_threshold function."""
        result = percent_of_patients_above_threshold(['Test Patient ID x', 'Test Patient ID y', 'Nonexistent ID'], 100,
                                                     self.pg_connection_dict
                                                     )
        self.assertFalse(result)


class TestPatientPercentageNoRecords(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pg_connection_dict["host"] = "localhost"
        cls.pg_connection_dict = pg_connection_dict
        cls.conn = psycopg2.connect(**pg_connection_dict)

    @classmethod
    def tearDownClass(cls):
        """Clean up after tests."""
        with cls.conn.cursor() as cursor:
            delete_query = "DELETE FROM patients_history"
            cursor.execute(delete_query)
            cls.conn.commit()
        cls.conn.close()

    def test_percent_above_threshold(self):
        """Test the case of an initial load where the history table is empty"""
        result = percent_of_patients_above_threshold(['Test Patient ID 1', 'Test Patient ID 2', 'Nonexistent ID'], 20,
                                                     self.pg_connection_dict
                                                     )
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
