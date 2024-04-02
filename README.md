# System design challenge

### Environment setup

If pyenv and pyenv-virtual env is not installed on your local environment, follow this [installation guide](https://medium.com/@adocquin/mastering-python-virtual-environments-with-pyenv-and-pyenv-virtualenv-c4e017c0b173#e871).
You can verify that both are installed by running `pyenv virtualenvs` and checking that the output does not contain errors. 
After that, run the `setup-pyenv.sh` script to set up your local dev environment. Activate the environment by running:
```
pyenv activate "fhir-ingest"
```

### Ingesting files

1. Start up the docker containers with:
```
docker compose up
```
2. Check that the containers are running and the database is initialized
```
docker ps -a
docker logs --follow postgres-db
```
If the Postgres database `structured` is not initialized, manually run the script in `/docker-entrypoint-initdb.d/init.sql`.

3. Run the first step in the ingestion process to process the data from the raw zone using the `structured_zone_transformer`:
```
docker exec ingest-service python /src/structured_zone_transformer.py
```
This runs validations on each of the JSON entries, checking for required fields. 

### Executing tests in docker
To execute tests in the docker containers, run the following commands after running the the first two
steps of the instructions in the "Ingesting files" section:
```
./execute-pytest.sh
```
A couple of integration tests require the database to be up and running with Docker compose.

### Architecture and design
This implementation follows a simplified version of the Databricks medallion architecture, where
1. The raw data is kept in CSV files for the "Bronze" zone. On a production environment, these would stored in S3 prefixes with the date of the file 
2. The patient and claims data is normalized, mapped, and validated and stored in "Silver" zone tables.
3. The consumer zone transformer, which loads data from the "Silver" (structured) zone tables, consolidates historical data and removes data that does not conform to business definitions. 
For example, it ensures claims written to the data warehouse have patient ID fields populated. 
#### Structured zone tables
The table structures are defined in init.sql. The `patients` and `claims` tables keep track of the current claims and patients. 
The `patients_history` and `claims_history` tables keep track of the history of updates to claims and patients. Different
versions of the data is tracked across time in order to be resilient to changes in the business environment. The historical 
data in these tables facilitate reprocessing when business logic changes. 

#### Use cases supported and implemented
The current ingestion code handles each of these cases:
- Update the schema of the different files without telling us, changing the names of columns
- Send a completely different group of patients in the Patient.ndjson file
- Include duplicate records. Assume that occurrence in the file indicates the order in which they are updated, i.e lower in the file = more recent.
- Column values don’t conform to the proposed schema
    - Formats not as defined
- NULL values in one update that are filled in in later updates
    - Send a small number of claims with no corresponding users one week, only to include the patients in a subsequent update.
- The file contains a lot of validation errors

The `structured_zone_transformer` supports each of these cases respectively by:
- running validations on each of the JSON entries, checking for required fields.
- checking that the percentage of patient IDs in the history table is above a certain threshold before inserting rows 
into tables (see `test_percent_above_threshold`)
- using upserts to update existing patients and claims with the `upsert_patient` and `upsert_claim` methods 
- checking for formats of date fields and checking field values for enum fields; checks for other formats and other
enum fields can follow the same structure
- for NULL fields that get filled in at a later date, the `map_values` method sets those mapped field values to `None`,
which shows up as NULL in the database. There is a test case `test_map_values_with_null_patient_reference` that validates
this behavior. Once the field is filled in at a later date, the `upsert_patient` and `upsert_claim` methods update the
current tables, and the consumer zone transformer would load the data into the warehouse.
- if the file contains more than the percentage of allowed validation errors, it is recommended to check with the hospital
where the file is being produced to resolve the data quality issue. This check is performed with `FHIRResourceProcessor.total_warnings_below_threshold`

#### Out of scope
This design does not aim to implement the full ETL pipeline, which requires Airflow and EMR to run. However, to scale up
to data set sizes that are 10,000 times larger, the `validate`, `map_values`, and `normalize` methods can each be implemented
in Spark, and the upsert methods can be modified to write files in parallel to S3. The current upsert methods also do not
support routing to different databases determined by the `origin` field, which is designed to support multi-tenancy. 

The implementation also does not include the consumer zone transformer to build the data warehouse, as the business uses
cases need to be clarified before implementation. However, the consumer zone would contain tables for building dashboards
and reports. 

#### Soda validations
API keys are required to run validations (https://docs.soda.io/soda-library/install.html#configure-soda). Once they are configured, 
after running the transformer, one can run the validations on the data that was produced:
```
docker exec ingest-service python /src/structured_zone_validations.py
```
