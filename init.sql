DROP DATABASE IF EXISTS structured;
CREATE DATABASE structured;
-- create a table
CREATE TABLE patients
(
    id         INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    first_name VARCHAR(128) NOT NULL,
    last_name  VARCHAR(128) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    insert_ts  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE patients_history
(
    id         INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    first_name VARCHAR(128) NOT NULL,
    last_name  VARCHAR(128) NOT NULL,
    patient_id VARCHAR(255) NOT NULL,
    insert_ts  TIMESTAMP,
    change_ts  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);
