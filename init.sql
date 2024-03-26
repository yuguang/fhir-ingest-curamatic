DROP DATABASE IF EXISTS structured;
CREATE DATABASE structured;
CREATE TABLE claims
(
    id         INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    patient_id VARCHAR(255) NOT NULL,
    billing_start DATE NOT NULL,
    billing_end DATE NOT NULL,
    provider VARCHAR(255) NOT NULL,
    admitting_diagnosis VARCHAR(128),
    insurance VARCHAR(50),
    status VARCHAR(50),
    amount NUMERIC(10, 2) NOT NULL,
    insert_ts  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE claims_history
(
    id         INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    patient_id VARCHAR(255) NOT NULL,
    billing_start DATE NOT NULL,
    billing_end DATE NOT NULL,
    provider VARCHAR(255) NOT NULL,
    admitting_diagnosis VARCHAR(128),
    insurance VARCHAR(50),
    status VARCHAR(50),
    amount NUMERIC(10, 2) NOT NULL,
    insert_ts  TIMESTAMP,
    change_ts  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

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
