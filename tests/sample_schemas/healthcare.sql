CREATE TABLE patients (
    patient_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    date_of_birth DATE NOT NULL,
    gender CHAR(1) CHECK (gender IN ('M', 'F', 'O')),
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state CHAR(2),
    zip_code VARCHAR(10),
    insurance_id VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE providers (
    provider_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    specialty VARCHAR(100),
    npi_number VARCHAR(10) UNIQUE NOT NULL,
    email VARCHAR(100),
    phone VARCHAR(20)
);

CREATE TABLE encounters (
    encounter_id INT PRIMARY KEY,
    patient_id INT NOT NULL,
    provider_id INT NOT NULL,
    encounter_date DATE NOT NULL,
    encounter_type VARCHAR(20) CHECK (encounter_type IN ('inpatient', 'outpatient', 'emergency')),
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'completed', 'cancelled')),
    notes TEXT,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (provider_id) REFERENCES providers(provider_id)
);

CREATE TABLE diagnoses (
    diagnosis_id INT PRIMARY KEY,
    encounter_id INT NOT NULL,
    icd10_code VARCHAR(7) NOT NULL,
    description VARCHAR(200),
    diagnosis_type VARCHAR(20) CHECK (diagnosis_type IN ('primary', 'secondary', 'admitting')),
    diagnosed_date DATE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id)
);

CREATE TABLE medications (
    medication_id INT PRIMARY KEY,
    encounter_id INT NOT NULL,
    drug_name VARCHAR(100) NOT NULL,
    dosage VARCHAR(50),
    frequency VARCHAR(50),
    start_date DATE NOT NULL,
    end_date DATE,
    prescribing_provider_id INT,
    FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id),
    FOREIGN KEY (prescribing_provider_id) REFERENCES providers(provider_id)
);
