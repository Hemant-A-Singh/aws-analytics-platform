
DROP TABLE IF EXISTS staging.stg_hubspot_contacts;

CREATE TABLE staging.stg_hubspot_contacts(
    id VARCHAR(100),
    email VARCHAR(100),
    email_clean VARCHAR(100),
    hs_object_id VARCHAR(100),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    phone VARCHAR(50),
    office VARCHAR(100),
    student_status VARCHAR(100),
    counsellor VARCHAR(100),
    country VARCHAR(255),
    lead_source VARCHAR(255),
    country_of_passport VARCHAR(255),
    interested_destination VARCHAR(255),
    -------------------------------------
    createdate TIMESTAMP,
    lastmodifieddate TIMESTAMP,
    ---------------------------------------
    _extracted_at TIMESTAMP,
    _run_id VARCHAR(50),
    _loaded_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY(email_clean)
SORTKEY(createdate);