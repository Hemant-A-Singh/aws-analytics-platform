CREATE TABLE IF NOT EXISTS reporting.fact_leads(
    contact_id             VARCHAR(50),
    email_clean            VARCHAR(255),   -- lowercased, trimmed
    First_name             VARCHAR(255),
    last_name              VARCHAR(255),
    full_name              VARCHAR(255),
    office                 VARCHAR(255),
    office_type            VARCHAR(255),
    student_status         VARCHAR(100),
    counsellor             VARCHAR(255),
    country                VARCHAR(255),
    lead_source            VARCHAR(255),
    country_of_passport    VARCHAR(255),
    interested_destination VARCHAR(255),
    created_date           DATE,
    last_modified_date     DATE,
)
DISTSTYLE KEY
DISTKEY (contact_id)
SORTKEY(created_date);

MERGE INTO reporting.fact_leads tgt
USING transform.trf_contacts_cleaned src
ON tgt.contact_id = src.contact_id

WHEN MATCHED THEN
UPDATE SET
email_clean = src.email_clean,
office = src.office,
office_type = src.office_type,
student_status = src.student_status,
counsellor = src.counsellor,
lead_source = src.lead_source
interested_destination = src.interested_destination
last_modified_date = GETDATE()

WHEN NOT MATCHED THEN
INSERT(
contact_id
email_clean
First_name
last_name
full_name
office
office_type
student_status
counsellor
country
lead_source
country_of_passport
interested_destination
created_date
last_modified_date
)
VALUES(
src.contact_id,
src.email_clean,
src.First_name,
src.last_name,
src.full_name,
src.office,
src.office_type,
src.student_status,
src.counsellor,
src.country,
src.lead_source,
src.country_of_passport,
src.interested_destination,
GETDATE(),
GETDATE()
);
