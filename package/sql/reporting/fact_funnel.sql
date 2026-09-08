CREATE TABLE IF NOT EXISTS reporting.fact_funnel(
    contact_id             VARCHAR(50),
    email_clean            VARCHAR(255),   -- lowercased, trimmed
    full_name              VARCHAR(255),
    office                 VARCHAR(255),
    lead_source            VARCHAR(255),
    counsellor             VARCHAR(255),
    admission_officer      VARCHAR(255),
    created_date           DATE,
    student_applied_status VARCHAR(50),
    applications_number    INT,
    offer_status           VARCHAR(100),
    coe_status             VARCHAR(100),
    student_status         VARCHAR(100),
    _loaded_at             TIMESTAMP,
    _updated_at             TIMESTAMP
)
DISTSTYLE AUTO
SORTKEY(created_date);

MERGE INTO reporting.fact_funnel tgt
USING transform.trf_leads_funnel src
ON tgt.email_clean = src.email_clean

WHEN MATCHED THEN
UPDATE SET
office = src.office,
counsellor = src.counsellor,
admission_officer = src.admission_officer,
student_applied_status = src.student_applied_status,
applications_number = src.applications_number,
offer_status = src.offer_status,
coe_status = src.coe_status,
student_status = src.student_status,
_updated_at = GETDATE()

WHEN NOT MATCHED THEN
INSERT(
contact_id,
email_clean,
full_name,
office,
lead_source,
counsellor,
admission_officer,
created_date,
student_applied_status,
applications_number,
offer_status,
coe_status,
student_status,
_loaded_at,
_updated_at
)
VALUES(
src.contact_id,
src.email_clean,
src.full_name,
src.office,
src.lead_source,
src.counsellor,
src.admission_officer,
src.created_date,
src.student_applied_status,
src.applications_number,
src.offer_status,
src.coe_status,
src.student_status,
GETDATE(),
GETDATE()
);