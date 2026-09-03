CREATE TABLE IF NOT EXISTS reporting.fact_applications(
    app_id                  VARCHAR(50),
    student_id              VARCHAR(50),
    uni_student_id          VARCHAR(100),
    email_clean             VARCHAR(255),   -- lowercased, trimmed
    alternate_email         VARCHAR(255),
    date_of_birth           DATE,
    nationality             VARCHAR(100),
    mobile                  VARCHAR(50),
    city                    VARCHAR(255),
    country                 VARCHAR(255),
    app_status              VARCHAR(100),
    coe_status              VARCHAR(100),
    student_status          VARCHAR(100),
    offer_status            VARCHAR(100),
    to_country              VARCHAR(100),
    institution             VARCHAR(255),
    faculty                 VARCHAR(255),
    program                 VARCHAR(255),
    counsellor              VARCHAR(255),
    admission_officer       VARCHAR(255),
    office                  VARCHAR(255),
    office_type             VARCHAR(50),
    app_date                DATE,
    offer_date              DATE,
    coe_date                DATE,
    start_date              DATE,
    finish_date             DATE,
    _extracted_at           TIMESTAMP,
    _loaded_at              TIMESTAMP,
    updated_at              TIMESTAMP
)
DISTSTYLE KEY
DISTKEY (app_id)
SORTKEY(app_date);



MERGE INTO reporting.fact_applications tgt
USING transform.trf_applications_cleaned src
ON tgt.app_id = src.app_id

WHEN MATCHED THEN 
UPDATE SET
tgt.mobile = src.mobile,
tgt.app_status = src.app_status,
tgt.coe_status = src.coe_status,
tgt.student_status = src.student_status,
tgt.offer_status = src.offer_status,
tgt.counsellor = src.counsellor,
tgt.admission_officer = src.admission_officer,
tgt.office = src.office,
tgt.office_type = src.office_type,
tgt.offer_date = src.offer_date,
tgt.coe_date = src.coe_date,
tgt.start_date = src.start_date,
tgt.finish_date = src.finish_date,
tgt.updated_at = GETDATE()

WHEN NOT MATCHED THEN
INSERT(
app_id,
student_id,
uni_student_id,
email_clean,
alternate_email,
date_of_birth,
nationality,
mobile,
city,
country,
app_status,
coe_status,
student_status,
offer_status,
to_country,
institution,
faculty,
program,
counsellor,
admission_officer,
office,
office_type,
app_date,
offer_date,
coe_date,
start_date,
finish_date,
_extracted_at,
_loaded_at,
updated_at
)
VALUES(
src.app_id,
src.student_id,
src.uni_student_id,
src.email_clean,
src.alternate_email,
src.date_of_birth,
src.nationality,
src.mobile,
src.city,
src.country,
src.app_status,
src.coe_status,
src.student_status,
src.offer_status,
src.to_country,
src.institution,
src.faculty,
src.program,
src.counsellor,
src.admission_officer,
src.office,
src.office_type,
src.app_date,
src.offer_date,
src.coe_date,
src.start_date,
src.finish_date,
src._extracted_at,
src.loaded_at,
GETDATE()
);







