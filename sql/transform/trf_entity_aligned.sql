DROP TABLE IF EXISTS transform.trf_entity_aligned;

CREATE TABLE transform.trf_entity_aligned
DISTSTYLE KEY
DISTKEY (email_clean)
SORTKEY (app_id) AS

SELECT
app.app_id,
app.student_id,
app.email_clean,
app.date_of_birth,
app.nationality,
app.city,
app.country,
app.app_status,
app.coe_status,
app.student_status,
app.to_country,
app.institution,
app.faculty,
app.program,
app.counsellor,
app.admission_officer,
app.office,
app.office_type,
app.lead_type_clean,
app.app_date,
app.offer_date,
app.coe_date,
app.start_date,
hs.lead_source,
hs.created_date,
DATEDIFF(DAY, hs.created_date, app.app_date) AS lead_to_application_days,
DATEDIFF(DAY, app.app_date, app.offer_date) AS app_to_offer_days,
DATEDIFF(DAY, app.offer_date, app.coe_date) AS offer_to_coe_days,
DATEDIFF(DAY, app.coe_date, app.start_date) AS coe_to_enrollment_days,
DATEDIFF(DAY, hs.created_date, app.start_date) AS lead_to_enrollment_days,
GETDATE() AS _aligned_at

FROM transform.trf_applications_cleaned app
LEFT JOIN transform.trf_contacts_cleaned hs ON
app.email_clean = hs.email_clean AND
hs.email_row_number = 1;
