DROP TABLE IF EXISTS trf_leads_funnel;

CREATE TABLE trf_leads_funnel
DISTSTYLE KEY
DISTKEY (hs.email_clean)
SORTKEY (created_date) AS 
WITH ranked AS (
    SELECT *,
    COUNT(app_id) OVER(PARTITION BY email_clean) AS applications_number,
    ROW_NUMBER() OVER(PARTITION BY email_clean ORDER BY 
    best_student_status_stage ASC NULLS LAST, 
    best_coe_stage ASC NULLS LAST, 
    best_offer_stage ASC NULLS LAST, 
    app_date ASC NULLS LAST,
    app_id ASC) AS rn
    FROM trf_applications_cleaned
)
SELECT
hs.contact_id,
hs.email_clean,
hs.full_name,
hs.office,
hs.lead_source,
rnk.counsellor,
rnk.admission_officer,
hs.created_date,
CASE
    WHEN rnk.student_status IS NOT NULL THEN 'Applied'
    ELSE 'Not Applied' END AS student_applied_status,
rnk.applications_number,
rnk.offer_status,
rnk.coe_status,
rnk.student_status,

FROM trf.contacts_cleaned hs
LEFT JOIN ranked rnk ON hs.email_clean = rnk.email_clean AND
                        rnk.rn = 1
WHERE hs.email_row_number = 1;
