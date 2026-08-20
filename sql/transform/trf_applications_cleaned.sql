DROP TABLE IF EXISTS transform.trf_applications_cleaned;

CREATE TABLE transform.trf_applications_cleaned
DISTSTYLE KEY
DISTKEY (email_clean)
SORTKEY (app_date) AS

SELECT

app_id,
student_id,
uni_student_id,
email_clean,
email AS email_raw,
LOWER(TRIM(email_2)) AS alternate_email,
dob AS date_of_birth,
INITCAP(TRIM(nationality)) AS nationality,
tel,
mobile,
TRIM(COALESCE(city,'Unknown')) AS city,
TRIM(COALESCE(country,'Unknown')) AS country,
TRIM(COALESCE(app_status,'Unknown')) AS app_status,
TRIM(COALESCE(coe_status,'Unknown')) AS coe_status,
TRIM(COALESCE(student_status,'Unknown')) AS student_status,
COALESCE(NULLIF(TRIM(to_country),''),'Unknown') AS to_country,
COALESCE(NULLIF(TRIM(institution),''),'Unknown') AS institution,
representing_entity,
level,
faculty,
program,
COALESCE(counsellor, 'Unknown')     AS counsellor,
COALESCE(admission_officer, 'Unknown') AS admission_officer,
COALESCE(owner, 'Unknown')          AS owner,
COALESCE(office, 'Unknown')         AS office,
CASE
    WHEN office = 'JHG-Saudi' THEN 'JHG-Saudi'
    WHEN office = 'JHG-Oman' THEN 'JHG-Oman'
    WHEN office = 'JHG-Dubai' THEN 'JHG-Dubai'
    WHEN office = 'JHG-China' THEN 'JHG-China'
    WHEN office = 'JHG-Global' THEN 'JHG-Global'
    WHEN office like 'JHG%' THEN 'Licensed Office'
    else 'Subagent'
END AS office_type,
CASE
    WHEN LOWER(lead_type) LIKE '%sub-agent%'    THEN 'Sub-Agent'
    WHEN LOWER(lead_type) LIKE '%direct%'       THEN 'Direct Inquiry'
    WHEN LOWER(lead_type) LIKE '%referral%'     THEN 'Referral'
    WHEN LOWER(lead_type) LIKE '%partnership%'  THEN 'Partnership'
    ELSE COALESCE(lead_type, 'Unknown')
END AS lead_type_clean,
CASE
    WHEN app_date IS NOT NULL AND TRIM(app_date)!='' 
    THEN CAST(LEFT(TRIM(app_date),10) AS DATE) END AS app_date,
CASE 
    WHEN offer_date IS NOT NULL AND TRIM(offer_date) != ''
    THEN CAST(LEFT(TRIM(offer_date), 10) AS DATE) END  AS offer_date,
CASE 
    WHEN coe_date   IS NOT NULL AND TRIM(coe_date)   != ''
    THEN CAST(LEFT(TRIM(coe_date),   10) AS DATE) END  AS coe_date,
CASE 
    WHEN start_date IS NOT NULL AND TRIM(start_date) != ''
    THEN CAST(LEFT(TRIM(start_date), 10) AS DATE) END  AS start_date,
CASE 
    WHEN finish_date IS NOT NULL AND TRIM(finish_date) != ''
    THEN CAST(LEFT(TRIM(finish_date),10) AS DATE) END  AS finish_date,
descriptions,
_extracted_at,
_run_id,
_loaded_at

FROM staging.stg_mysql_application
WHERE app_id IS NOT NULL;

