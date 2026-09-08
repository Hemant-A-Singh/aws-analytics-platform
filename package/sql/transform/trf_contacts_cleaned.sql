DROP TABLE IF EXISTS transform.trf_contacts_cleaned;

CREATE TABLE transform.trf_contacts_cleaned
DISTSTYLE KEY
DISTKEY (email_clean)
SORTKEY (createdate) AS

SELECT
id AS contact_id,
email AS email_raw,
email_clean,
TRIM(COALESCE(first_name,'')) AS first_name,
TRIM(COALESCE(last_name,'')) AS last_name,
TRIM(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')) AS full_name,
phone,
TRIM(COALESCE(office,'')) AS office,
CASE
    WHEN office = 'JHG-Saudi' THEN 'JHG-Saudi'
    WHEN office = 'JHG-Oman' THEN 'JHG-Oman'
    WHEN office = 'JHG-Dubai' THEN 'JHG-Dubai'
    WHEN office = 'JHG-China' THEN 'JHG-China'
    WHEN office = 'JHG-Global' THEN 'JHG-Global'
    WHEN office like 'JHG%' THEN 'Licensed Office'
    else 'Subagent'
END AS office_type,
INITCAP(TRIM(COALESCE(student_status,''))) AS student_status,
INITCAP(TRIM(COALESCE(counsellor,''))) AS counsellor,
TRIM(COALESCE(country,'')) AS country,
TRIM(COALESCE(lead_source,'')) AS lead_source,
TRIM(COALESCE(country_of_passport,'')) AS country_of_passport,
TRIM(COALESCE(interested_destination,'')) AS interested_destination,
CAST(createdate as DATE) AS created_date,
CAST(lastmodifieddate as DATE) as last_modified_date,
createdate,
lastmodifieddate,
ROW_NUMBER() OVER(PARTITION BY email_clean ORDER BY createdate ASC) AS email_row_number,
_extracted_at,
_run_id,
_loaded_at

FROM staging.stg_hubspot_contacts
WHERE email_clean IS NOT NULL
AND email_clean != '';


