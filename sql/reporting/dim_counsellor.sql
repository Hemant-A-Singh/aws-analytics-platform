CREATE TABLE IF NOT EXISTS reporting.dim_counsellor(
    counsellor_skey INTEGER IDENTITY(1,1),
    counsellor_name VARCHAR(100),
    counsellor_office VARCHAR(200),
    valid_from TIMESTAMP,
    valid_till TIMESTAMP,
    is_current BOOLEAN
)
DISTSTYLE ALL;

CREATE TEMP TABLE combined_records AS
SELECT counsellor, office FROM transform.trf_applications_cleaned
WHERE counsellor IS NOT NULL
UNION
SELECT counsellor, office FROM transform.trf_contacts_cleaned
WHERE counsellor IS NOT NULL;

CREATE TEMP TABLE changed_records AS
SELECT src.* FROM combined_records src
JOIN reporting.dim_counsellor tgt
ON src.counsellor = tgt.counsellor_name
AND tgt.is_current = TRUE
WHERE src.office<>tgt.counsellor_office;

UPDATE reporting.dim_counsellor tgt
SET valid_till = GETDATE(),
    is_current = FALSE
FROM changed_records c
WHERE tgt.counsellor_name = c.counsellor
AND tgt.is_current = TRUE;

INSERT INTO reporting.dim_counsellor
SELECT
    src.counsellor, src.office, GETDATE(),NULL,TRUE
FROM combined_records src
LEFT JOIN reporting.dim_counsellor tgt 
ON tgt.counsellor_name = src.counsellor
AND tgt.is_current = TRUE
WHERE tgt.counsellor_name IS NULL;

