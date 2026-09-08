CREATE TABLE IF NOT EXISTS reporting.dim_lead_source(
    lead_source_skey INTEGER IDENTITY(1,1),
    lead_source_name VARCHAR(100)
);

CREATE TEMP TABLE source_combined AS 
SELECT lead_type_clean AS lead_source FROM transform.trf_applications_cleaned
WHERE lead_type_clean IS NOT NULL
OR lead_type_clean != 'Unknown'
UNION
SELECT lead_source FROM transform.trf_contacts_cleaned
WHERE lead_source IS NOT NULL
AND lead_source != 'Unknown';

MERGE INTO reporting.dim_lead_source tgt
USING source_combined src
ON tgt.lead_source_name = src.lead_source

WHEN NOT MATCHED THEN
    INSERT(lead_source_name)
    VALUES(src.lead_source);