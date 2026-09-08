CREATE TABLE IF NOT EXISTS reporting.dim_institution(
    institution_skey INTEGER IDENTITY(1,1)
    institution_name TEXT
);

MERGE INTO reporting.dim_institution tgt
USING transform.trf_applications_cleaned src
ON tgt.institution_name = src.institution

WHEN NOT MATCHED THEN
    INSERT(institution_name)
    VALUES(src.institution);