DROP TABLE IF EXISTS reporting.dim_date;

CREATE TABLE reporting.dim_date(
    date_key        INTEGER         NOT NULL,   -- YYYYMMDD integer
    full_date       DATE            NOT NULL,
    year            SMALLINT,
    quarter         SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(10),
    week            SMALLINT,
    day_of_month    SMALLINT,
    day_of_week     SMALLINT,
    day_name        VARCHAR(10),
    is_weekend      BOOLEAN,
    financial_year  VARCHAR(10)
)
DISTSTYLE ALL
SORTKEY (full_date);

INSERT INTO reporting.dim_date
WITH RECURSIVE date_spine AS (
SELECT CAST('2020-01-01' AS DATE) AS dt
UNION ALL
SELECT DATEADD(day,1,dt) FROM datespine
WHERE dt<'2030-12-31')
SELECT
    CAST(TO_CHAR(dt,'YYYYMMDD') AS INTEGER) AS date_key,
    dt AS full_date,
    EXTRACT(YEAR FROM dt)::SMALLINT AS year,
    EXTRACT(QUARTER FROM dt)::SMALLINT AS quarter,
    EXTRACT(MONTH FROM dt)::SMALLINT AS month,
    TO_CHAR(dt,'Month') AS month_name,
    EXTRACT(WEEK FROM dt)::SMALLINT AS WEEK,
    EXTRACT(DAY FROM dt)::SMALLINT AS day_of_month,
    EXTRACT(DOW FROM dt)::SMALLINT AS day_of_week,
    TO_CHAR(dt,'day') AS day_name,
    CASE
        WHEN EXTRACT(DOW FROM dt) IN (0,6) THEN TRUE
        ELSE FALSE END AS is_weekend,
    CASE
        WHEN EXTRACT(MONTH FROM dt)>=7 THEN (EXTRACT(YEAR FROM dt))::VARCHAR(5) || '-' || (EXTRACT(YEAR FROM dt)+1)::VARCHAR(5)
        ELSE (   EXTRACT(YEAR FROM dt)-1)::VARCHAR(5) || '-' ||  (EXTRACT(YEAR FROM dt))::VARCHAR(5)
        END AS financial_year
    FROM date_spine;