DROP TABLE IF EXISTS staging.stg_mysql_applications;

CREATE TABLE staging.stg_mysql_applications(
    app_id                  VARCHAR(50),
    student_id              VARCHAR(50),
    uni_student_id          VARCHAR(100),

    -- Student identity
    full_name               VARCHAR(510),
    email                   VARCHAR(255),
    email_clean             VARCHAR(255),   -- lowercased, trimmed
    email2                  VARCHAR(255),
    dob                     DATE,
    nationality             VARCHAR(100),

    -- Contact
    tel                     VARCHAR(50),
    mobile                  VARCHAR(50),
    city                    VARCHAR(255),
    country                 VARCHAR(255),

    -- Application status pipeline
    app_status              VARCHAR(100),
    offer_status            VARCHAR(100),
    coe_status              VARCHAR(100),
    student_status          VARCHAR(100),

    -- Destination & institution
    to_country              VARCHAR(100),
    institution             VARCHAR(255),
    representing_entity     VARCHAR(255),
    level                   VARCHAR(100),
    faculty                 VARCHAR(255),
    program                 VARCHAR(255),

    -- Team
    counsellor              VARCHAR(255),
    admission_officer       VARCHAR(255),
    owner                   VARCHAR(255),
    office                  VARCHAR(255),

    -- Lead attribution
    lead_type               VARCHAR(100),

    -- Key dates
    app_date                DATE,
    offer_date              DATE,
    coe_date                DATE,
    start_date              DATE,
    finish_date             DATE,

    -- Notes
    descriptions            VARCHAR(2000),

    -- Pipeline metadata
    _extracted_at           TIMESTAMP,
    _run_id                 VARCHAR(50),
    _loaded_at              TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY(email_clean)
SORTKEY(app_date);