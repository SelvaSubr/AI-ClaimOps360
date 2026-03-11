-- adls_integration.sql — ADLS Gen2 → Snowflake zero-copy setup. Run as ACCOUNTADMIN after setup.sql.
-- Required variables: SET AZURE_TENANT_ID='<value>'; SET ADLS_ACCOUNT_NAME='<value>';

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAIMS_ANALYTICS;

-- Create storage integration (Snowflake-managed Azure service principal)

CREATE STORAGE INTEGRATION IF NOT EXISTS claims_adls_integration
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'AZURE'
    ENABLED                   = TRUE
    AZURE_TENANT_ID           = '&AZURE_TENANT_ID'
    STORAGE_ALLOWED_LOCATIONS = (
        'azure://&ADLS_ACCOUNT_NAME.blob.core.windows.net/silver/',
        'azure://&ADLS_ACCOUNT_NAME.blob.core.windows.net/gold/'
    )
    COMMENT = 'AI-ClaimOps360 — zero-copy reads from ADLS Gen2 silver + gold containers';

GRANT USAGE ON INTEGRATION claims_adls_integration TO ROLE CLAIMS_LOADER_ROLE;

DESCRIBE INTEGRATION claims_adls_integration;

-- Azure Portal — grant STORAGE_AZURE_MULTI_TENANT_APP_NAME the Storage Blob Data Reader
-- role on &ADLS_ACCOUNT_NAME (IAM → Enterprise application). Consent STORAGE_AZURE_CONSENT_URL first.
-- External stage + external tables (run after IAM propagates)

DROP STAGE IF EXISTS CLAIMS_ANALYTICS.SILVER.claims_adls_stage;
DROP STAGE IF EXISTS CLAIMS_ANALYTICS.GOLD.claims_adls_stage;

CREATE STAGE IF NOT EXISTS CLAIMS_ANALYTICS.SILVER.claims_adls_stage
    STORAGE_INTEGRATION = claims_adls_integration
    URL                 = 'azure://&ADLS_ACCOUNT_NAME.blob.core.windows.net/silver/'
    FILE_FORMAT         = (TYPE = PARQUET)
    COMMENT             = 'Zero-copy: Delta Parquet from ADLS Gen2 silver containers';

CREATE STAGE IF NOT EXISTS CLAIMS_ANALYTICS.GOLD.claims_adls_stage
    STORAGE_INTEGRATION = claims_adls_integration
    URL                 = 'azure://&ADLS_ACCOUNT_NAME.blob.core.windows.net/gold/'
    FILE_FORMAT         = (TYPE = PARQUET)
    COMMENT             = 'Zero-copy: Delta Parquet from ADLS Gen2 gold containers';

GRANT READ ON STAGE CLAIMS_ANALYTICS.SILVER.claims_adls_stage
    TO ROLE CLAIMS_LOADER_ROLE;

GRANT READ ON STAGE CLAIMS_ANALYTICS.GOLD.claims_adls_stage
    TO ROLE CLAIMS_LOADER_ROLE;

-- EXTERNAL TABLE: SILVER.CLAIMS (DROP first — CREATE OR REPLACE cannot replace a managed table)
DROP TABLE IF EXISTS CLAIMS_ANALYTICS.SILVER.CLAIMS;
CREATE OR REPLACE EXTERNAL TABLE CLAIMS_ANALYTICS.SILVER.CLAIMS (
    claim_id                VARCHAR AS ($1:claim_id::VARCHAR),
    patient_id              VARCHAR AS ($1:patient_id::VARCHAR),
    billing_npi             VARCHAR AS ($1:billing_npi::VARCHAR),
    primary_diagnosis_code  VARCHAR AS ($1:primary_diagnosis_code::VARCHAR),
    procedure_code          VARCHAR AS ($1:procedure_code::VARCHAR),
    billed_amount           NUMBER(12,2) AS ($1:billed_amount::NUMBER(12,2)),
    service_date            DATE    AS ($1:service_date::DATE),
    prior_auth_number       VARCHAR AS ($1:prior_auth_number::VARCHAR),
    fhir_resource_type      VARCHAR AS ($1:fhir_resource_type::VARCHAR),
    payer_id                VARCHAR AS ($1:payer_id::VARCHAR),
    ingest_ts               TIMESTAMP_NTZ AS ($1:ingest_ts::TIMESTAMP_NTZ),
    processed_ts            TIMESTAMP_NTZ AS ($1:processed_ts::TIMESTAMP_NTZ),
    source_system           VARCHAR AS ($1:source_system::VARCHAR),
    -- 7 CMS-mandatory AI columns
    denial_risk_score       INT     AS ($1:denial_risk_score::INT),
    human_review_required   BOOLEAN AS ($1:human_review_required::BOOLEAN),
    provider_explanation    VARCHAR AS ($1:provider_explanation::VARCHAR),
    ai_decision_only        BOOLEAN AS ($1:ai_decision_only::BOOLEAN),
    reconsideration_right   BOOLEAN AS ($1:reconsideration_right::BOOLEAN),
    primary_driver          VARCHAR AS ($1:primary_driver::VARCHAR),
    decision_basis          VARCHAR AS ($1:decision_basis::VARCHAR)
)
WITH LOCATION       = @CLAIMS_ANALYTICS.SILVER.claims_adls_stage
AUTO_REFRESH        = FALSE
FILE_FORMAT         = (TYPE = PARQUET);

GRANT OWNERSHIP ON EXTERNAL TABLE CLAIMS_ANALYTICS.SILVER.CLAIMS TO ROLE CLAIMS_LOADER_ROLE;

-- EXTERNAL TABLE: GOLD.REMITTANCE
DROP TABLE IF EXISTS CLAIMS_ANALYTICS.GOLD.REMITTANCE;
CREATE OR REPLACE EXTERNAL TABLE CLAIMS_ANALYTICS.GOLD.REMITTANCE (
    claim_id        VARCHAR      AS ($1:claim_id::VARCHAR),
    member_id       VARCHAR      AS ($1:member_id::VARCHAR),
    date_of_service DATE         AS ($1:date_of_service::DATE),
    status          VARCHAR      AS ($1:status::VARCHAR),
    billed_amount   NUMBER(12,2) AS ($1:billed_amount::NUMBER(12,2)),
    paid_amount     NUMBER(12,2) AS ($1:paid_amount::NUMBER(12,2)),
    cas_amount      NUMBER(12,2) AS ($1:cas_amount::NUMBER(12,2)),
    cas_code        VARCHAR      AS ($1:cas_code::VARCHAR),
    denial_reason   VARCHAR      AS ($1:denial_reason::VARCHAR),
    processed_ts    TIMESTAMP_NTZ AS ($1:processed_ts::TIMESTAMP_NTZ)
)
WITH LOCATION       = @CLAIMS_ANALYTICS.GOLD.claims_adls_stage/remittance/
AUTO_REFRESH        = FALSE
FILE_FORMAT         = (TYPE = PARQUET);

GRANT OWNERSHIP ON EXTERNAL TABLE CLAIMS_ANALYTICS.GOLD.REMITTANCE TO ROLE CLAIMS_LOADER_ROLE;

-- Verify (run after a Databricks pipeline has written to ADLS)
SELECT COUNT(*) FROM CLAIMS_ANALYTICS.SILVER.CLAIMS;
SELECT COUNT(*) FROM CLAIMS_ANALYTICS.GOLD.REMITTANCE;
SELECT * FROM CLAIMS_ANALYTICS.SILVER.CLAIMS LIMIT 3;
