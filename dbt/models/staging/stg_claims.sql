{{ config(materialized='view') }}
-- Source: CLAIMS_ANALYTICS.SILVER.CLAIMS (output of bronze_to_silver.py)
-- Column aliases bridge Silver column names to dbt/downstream convention.
SELECT
    claim_id,
    patient_id,
    billing_npi,
    NULL::VARCHAR(10)                               AS rendering_npi,      -- not captured in current pipeline
    service_date                                    AS date_of_service,
    CAST(billed_amount AS DECIMAL(10,2))            AS billed_amount,
    prior_auth_number                               AS prior_auth,
    primary_diagnosis_code                          AS diagnosis_codes,
    procedure_code                                  AS procedure_codes,
    CAST(denial_risk_score AS INTEGER)              AS denial_risk_score,
    CAST(human_review_required AS BOOLEAN)          AS human_review_required,
    provider_explanation,
    processed_ts,
    CASE WHEN denial_risk_score >= 70 THEN 'HIGH'
         WHEN denial_risk_score >= 40 THEN 'MEDIUM'
         ELSE 'LOW' END                             AS risk_tier
FROM {{ source('silver', 'claims') }}
WHERE claim_id IS NOT NULL
