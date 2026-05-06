-- stg_providers: staging layer for NPI/provider dimension
-- Source: built from Silver claims data — one row per unique billing_npi
{{ config(materialized='view') }}

SELECT DISTINCT
    billing_npi                               AS provider_npi,
    NULL::VARCHAR(10)                         AS rendering_npi,
    CONCAT('Provider-', billing_npi)           AS provider_name,
    'GENERAL_PRACTICE'                         AS provider_specialty,
    'TX'                                       AS provider_state,
    MIN(service_date)                          AS first_claim_date,
    MAX(service_date)                          AS last_claim_date,
    COUNT(DISTINCT claim_id)                   AS total_claims
FROM {{ source('silver', 'claims') }}
WHERE billing_npi IS NOT NULL
  AND LENGTH(billing_npi) = 10
GROUP BY billing_npi, rendering_npi
