{{ config(materialized='table') }}
SELECT claim_month,
    COUNT(DISTINCT claim_id)   AS claim_count,
    COUNT(DISTINCT billing_npi) AS provider_count,
    SUM(billed_amount)         AS total_billed,
    SUM(paid_amount)           AS total_paid,
    SUM(cas_amount)            AS total_adjustments,
    ROUND(SUM(paid_amount)/NULLIF(SUM(billed_amount),0)*100,2) AS collection_rate_pct,
    SUM(is_denied)             AS denied_claims,
    ROUND(SUM(is_denied)*100.0/NULLIF(COUNT(*),0),2) AS denial_rate_pct
FROM {{ ref('int_claim_adjudication') }}
WHERE claim_month IS NOT NULL
GROUP BY claim_month ORDER BY claim_month DESC
