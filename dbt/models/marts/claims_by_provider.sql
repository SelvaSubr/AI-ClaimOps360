{{ config(materialized='table') }}
SELECT billing_npi,
    COUNT(DISTINCT claim_id)                              AS claim_count,
    COUNT(DISTINCT patient_id)                           AS unique_patients,
    SUM(billed_amount)                                   AS total_billed,
    SUM(paid_amount)                                     AS total_paid,
    SUM(cas_amount)                                      AS total_adjustments,
    ROUND(SUM(paid_amount)/NULLIF(SUM(billed_amount),0)*100,2) AS collection_rate_pct,
    SUM(is_denied)                                       AS denied_claim_count,
    ROUND(SUM(is_denied)*100.0/NULLIF(COUNT(*),0),2)    AS denial_rate,
    AVG(denial_risk_score)                               AS avg_denial_risk,
    SUM(CASE WHEN human_review_required THEN 1 ELSE 0 END) AS review_required_count
FROM {{ ref('int_claim_adjudication') }}
GROUP BY billing_npi ORDER BY total_billed DESC
