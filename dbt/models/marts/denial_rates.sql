{{ config(materialized='table') }}
SELECT claim_month, billing_npi,
    COUNT(claim_id)                                              AS total_claims,
    SUM(is_denied)                                              AS denied_claims,
    ROUND(SUM(is_denied)*100.0/NULLIF(COUNT(*),0),2)           AS denial_rate_pct,
    SUM(CASE WHEN denial_risk_score >= 70 THEN 1 ELSE 0 END)   AS high_risk_claims,
    SUM(CASE WHEN denial_risk_score BETWEEN 40 AND 69 THEN 1 ELSE 0 END) AS medium_risk_claims,
    ROUND(AVG(denial_risk_score),2)                             AS avg_risk_score
FROM {{ ref('int_claim_adjudication') }}
WHERE claim_month IS NOT NULL
GROUP BY claim_month, billing_npi
ORDER BY claim_month DESC, denial_rate_pct DESC
