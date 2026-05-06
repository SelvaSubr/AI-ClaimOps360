{{ config(materialized='table') }}
SELECT
    c.claim_id, c.patient_id, c.billing_npi, c.rendering_npi,
    c.date_of_service,
    DATE_TRUNC('month', c.date_of_service)    AS claim_month,
    c.billed_amount,
    COALESCE(r.paid_amount, 0)                AS paid_amount,
    COALESCE(r.cas_amount,  c.billed_amount)  AS cas_amount,
    COALESCE(r.cas_code,   'PENDING')         AS cas_code,
    COALESCE(r.status,     'pending')         AS adjudication_status,
    COALESCE(r.denial_reason, '')             AS denial_reason,
    c.denial_risk_score, c.risk_tier, c.human_review_required,
    c.provider_explanation, c.prior_auth,
    CASE WHEN r.status = 'paid'   THEN 1 ELSE 0 END AS is_paid,
    CASE WHEN r.status = 'denied' THEN 1 ELSE 0 END AS is_denied,
    ROUND(COALESCE(r.paid_amount,0) / NULLIF(c.billed_amount,0) * 100, 2) AS collection_rate_pct
FROM {{ ref('stg_claims') }} c
LEFT JOIN {{ ref('stg_remittance') }} r ON c.claim_id = r.claim_id
