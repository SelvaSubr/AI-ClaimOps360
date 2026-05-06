{{ config(materialized='view') }}
SELECT claim_id, member_id, CAST(date_of_service AS DATE) AS date_of_service,
    status, CAST(billed_amount AS DECIMAL(10,2)) AS billed_amount,
    CAST(paid_amount AS DECIMAL(10,2)) AS paid_amount,
    CAST(cas_amount  AS DECIMAL(10,2)) AS cas_amount,
    cas_code, denial_reason,
    CASE WHEN billed_amount > 0
         THEN ROUND(cas_amount / billed_amount * 100, 2)
         ELSE 0 END AS adjustment_rate_pct
FROM {{ source('gold', 'remittance') }}
WHERE claim_id IS NOT NULL
