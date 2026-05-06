-- dbt/tests/assert_denial_score_range.sql
-- Run: dbt test --target databricks
SELECT claim_id, denial_risk_score
FROM {{ ref('stg_claims') }}
WHERE denial_risk_score IS NOT NULL
  AND (denial_risk_score < 0 OR denial_risk_score > 100)
