-- dbt/tests/assert_no_orphan_remittance.sql
-- Fails if remittance rows exist with no matching claim in stg_claims.
SELECT r.claim_id
FROM {{ ref('stg_remittance') }} r
LEFT JOIN {{ ref('stg_claims') }} c ON r.claim_id = c.claim_id
WHERE c.claim_id IS NULL
