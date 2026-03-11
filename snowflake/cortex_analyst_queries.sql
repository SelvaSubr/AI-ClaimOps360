-- Cortex Analyst reference queries — run in Snowsight after uploading cortex_semantic_model.yaml

-- Provider denial rate ranking
SELECT billing_npi, denial_rate, claim_count, avg_denial_risk
FROM GOLD.CLAIMS_BY_PROVIDER
ORDER BY denial_rate DESC
LIMIT 10;

-- Claims requiring human review
SELECT claim_id, billing_npi, denial_risk_score,
       cortex_provider_explanation, recommended_action
FROM GOLD.DENIAL_EXPLANATIONS
WHERE denial_risk_score >= 70
ORDER BY denial_risk_score DESC;

-- Revenue by month
SELECT claim_month, total_billed, total_paid,
       collection_rate_pct, claim_count
FROM GOLD.REVENUE_SUMMARY
ORDER BY claim_month DESC;

-- High-risk months
SELECT claim_month, SUM(high_risk_claims) AS total_high_risk,
       AVG(avg_risk_score) AS avg_risk
FROM GOLD.DENIAL_RATES
GROUP BY claim_month
ORDER BY total_high_risk DESC;

-- Cortex LLM connectivity test (CORTEX_MODEL env var default: llama3.1-70b)
SET cortex_model = 'llama3.1-70b';
SELECT SNOWFLAKE.CORTEX.COMPLETE($cortex_model, 'Test: respond OK') AS test_response;
