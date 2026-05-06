{{ config(materialized='table', tags=['snowflake-only']) }}

{% if target.type == 'snowflake' %}

SELECT
    claim_id, billing_npi, date_of_service,
    denial_risk_score, risk_tier, human_review_required,
    cas_code, adjudication_status,
    provider_explanation AS shap_explanation,
    SNOWFLAKE.CORTEX.COMPLETE(
        '{{ var("cortex_model") }}',
        CONCAT(
            'You are a healthcare claims specialist. Write a 2-sentence provider-facing ',
            'explanation for this claim decision in plain English for a billing specialist. ',
            'Claim ID: ', claim_id, '. Risk score: ', CAST(denial_risk_score AS VARCHAR),
            '/100. Risk category: ', risk_tier, '. Adjustment code: ', cas_code,
            '. Base explanation: ', provider_explanation
        )
    ) AS cortex_provider_explanation,
    SNOWFLAKE.CORTEX.COMPLETE(
        '{{ var("cortex_model") }}',
        CONCAT(
            'In one sentence, what action should the provider take? Risk: ',
            risk_tier, '. Adjustment: ', cas_code, '.'
        )
    ) AS recommended_action
FROM {{ ref('int_claim_adjudication') }}
WHERE human_review_required = TRUE AND claim_id IS NOT NULL

{% else %}

-- Run dbt run --target snowflake to populate LLM columns.
SELECT
    claim_id, billing_npi, date_of_service,
    denial_risk_score, risk_tier, human_review_required,
    cas_code, adjudication_status,
    provider_explanation AS shap_explanation,
    CAST(NULL AS STRING)  AS cortex_provider_explanation,
    CAST(NULL AS STRING)  AS recommended_action
FROM {{ ref('int_claim_adjudication') }}
WHERE human_review_required = TRUE AND claim_id IS NOT NULL

{% endif %}
