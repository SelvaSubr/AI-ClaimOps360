-- dbt/macros/safe_divide.sql
-- Usage: {{ safe_divide('paid_amount', 'billed_amount') }}
{% macro safe_divide(numerator, denominator) %}
    CASE
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL
        THEN NULL
        ELSE {{ numerator }} / {{ denominator }}
    END
{% endmacro %}
