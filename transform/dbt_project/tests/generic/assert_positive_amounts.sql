-- Generic test: asserts that a numeric column contains only non-negative values.
-- Usage in schema.yml:
--   - dbt_utils.expression_is_true:
--       expression: ">= 0"
-- Or use this directly as a custom singular test per model.

{% test assert_positive_amounts(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
