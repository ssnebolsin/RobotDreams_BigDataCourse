{% test wage_vs_value(model, wage_col, value_col) %}

with validation as (
    select
        {{ wage_col }} as wage_eur,
        {{ value_col }} as value_eur
     from {{ model }}
)
select *
from validation
where wage_eur > value_eur

{% endtest %}