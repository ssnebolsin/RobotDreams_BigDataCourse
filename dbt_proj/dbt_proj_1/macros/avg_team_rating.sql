{% test avg_team_rating(model, rating_col) %}

with validation as (
    select
        {{ rating_col }} as avg_overall_rating
     from {{ model }}
)
select *
from validation
where avg_overall_rating > 100 OR avg_overall_rating < 40

{% endtest %}