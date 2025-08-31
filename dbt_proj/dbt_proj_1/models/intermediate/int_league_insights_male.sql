{{ config(
    materialized='table',
    tags=['intermediate', 'male', 'league', 'agg']
) }}

with source as (
    select * from {{ ref('int_players_male') }}
),
league_info as (
select
     league_id
    ,league_name
    ,overall
    ,case when age <=23 and overall > 85 then 1 else 0 end as young_talent
from source
),
agg as (
select
      league_id
    , league_name
    , SUM(young_talent) as young_talents_cnt
    , AVG(overall) as avg_overall_rating
from league_info
group by
      league_id
    , league_name
)
select
      league_id
    , league_name
    , case when young_talents_cnt > 0 then TRUE else FALSE end as young_talents_present
    , avg_overall_rating
from agg