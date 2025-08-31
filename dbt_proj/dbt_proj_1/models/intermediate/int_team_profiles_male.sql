{{ config(
    materialized='table',
    tags=['intermediate', 'male', 'players', 'agg']
) }}

with source as (
    select * from {{ ref('int_players_male') }}
),
teams as (
    select * from {{ ref('int_dim_teams_male') }}
)
select
  t.club_team_id
, t.club_name
, AVG(s.overall) as avg_overall_rating
, SUM(s.value_eur) as total_club_value
from source s
left join teams t on s.club_team_id = t.club_team_id
group by t.club_team_id, t.club_name