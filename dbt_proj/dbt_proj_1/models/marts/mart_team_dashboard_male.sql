{{ config(
    materialized='table',
    tags=['mart', 'male', 'player']
) }}

with players as (
    select * from {{ ref('int_players_male') }}
),
teams as (
    select * from {{ ref('int_team_profiles_male') }}
),
players_agg as
(
select
	long_name,
	club_team_id,
	avg(overall) as avg_rating,
	avg(value_eur) as avg_value_eur
from players ipm
group by ipm.long_name, club_team_id ),
teams_agg as (
select
tpm.club_name,
pm.avg_rating,
rank() over (
        partition by tpm.club_name
        order by avg_rating desc
    ) as rank_in_club,
pm.avg_value_eur,
pm.long_name
from teams tpm
left join players_agg pm
on tpm.club_team_id = pm.club_team_id
)
select
    club_name,
    avg_rating,
    rank_in_club,
    avg_value_eur,
    long_name as key_players
from teams_agg
where rank_in_club < 4