{{ config(
    materialized='table',
    tags=['intermediate', 'male', 'teams', 'dim']
) }}

with source as (
    select * from {{ ref('int_players_male') }}
)
select
    distinct on (club_team_id) club_team_id, club_name
from source
order by club_team_id, club_name desc
