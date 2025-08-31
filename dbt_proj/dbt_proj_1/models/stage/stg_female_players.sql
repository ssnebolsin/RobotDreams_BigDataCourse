{{ config(
    materialized='table',
    tags=['stage', 'female', 'players']
) }}

with source as (
    select * from {{ source('public', 'female_players') }}
)

select * from source
where value_eur is not null
and league_id is not null
and club_team_id is not null
and nation_team_id is not null  --clear empty values

