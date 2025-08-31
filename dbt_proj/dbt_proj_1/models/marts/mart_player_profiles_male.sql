{{ config(
    materialized='table',
    tags=['mart', 'male', 'player']
) }}

with source as (
    select * from {{ ref('int_players_male') }}
),
positions as (
    select * from {{ source('public', 'positions') }}
)
select
     ipm.player_id
    ,ipm.short_name
    ,ipm.long_name
    ,case
        when ipm.age < 21 then 'U21'
        when ipm.age < 28 then 'Prime'
        else 'Veteran'
    end as age_group
    ,p.description
from source ipm
left join positions p on ipm.club_position = p.position