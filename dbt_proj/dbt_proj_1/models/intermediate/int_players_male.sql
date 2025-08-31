{{ config(
    materialized='table',
    tags=['intermediate', 'male', 'players']
) }}

with source as (
    select * from {{ ref('stg_male_players') }}
)
select
        *,
       'male' as gender from source
       where fifa_version = 22