{{ config(
    materialized='table',
    tags=['intermediate', 'female', 'players']
) }}

with source as (
    select * from {{ ref('stg_female_players') }}
)
select
        *,
       'female' as gender from source