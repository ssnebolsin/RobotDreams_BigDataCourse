{{ config(
    materialized='table',
    tags=['stage', 'female', 'coaches']
) }}

with source as (
    select * from {{ source('public', 'female_coaches') }}
)

select * from source
where dob is not null --clear empty values